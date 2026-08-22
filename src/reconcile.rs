use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use k8s_openapi::{
    api::{
        apps::v1::Deployment,
        core::v1::{Pod, Service},
        networking::v1::NetworkPolicy,
    },
    apimachinery::pkg::apis::meta::v1::OwnerReference,
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{DeleteParams, ListParams, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        watcher,
    },
};
use serde_json::{Value, json};
use thiserror::Error;
use tracing::{error, info, warn};

use crate::{
    LOAD_BALANCER_CLASS, RUNTIME_CLASS_NAME, TRAFFIC_MODE_ANNOTATION,
    crd::{FlashEndpoint, FlashService, FlashServicePhase, FlashServiceStatus},
    domain::{ExposureType, TrafficMode, ValidationError},
    image::{GIB_BYTES, ImageInspection, ImageInspector},
};

const FIELD_MANAGER: &str = "heterocloud-flash-controller";

#[derive(Clone)]
pub struct ControllerContext {
    client: Client,
    namespace: String,
    image_inspector: ImageInspector,
    registry_pull_secret: Option<String>,
}

impl ControllerContext {
    #[must_use]
    pub fn new(
        client: Client,
        namespace: String,
        image_inspector: ImageInspector,
        registry_pull_secret: Option<String>,
    ) -> Self {
        Self {
            client,
            namespace,
            image_inspector,
            registry_pull_secret,
        }
    }
}

pub async fn run_controller(
    client: Client,
    namespace: String,
    image_inspector: ImageInspector,
    registry_pull_secret: Option<String>,
) -> Result<()> {
    let services = Api::<FlashService>::namespaced(client.clone(), &namespace);
    let deployments = Api::<Deployment>::namespaced(client.clone(), &namespace);
    let network_services = Api::<Service>::namespaced(client.clone(), &namespace);
    let network_policies = Api::<NetworkPolicy>::namespaced(client.clone(), &namespace);
    let context = Arc::new(ControllerContext::new(
        client,
        namespace,
        image_inspector,
        registry_pull_secret,
    ));

    info!("FlashService controller started");
    Controller::new(services, watcher::Config::default())
        .owns(deployments, watcher::Config::default())
        .owns(network_services, watcher::Config::default())
        .owns(network_policies, watcher::Config::default())
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            match result {
                Ok((object, _action)) => info!(
                    name = %object.name,
                    namespace = %object.namespace.as_deref().unwrap_or(""),
                    "FlashService reconciled"
                ),
                Err(error) => error!(error = %error, "FlashService reconciliation failed"),
            }
        })
        .await;
    Ok(())
}

async fn reconcile(
    flash: Arc<FlashService>,
    context: Arc<ControllerContext>,
) -> Result<Action, ReconcileError> {
    let name = flash.name_any();
    let services = Api::<FlashService>::namespaced(context.client.clone(), &context.namespace);

    if let Err(error) = flash.spec.workload.validate() {
        patch_status_if_changed(
            &services,
            &name,
            flash.status.as_ref(),
            FlashServiceStatus {
                phase: FlashServicePhase::Error,
                observed_generation: flash.spec.desired_generation,
                desired_replicas: i32::try_from(flash.spec.workload.replicas).unwrap_or(i32::MAX),
                runtime_class: RUNTIME_CLASS_NAME.into(),
                message: Some(error.to_string()),
                ..FlashServiceStatus::default()
            },
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    let desired_replicas = i32::try_from(flash.spec.workload.replicas)
        .map_err(|_| ReconcileError::InvalidReplicaCount)?;
    let disk_budget_bytes = u64::from(flash.spec.workload.ephemeral_storage_gib)
        .checked_mul(GIB_BYTES)
        .ok_or(ReconcileError::StorageBudgetOverflow)?;
    let inspection = if let Some(inspection) = cached_image_inspection(&flash, disk_budget_bytes) {
        inspection
    } else {
        match tokio::time::timeout(
            Duration::from_secs(30),
            context
                .image_inspector
                .inspect(&flash.spec.workload.image, disk_budget_bytes),
        )
        .await
        {
            Ok(Ok(inspection)) => inspection,
            Ok(Err(error)) if error.retryable() => {
                patch_status_if_changed(
                    &services,
                    &name,
                    flash.status.as_ref(),
                    FlashServiceStatus {
                        phase: FlashServicePhase::Provisioning,
                        observed_generation: flash.spec.desired_generation,
                        desired_replicas,
                        runtime_class: RUNTIME_CLASS_NAME.into(),
                        message: Some(format!("waiting for image inspection: {error}")),
                        ..FlashServiceStatus::default()
                    },
                )
                .await?;
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
            Ok(Err(error)) => {
                suspend_deployment(&context.client, &context.namespace, &name).await?;
                patch_status_if_changed(
                    &services,
                    &name,
                    flash.status.as_ref(),
                    FlashServiceStatus {
                        phase: FlashServicePhase::Error,
                        observed_generation: flash.spec.desired_generation,
                        desired_replicas,
                        runtime_class: RUNTIME_CLASS_NAME.into(),
                        message: Some(error.to_string()),
                        ..FlashServiceStatus::default()
                    },
                )
                .await?;
                return Ok(Action::await_change());
            }
            Err(_) => {
                patch_status_if_changed(
                    &services,
                    &name,
                    flash.status.as_ref(),
                    FlashServiceStatus {
                        phase: FlashServicePhase::Provisioning,
                        observed_generation: flash.spec.desired_generation,
                        desired_replicas,
                        runtime_class: RUNTIME_CLASS_NAME.into(),
                        message: Some(
                            "waiting for image inspection: OCI registry request timed out".into(),
                        ),
                        ..FlashServiceStatus::default()
                    },
                )
                .await?;
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
        }
    };

    let owner = flash
        .controller_owner_ref(&())
        .ok_or(ReconcileError::MissingOwnerReference)?;
    let deployment = desired_deployment(
        &flash,
        &owner,
        &inspection.resolved_image,
        inspection.writable_storage_bytes,
        context.registry_pull_secret.as_deref(),
    )?;
    let desired_network_service = desired_service(&flash, &owner)?;
    let network_policy = desired_network_policy(&flash, &owner)?;
    let deployments = Api::<Deployment>::namespaced(context.client.clone(), &context.namespace);
    let network_services = Api::<Service>::namespaced(context.client.clone(), &context.namespace);
    let network_policies =
        Api::<NetworkPolicy>::namespaced(context.client.clone(), &context.namespace);
    let params = PatchParams::apply(FIELD_MANAGER).force();

    if let Some(network_policy) = &network_policy {
        network_policies
            .patch(&name, &params, &Patch::Apply(network_policy))
            .await?;
    }
    let deployment = deployments
        .patch(&name, &params, &Patch::Apply(&deployment))
        .await?;
    let network_service = if let Some(network_service) = &desired_network_service {
        Some(
            network_services
                .patch(&name, &params, &Patch::Apply(network_service))
                .await?,
        )
    } else {
        match network_services
            .delete(&name, &DeleteParams::default())
            .await
        {
            Ok(_) => {}
            Err(kube::Error::Api(response)) if response.code == 404 => {}
            Err(error) => return Err(error.into()),
        }
        None
    };
    if network_policy.is_none() {
        match network_policies
            .delete(&name, &DeleteParams::default())
            .await
        {
            Ok(_) => {}
            Err(kube::Error::Api(response)) if response.code == 404 => {}
            Err(error) => return Err(error.into()),
        }
    }

    let ready_replicas = deployment
        .status
        .as_ref()
        .and_then(|status| status.ready_replicas)
        .unwrap_or(0);
    let endpoints = network_service
        .as_ref()
        .map(|service| service_endpoints(&flash, service))
        .unwrap_or_default();
    let endpoint_ready = flash.spec.workload.ports.is_empty() || !endpoints.is_empty();
    let ready = ready_replicas == desired_replicas && endpoint_ready;
    let pods = Api::<Pod>::namespaced(context.client.clone(), &context.namespace)
        .list(&ListParams::default().labels(&format!(
            "flash.heterocloud.io/instance={}",
            flash.spec.service_instance_id
        )))
        .await?;
    let failure = workload_failure_message(&pods.items);
    let status = FlashServiceStatus {
        phase: if failure.is_some() {
            FlashServicePhase::Error
        } else if ready {
            FlashServicePhase::Ready
        } else {
            FlashServicePhase::Provisioning
        },
        observed_generation: flash.spec.desired_generation,
        ready_replicas,
        desired_replicas,
        runtime_class: RUNTIME_CLASS_NAME.into(),
        endpoints,
        message: failure.or_else(|| {
            (!ready).then(|| {
                if flash.spec.workload.ports.is_empty() {
                    format!("waiting for {desired_replicas} gVisor replicas")
                } else {
                    format!(
                        "waiting for {desired_replicas} gVisor replicas and a routable service endpoint"
                    )
                }
            })
        }),
        resolved_image: Some(inspection.resolved_image),
        image_size_bytes: Some(inspection.image_size_bytes),
        writable_storage_bytes: Some(inspection.writable_storage_bytes),
    };
    let settled = status.phase == FlashServicePhase::Ready;
    patch_status_if_changed(&services, &name, flash.status.as_ref(), status).await?;
    if settled {
        Ok(Action::await_change())
    } else {
        Ok(Action::requeue(Duration::from_secs(5)))
    }
}

fn error_policy(
    flash: Arc<FlashService>,
    error: &ReconcileError,
    _context: Arc<ControllerContext>,
) -> Action {
    warn!(name = %flash.name_any(), error = %error, "FlashService will be retried");
    Action::requeue(Duration::from_secs(5))
}

async fn patch_status(
    services: &Api<FlashService>,
    name: &str,
    status: FlashServiceStatus,
) -> Result<(), ReconcileError> {
    services
        .patch_status(
            name,
            &PatchParams::default(),
            &Patch::Merge(json!({ "status": status })),
        )
        .await?;
    Ok(())
}

async fn patch_status_if_changed(
    services: &Api<FlashService>,
    name: &str,
    current: Option<&FlashServiceStatus>,
    status: FlashServiceStatus,
) -> Result<(), ReconcileError> {
    if current != Some(&status) {
        patch_status(services, name, status).await?;
    }
    Ok(())
}

fn desired_deployment(
    flash: &FlashService,
    owner: &OwnerReference,
    resolved_image: &str,
    writable_storage_bytes: u64,
    registry_pull_secret: Option<&str>,
) -> Result<Deployment, ReconcileError> {
    let name = flash.name_any();
    let workload = &flash.spec.workload;
    let mut labels = base_labels(flash);
    if workload.exposure.traffic_mode == TrafficMode::Direct {
        labels.insert(
            TRAFFIC_MODE_ANNOTATION.into(),
            TrafficMode::Direct.as_annotation().into(),
        );
    }
    let mut seen_ports = BTreeSet::new();
    let ports = workload
        .ports
        .iter()
        .filter(|port| seen_ports.insert((port.container_port, port.protocol)))
        .map(|port| {
            json!({
                "name": port.name,
                "containerPort": port.container_port,
                "protocol": port.protocol.as_kubernetes(),
            })
        })
        .collect::<Vec<_>>();
    let env = workload
        .env
        .iter()
        .map(|(name, value)| json!({"name": name, "value": value}))
        .collect::<Vec<_>>();
    let mut container = json!({
        "name": "workload",
        "image": resolved_image,
        "imagePullPolicy": "IfNotPresent",
        "ports": ports,
        "env": env,
        "resources": {
            "requests": {
                "cpu": format!("{}m", workload.cpu_millis),
                "memory": format!("{}Mi", workload.memory_mib),
                "ephemeral-storage": writable_storage_bytes.to_string(),
            },
            "limits": {
                "cpu": format!("{}m", workload.cpu_millis),
                "memory": format!("{}Mi", workload.memory_mib),
                "ephemeral-storage": writable_storage_bytes.to_string(),
            }
        },
        "securityContext": {
            "runAsNonRoot": false,
            "runAsUser": 0,
            "runAsGroup": 0,
        }
    });
    if !workload.command.is_empty() {
        container["command"] = json!(workload.command);
    }
    if !workload.args.is_empty() {
        container["args"] = json!(workload.args);
    }
    let mut pod_spec = json!({
        "runtimeClassName": RUNTIME_CLASS_NAME,
        "automountServiceAccountToken": false,
        "enableServiceLinks": false,
        "terminationGracePeriodSeconds": 30,
        "securityContext": {
            "runAsNonRoot": false,
            "runAsUser": 0,
            "runAsGroup": 0,
            "seccompProfile": {"type": "RuntimeDefault"}
        },
        "topologySpreadConstraints": [{
            "maxSkew": 1,
            "topologyKey": "kubernetes.io/hostname",
            "whenUnsatisfiable": "ScheduleAnyway",
            "labelSelector": {"matchLabels": {"flash.heterocloud.io/instance": flash.spec.service_instance_id}}
        }],
        "containers": [container]
    });
    if let Some(secret) = registry_pull_secret {
        pod_spec["imagePullSecrets"] = json!([{"name": secret}]);
    }
    from_value(json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "labels": base_labels(flash),
            "ownerReferences": [owner],
        },
        "spec": {
            "replicas": workload.replicas,
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1}
            },
            "selector": {"matchLabels": {"flash.heterocloud.io/instance": flash.spec.service_instance_id}},
            "template": {
                "metadata": {"labels": labels},
                "spec": pod_spec
            }
        }
    }))
}

fn cached_image_inspection(
    flash: &FlashService,
    disk_budget_bytes: u64,
) -> Option<ImageInspection> {
    let status = flash.status.as_ref()?;
    if status.observed_generation != flash.spec.desired_generation {
        return None;
    }
    let resolved_image = status.resolved_image.clone()?;
    let image_size_bytes = status.image_size_bytes?;
    let writable_storage_bytes = status.writable_storage_bytes?;
    let expected_writable = disk_budget_bytes.checked_sub(image_size_bytes)?;
    if image_size_bytes >= disk_budget_bytes || writable_storage_bytes != expected_writable {
        return None;
    }
    Some(ImageInspection {
        resolved_image,
        image_size_bytes,
        writable_storage_bytes,
    })
}

async fn suspend_deployment(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Result<(), ReconcileError> {
    let deployments = Api::<Deployment>::namespaced(client.clone(), namespace);
    if deployments.get_opt(name).await?.is_some_and(|deployment| {
        deployment.spec.as_ref().and_then(|spec| spec.replicas) != Some(0)
    }) {
        deployments
            .patch(
                name,
                &PatchParams::default(),
                &Patch::Merge(json!({"spec": {"replicas": 0}})),
            )
            .await?;
    }
    Ok(())
}

fn workload_failure_message(pods: &[Pod]) -> Option<String> {
    const FAILURE_REASONS: &[&str] = &[
        "CreateContainerConfigError",
        "CrashLoopBackOff",
        "ErrImagePull",
        "ImagePullBackOff",
        "InvalidImageName",
        "RunContainerError",
    ];

    for pod in pods {
        let Some(status) = pod.status.as_ref() else {
            continue;
        };
        if let Some(condition) = status.conditions.as_ref().and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.type_ == "PodScheduled"
                    && condition.status == "False"
                    && condition.reason.as_deref() == Some("Unschedulable")
            })
        }) {
            return Some(format!(
                "pod {} is unschedulable: {}",
                pod.name_any(),
                condition.message.as_deref().unwrap_or("no eligible node")
            ));
        }
        for container in status.container_statuses.iter().flatten() {
            if let Some(terminated) = container
                .state
                .as_ref()
                .and_then(|state| state.terminated.as_ref())
                .or_else(|| {
                    container
                        .last_state
                        .as_ref()
                        .and_then(|state| state.terminated.as_ref())
                })
            {
                let detail = terminated
                    .message
                    .as_deref()
                    .or(terminated.reason.as_deref())
                    .unwrap_or("the image process stopped");
                return Some(if terminated.exit_code == 0 {
                    format!(
                        "pod {} exited: {detail}; configure a long-running command for this service",
                        pod.name_any()
                    )
                } else {
                    format!(
                        "pod {} exited with code {}: {detail}",
                        pod.name_any(),
                        terminated.exit_code
                    )
                });
            }
            let Some(waiting) = container
                .state
                .as_ref()
                .and_then(|state| state.waiting.as_ref())
            else {
                continue;
            };
            let Some(reason) = waiting
                .reason
                .as_deref()
                .filter(|reason| FAILURE_REASONS.contains(reason))
            else {
                continue;
            };
            return Some(format!(
                "pod {} cannot start ({reason}): {}",
                pod.name_any(),
                waiting
                    .message
                    .as_deref()
                    .unwrap_or("container startup failed")
            ));
        }
    }
    None
}

fn desired_service(
    flash: &FlashService,
    owner: &OwnerReference,
) -> Result<Option<Service>, ReconcileError> {
    let workload = &flash.spec.workload;
    if workload.ports.is_empty() {
        return Ok(None);
    }
    let ports = workload
        .ports
        .iter()
        .map(|port| {
            json!({
                "name": port.name,
                "port": port.service_port,
                "protocol": port.protocol.as_kubernetes(),
                "targetPort": port.container_port,
            })
        })
        .collect::<Vec<_>>();
    let mut annotations = BTreeMap::new();
    let (kind, load_balancer_class, external_traffic_policy) =
        if workload.exposure.kind == ExposureType::Public {
            annotations.insert(
                TRAFFIC_MODE_ANNOTATION,
                workload.exposure.traffic_mode.as_annotation(),
            );
            (
                "LoadBalancer",
                Some(LOAD_BALANCER_CLASS),
                Some(match workload.exposure.traffic_mode {
                    TrafficMode::Forwarded => "Cluster",
                    TrafficMode::Direct => "Local",
                }),
            )
        } else {
            ("ClusterIP", None, None)
        };
    let mut spec = json!({
        "type": kind,
        "selector": {"flash.heterocloud.io/instance": flash.spec.service_instance_id},
        "ports": ports,
    });
    if let Some(value) = load_balancer_class {
        spec["allocateLoadBalancerNodePorts"] = json!(false);
        spec["loadBalancerClass"] = json!(value);
    }
    if let Some(value) = external_traffic_policy {
        spec["externalTrafficPolicy"] = json!(value);
    }
    if workload.exposure.kind == ExposureType::Public && workload.exposure.has_source_policy() {
        let mut source_ranges = workload
            .exposure
            .effective_source_networks()?
            .into_iter()
            .map(|network| network.to_string())
            .collect::<Vec<_>>();
        if source_ranges.is_empty() {
            source_ranges = vec!["0.0.0.0/32".into(), "::/128".into()];
        }
        spec["loadBalancerSourceRanges"] = json!(source_ranges);
    }
    Ok(Some(from_value(json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": flash.name_any(),
            "labels": base_labels(flash),
            "annotations": annotations,
            "ownerReferences": [owner],
        },
        "spec": spec,
    }))?))
}

fn desired_network_policy(
    flash: &FlashService,
    owner: &OwnerReference,
) -> Result<Option<NetworkPolicy>, ReconcileError> {
    let exposure = &flash.spec.workload.exposure;
    // Forwarded public traffic may be SNATed to the ingress node before a Pod
    // NetworkPolicy is evaluated. The Service firewall handles that path.
    if flash.spec.workload.ports.is_empty()
        || !exposure.has_source_policy()
        || (exposure.kind == ExposureType::Public
            && exposure.traffic_mode == TrafficMode::Forwarded)
    {
        return Ok(None);
    }
    let sources = exposure
        .effective_source_networks()?
        .into_iter()
        .map(|network| json!({"ipBlock": {"cidr": network.to_string()}}))
        .collect::<Vec<_>>();
    let ingress = if sources.is_empty() {
        Vec::new()
    } else {
        vec![json!({"from": sources})]
    };
    Ok(Some(from_value(json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": flash.name_any(),
            "labels": base_labels(flash),
            "ownerReferences": [owner],
        },
        "spec": {
            "podSelector": {
                "matchLabels": {
                    "flash.heterocloud.io/instance": flash.spec.service_instance_id
                }
            },
            "policyTypes": ["Ingress"],
            "ingress": ingress,
        }
    }))?))
}

fn service_endpoints(flash: &FlashService, service: &Service) -> Vec<FlashEndpoint> {
    let hosts = match flash.spec.workload.exposure.kind {
        ExposureType::Internal => service
            .spec
            .as_ref()
            .and_then(|spec| spec.cluster_ip.as_deref())
            .filter(|value| !value.is_empty() && *value != "None")
            .map(|_| {
                vec![format!(
                    "{}.{}.svc.cluster.local",
                    flash.name_any(),
                    flash.namespace().as_deref().unwrap_or("default")
                )]
            })
            .unwrap_or_default(),
        ExposureType::Public => service
            .status
            .as_ref()
            .and_then(|status| status.load_balancer.as_ref())
            .and_then(|status| status.ingress.as_ref())
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|entry| entry.ip.clone().or_else(|| entry.hostname.clone()))
                    .collect()
            })
            .unwrap_or_default(),
    };
    hosts
        .into_iter()
        .flat_map(|host| {
            flash
                .spec
                .workload
                .ports
                .iter()
                .map(move |port| FlashEndpoint {
                    name: port.name.clone(),
                    protocol: port.protocol,
                    host: host.clone(),
                    port: port.service_port,
                })
        })
        .collect()
}

fn base_labels(flash: &FlashService) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app.kubernetes.io/name".into(), "heterocloud-flash".into()),
        ("app.kubernetes.io/managed-by".into(), FIELD_MANAGER.into()),
        (
            "flash.heterocloud.io/instance".into(),
            flash.spec.service_instance_id.clone(),
        ),
        (
            "flash.heterocloud.io/organization".into(),
            flash.spec.organization_id.clone(),
        ),
    ])
}

fn from_value<T>(value: Value) -> Result<T, ReconcileError>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(value)
        .context("construct Kubernetes resource")
        .map_err(ReconcileError::Resource)
}

impl TrafficMode {
    const fn as_annotation(self) -> &'static str {
        match self {
            Self::Forwarded => "forwarded",
            Self::Direct => "direct",
        }
    }
}

#[derive(Debug, Error)]
pub enum ReconcileError {
    #[error("FlashService is missing a controller owner reference")]
    MissingOwnerReference,
    #[error("replica count cannot be represented by Kubernetes")]
    InvalidReplicaCount,
    #[error("disk budget cannot be represented in bytes")]
    StorageBudgetOverflow,
    #[error(transparent)]
    Kubernetes(#[from] kube::Error),
    #[error(transparent)]
    Resource(#[from] anyhow::Error),
    #[error(transparent)]
    Validation(#[from] ValidationError),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use k8s_openapi::api::core::v1::{Pod, Service};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
    use serde_json::json;

    use super::{
        desired_deployment, desired_network_policy, desired_service, workload_failure_message,
    };
    use crate::{
        LOAD_BALANCER_CLASS, RUNTIME_CLASS_NAME,
        crd::{FlashService, FlashServiceSpec},
        domain::{
            ExposureType, FlashExposure, FlashPort, FlashSpec, TrafficMode, TransportProtocol,
        },
    };

    fn service(mode: TrafficMode) -> FlashService {
        FlashService::new(
            "flash-00000000-0000-0000-0000-000000000001",
            FlashServiceSpec {
                desired_generation: 1,
                display_name: "UDP echo".into(),
                organization_id: "00000000-0000-0000-0000-000000000002".into(),
                project_id: "00000000-0000-0000-0000-000000000003".into(),
                service_instance_id: "00000000-0000-0000-0000-000000000001".into(),
                workload: FlashSpec {
                    region: "heteronet-global".into(),
                    image: "example.invalid/udp:v1".into(),
                    replicas: 3,
                    cpu_millis: 250,
                    memory_mib: 128,
                    ephemeral_storage_gib: 10,
                    ports: vec![FlashPort {
                        name: "game-udp".into(),
                        protocol: TransportProtocol::Udp,
                        container_port: 7777,
                        service_port: 7777,
                    }],
                    exposure: FlashExposure {
                        kind: ExposureType::Public,
                        traffic_mode: mode,
                        allowed_source_cidrs: Vec::new(),
                        denied_source_cidrs: Vec::new(),
                    },
                    env: BTreeMap::new(),
                    command: Vec::new(),
                    args: Vec::new(),
                    metadata: BTreeMap::new(),
                },
            },
        )
    }

    fn owner() -> OwnerReference {
        OwnerReference {
            api_version: "flash.heterocloud.io/v1alpha1".into(),
            block_owner_deletion: Some(true),
            controller: Some(true),
            kind: "FlashService".into(),
            name: "flash-test".into(),
            uid: "test-uid".into(),
        }
    }

    fn exposed_service(flash: &FlashService) -> Result<Service, Box<dyn std::error::Error>> {
        desired_service(flash, &owner())?
            .ok_or_else(|| std::io::Error::other("expected a Kubernetes Service").into())
    }

    #[test]
    fn workload_is_forced_through_gvisor_and_keeps_udp() -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_value(desired_deployment(
            &service(TrafficMode::Forwarded),
            &owner(),
            "example.invalid/udp@sha256:verified",
            10 * 1024 * 1024 * 1024 - 600,
            Some("heterocloud-registry-pull"),
        )?)?;
        assert_eq!(
            value.pointer("/spec/template/spec/runtimeClassName"),
            Some(&json!(RUNTIME_CLASS_NAME))
        );
        assert_eq!(
            value.pointer("/spec/template/spec/containers/0/ports/0/protocol"),
            Some(&json!("UDP"))
        );
        assert_eq!(
            value.pointer("/spec/template/spec/hostNetwork"),
            None,
            "Flash must not bypass gVisor netstack"
        );
        assert_eq!(
            value.pointer(
                "/spec/template/spec/containers/0/securityContext/allowPrivilegeEscalation"
            ),
            None
        );
        assert_eq!(
            value.pointer("/spec/template/spec/containers/0/securityContext/capabilities/drop/0"),
            None
        );
        assert_eq!(
            value.pointer("/spec/template/spec/containers/0/securityContext/runAsNonRoot"),
            Some(&json!(false))
        );
        assert_eq!(
            value.pointer("/spec/template/spec/containers/0/securityContext/runAsUser"),
            Some(&json!(0))
        );
        assert_eq!(
            value.pointer("/spec/template/spec/containers/0/securityContext/runAsGroup"),
            Some(&json!(0))
        );
        assert_eq!(
            value.pointer("/spec/template/spec/securityContext/runAsNonRoot"),
            Some(&json!(false))
        );
        assert_eq!(
            value.pointer("/spec/template/spec/securityContext/runAsUser"),
            Some(&json!(0))
        );
        assert_eq!(
            value.pointer("/spec/template/spec/securityContext/runAsGroup"),
            Some(&json!(0))
        );
        assert_eq!(
            value.pointer("/spec/template/spec/securityContext/seccompProfile/type"),
            Some(&json!("RuntimeDefault"))
        );
        assert_eq!(
            value.pointer("/spec/template/spec/containers/0/resources/requests/ephemeral-storage"),
            Some(&json!((10_u64 * 1024 * 1024 * 1024 - 600).to_string()))
        );
        assert_eq!(
            value.pointer("/spec/template/spec/containers/0/resources/limits/ephemeral-storage"),
            Some(&json!((10_u64 * 1024 * 1024 * 1024 - 600).to_string()))
        );
        assert_eq!(
            value.pointer("/spec/template/spec/imagePullSecrets/0/name"),
            Some(&json!("heterocloud-registry-pull"))
        );
        assert_eq!(
            value.pointer("/spec/template/spec/containers/0/image"),
            Some(&json!("example.invalid/udp@sha256:verified"))
        );
        Ok(())
    }

    #[test]
    fn multiple_endpoints_can_share_a_container_port() -> Result<(), Box<dyn std::error::Error>> {
        let mut flash = service(TrafficMode::Forwarded);
        flash.spec.workload.ports.push(FlashPort {
            name: "alternate-udp".into(),
            protocol: TransportProtocol::Udp,
            container_port: 7777,
            service_port: 30_001,
        });

        let deployment = serde_json::to_value(desired_deployment(
            &flash,
            &owner(),
            "example.invalid/udp@sha256:verified",
            1024,
            None,
        )?)?;
        let service = serde_json::to_value(exposed_service(&flash)?)?;

        assert_eq!(
            deployment.pointer("/spec/template/spec/containers/0/ports"),
            Some(&json!([{
                "name": "game-udp",
                "containerPort": 7777,
                "protocol": "UDP"
            }]))
        );
        assert_eq!(
            service.pointer("/spec/ports/0/targetPort"),
            Some(&json!(7777))
        );
        assert_eq!(
            service.pointer("/spec/ports/1/targetPort"),
            Some(&json!(7777))
        );
        Ok(())
    }

    #[test]
    fn reports_terminal_container_startup_failures() -> Result<(), Box<dyn std::error::Error>> {
        let pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "flash-test-abc"},
            "status": {
                "containerStatuses": [{
                    "name": "workload",
                    "image": "example.invalid/test:v1",
                    "imageID": "",
                    "ready": false,
                    "restartCount": 0,
                    "started": false,
                    "state": {
                        "waiting": {
                            "reason": "CreateContainerConfigError",
                            "message": "image configuration is incompatible"
                        }
                    }
                }]
            }
        }))?;
        let Some(message) = workload_failure_message(&[pod]) else {
            return Err(std::io::Error::other("missing startup failure").into());
        };
        assert!(message.contains("CreateContainerConfigError"));
        assert!(message.contains("image configuration is incompatible"));
        Ok(())
    }

    #[test]
    fn ignores_transient_container_creation() -> Result<(), Box<dyn std::error::Error>> {
        let pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "flash-test-abc"},
            "status": {
                "containerStatuses": [{
                    "name": "workload",
                    "image": "example.invalid/test:v1",
                    "imageID": "",
                    "ready": false,
                    "restartCount": 0,
                    "started": false,
                    "state": {"waiting": {"reason": "ContainerCreating"}}
                }]
            }
        }))?;
        assert_eq!(workload_failure_message(&[pod]), None);
        Ok(())
    }

    #[test]
    fn reports_images_whose_default_process_exits() -> Result<(), Box<dyn std::error::Error>> {
        let pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "flash-test-abc"},
            "status": {
                "containerStatuses": [{
                    "name": "workload",
                    "image": "ubuntu:22.04",
                    "imageID": "example",
                    "ready": false,
                    "restartCount": 1,
                    "started": false,
                    "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                    "lastState": {
                        "terminated": {
                            "containerID": "containerd://example",
                            "exitCode": 0,
                            "finishedAt": "2026-08-21T00:00:01Z",
                            "reason": "Completed",
                            "startedAt": "2026-08-21T00:00:00Z"
                        }
                    }
                }]
            }
        }))?;
        let Some(message) = workload_failure_message(&[pod]) else {
            return Err(std::io::Error::other("missing process exit failure").into());
        };
        assert!(message.contains("configure a long-running command"));
        Ok(())
    }

    #[test]
    fn public_service_uses_heteronetwork_forwarding_policy()
    -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_value(exposed_service(&service(TrafficMode::Forwarded))?)?;
        assert_eq!(
            value.pointer("/spec/loadBalancerClass"),
            Some(&json!(LOAD_BALANCER_CLASS))
        );
        assert_eq!(
            value.pointer("/spec/externalTrafficPolicy"),
            Some(&json!("Cluster"))
        );
        assert_eq!(
            value.pointer("/spec/allocateLoadBalancerNodePorts"),
            Some(&json!(false))
        );
        assert_eq!(
            value.pointer("/metadata/annotations/networking.heteronetwork.io~1traffic-mode"),
            Some(&json!("forwarded"))
        );
        Ok(())
    }

    #[test]
    fn source_policy_updates_load_balancer_and_pod_firewalls()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut flash = service(TrafficMode::Direct);
        flash.spec.workload.exposure.allowed_source_cidrs = vec!["192.0.2.0/24".into()];
        flash.spec.workload.exposure.denied_source_cidrs = vec!["192.0.2.128/25".into()];

        let service = serde_json::to_value(exposed_service(&flash)?)?;
        assert_eq!(
            service.pointer("/spec/loadBalancerSourceRanges"),
            Some(&json!(["192.0.2.0/25"]))
        );

        let policy = desired_network_policy(&flash, &owner())?
            .ok_or_else(|| std::io::Error::other("source policy was not created"))?;
        let policy = serde_json::to_value(policy)?;
        assert_eq!(
            policy.pointer("/spec/policyTypes"),
            Some(&json!(["Ingress"]))
        );
        assert_eq!(
            policy.pointer("/spec/ingress/0/from/0/ipBlock/cidr"),
            Some(&json!("192.0.2.0/25"))
        );
        Ok(())
    }

    #[test]
    fn service_without_source_policy_does_not_install_a_firewall()
    -> Result<(), Box<dyn std::error::Error>> {
        let flash = service(TrafficMode::Forwarded);
        let service = serde_json::to_value(exposed_service(&flash)?)?;
        assert_eq!(service.pointer("/spec/loadBalancerSourceRanges"), None);
        assert!(desired_network_policy(&flash, &owner())?.is_none());
        Ok(())
    }

    #[test]
    fn forwarded_public_policy_is_enforced_before_source_nat()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut flash = service(TrafficMode::Forwarded);
        flash.spec.workload.exposure.denied_source_cidrs = vec!["198.51.100.0/24".into()];
        let service = serde_json::to_value(exposed_service(&flash)?)?;
        assert!(service.pointer("/spec/loadBalancerSourceRanges").is_some());
        assert!(desired_network_policy(&flash, &owner())?.is_none());
        Ok(())
    }

    #[test]
    fn service_without_endpoints_has_no_network_resources() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut flash = service(TrafficMode::Forwarded);
        flash.spec.workload.ports.clear();

        assert!(desired_service(&flash, &owner())?.is_none());
        assert!(desired_network_policy(&flash, &owner())?.is_none());
        let deployment = serde_json::to_value(desired_deployment(
            &flash,
            &owner(),
            "example.invalid/udp@sha256:verified",
            1024,
            None,
        )?)?;
        assert_eq!(
            deployment.pointer("/spec/template/spec/containers/0/ports"),
            Some(&json!([]))
        );
        Ok(())
    }
}
