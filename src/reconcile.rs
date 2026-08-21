use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use k8s_openapi::{
    api::{apps::v1::Deployment, core::v1::Service},
    apimachinery::pkg::apis::meta::v1::OwnerReference,
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{Patch, PatchParams},
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
    domain::{ExposureType, TrafficMode},
};

const FIELD_MANAGER: &str = "heterocloud-flash-controller";

#[derive(Clone)]
pub struct ControllerContext {
    client: Client,
    namespace: String,
}

impl ControllerContext {
    #[must_use]
    pub fn new(client: Client, namespace: String) -> Self {
        Self { client, namespace }
    }
}

pub async fn run_controller(client: Client, namespace: String) -> Result<()> {
    let services = Api::<FlashService>::namespaced(client.clone(), &namespace);
    let deployments = Api::<Deployment>::namespaced(client.clone(), &namespace);
    let network_services = Api::<Service>::namespaced(client.clone(), &namespace);
    let context = Arc::new(ControllerContext::new(client, namespace));

    info!("FlashService controller started");
    Controller::new(services, watcher::Config::default())
        .owns(deployments, watcher::Config::default())
        .owns(network_services, watcher::Config::default())
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
        patch_status(
            &services,
            &name,
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

    let owner = flash
        .controller_owner_ref(&())
        .ok_or(ReconcileError::MissingOwnerReference)?;
    let deployment = desired_deployment(&flash, &owner)?;
    let network_service = desired_service(&flash, &owner)?;
    let deployments = Api::<Deployment>::namespaced(context.client.clone(), &context.namespace);
    let network_services = Api::<Service>::namespaced(context.client.clone(), &context.namespace);
    let params = PatchParams::apply(FIELD_MANAGER).force();

    let deployment = deployments
        .patch(&name, &params, &Patch::Apply(&deployment))
        .await?;
    let network_service = network_services
        .patch(&name, &params, &Patch::Apply(&network_service))
        .await?;

    let desired_replicas = i32::try_from(flash.spec.workload.replicas)
        .map_err(|_| ReconcileError::InvalidReplicaCount)?;
    let ready_replicas = deployment
        .status
        .as_ref()
        .and_then(|status| status.ready_replicas)
        .unwrap_or(0);
    let endpoints = service_endpoints(&flash, &network_service);
    let endpoint_ready = match flash.spec.workload.exposure.kind {
        ExposureType::Internal => !endpoints.is_empty(),
        ExposureType::Public => !endpoints.is_empty(),
    };
    let ready = ready_replicas == desired_replicas && endpoint_ready;
    let status = FlashServiceStatus {
        phase: if ready {
            FlashServicePhase::Ready
        } else {
            FlashServicePhase::Provisioning
        },
        observed_generation: flash.spec.desired_generation,
        ready_replicas,
        desired_replicas,
        runtime_class: RUNTIME_CLASS_NAME.into(),
        endpoints,
        message: (!ready).then(|| {
            format!(
                "waiting for {desired_replicas} gVisor replicas and a routable service endpoint"
            )
        }),
    };
    if flash.status.as_ref() != Some(&status) {
        patch_status(&services, &name, status).await?;
    }
    Ok(Action::requeue(Duration::from_secs(5)))
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

fn desired_deployment(
    flash: &FlashService,
    owner: &OwnerReference,
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
    let ports = workload
        .ports
        .iter()
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
        "image": workload.image,
        "imagePullPolicy": "IfNotPresent",
        "ports": ports,
        "env": env,
        "resources": {
            "requests": {
                "cpu": format!("{}m", workload.cpu_millis),
                "memory": format!("{}Mi", workload.memory_mib),
            },
            "limits": {
                "cpu": format!("{}m", workload.cpu_millis),
                "memory": format!("{}Mi", workload.memory_mib),
            }
        },
        "securityContext": {
            "allowPrivilegeEscalation": false,
            "runAsNonRoot": true,
            "capabilities": {"drop": ["ALL"]},
        }
    });
    if !workload.command.is_empty() {
        container["command"] = json!(workload.command);
    }
    if !workload.args.is_empty() {
        container["args"] = json!(workload.args);
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
                "spec": {
                    "runtimeClassName": RUNTIME_CLASS_NAME,
                    "automountServiceAccountToken": false,
                    "enableServiceLinks": false,
                    "terminationGracePeriodSeconds": 30,
                    "securityContext": {
                        "runAsNonRoot": true,
                        "seccompProfile": {"type": "RuntimeDefault"}
                    },
                    "topologySpreadConstraints": [{
                        "maxSkew": 1,
                        "topologyKey": "kubernetes.io/hostname",
                        "whenUnsatisfiable": "ScheduleAnyway",
                        "labelSelector": {"matchLabels": {"flash.heterocloud.io/instance": flash.spec.service_instance_id}}
                    }],
                    "containers": [container]
                }
            }
        }
    }))
}

fn desired_service(
    flash: &FlashService,
    owner: &OwnerReference,
) -> Result<Service, ReconcileError> {
    let workload = &flash.spec.workload;
    let ports = workload
        .ports
        .iter()
        .map(|port| {
            json!({
                "name": port.name,
                "port": port.service_port,
                "protocol": port.protocol.as_kubernetes(),
                "targetPort": port.name,
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
    from_value(json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": flash.name_any(),
            "labels": base_labels(flash),
            "annotations": annotations,
            "ownerReferences": [owner],
        },
        "spec": spec,
    }))
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
    #[error(transparent)]
    Kubernetes(#[from] kube::Error),
    #[error(transparent)]
    Resource(#[from] anyhow::Error),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
    use serde_json::json;

    use super::{desired_deployment, desired_service};
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
                    ports: vec![FlashPort {
                        name: "game-udp".into(),
                        protocol: TransportProtocol::Udp,
                        container_port: 7777,
                        service_port: 7777,
                    }],
                    exposure: FlashExposure {
                        kind: ExposureType::Public,
                        traffic_mode: mode,
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

    #[test]
    fn workload_is_forced_through_gvisor_and_keeps_udp() -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_value(desired_deployment(
            &service(TrafficMode::Forwarded),
            &owner(),
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
        Ok(())
    }

    #[test]
    fn public_service_uses_heteronetwork_forwarding_policy()
    -> Result<(), Box<dyn std::error::Error>> {
        let value =
            serde_json::to_value(desired_service(&service(TrafficMode::Forwarded), &owner())?)?;
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
}
