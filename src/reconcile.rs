use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use ipnet::IpNet;
use k8s_openapi::{
    api::{
        apps::v1::Deployment,
        autoscaling::v2::HorizontalPodAutoscaler,
        core::v1::{Node, PersistentVolumeClaim, Pod, Service},
        networking::v1::NetworkPolicy,
    },
    apimachinery::pkg::apis::meta::v1::OwnerReference,
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{DeleteParams, ListParams, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        reflector::ObjectRef,
        watcher,
    },
};
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    LOAD_BALANCER_CLASS, RUNTIME_CLASS_NAME, TRAFFIC_MODE_ANNOTATION,
    crd::{FlashEndpoint, FlashService, FlashServicePhase, FlashServiceStatus},
    domain::{EndpointMode, ExposureType, TrafficMode, ValidationError},
    image::{GIB_BYTES, ImageInspection, ImageInspector},
};

const FIELD_MANAGER: &str = "heterocloud-flash-controller";
const REPLICAS_MANAGER: &str = "heterocloud-flash-replicas";
const DNS_HOSTNAME: &str = "external-dns.alpha.kubernetes.io/hostname";
const DNS_PUBLISH_LABEL: &str = "dns.heterocloud.io/publish";
const GENERATION_LABEL: &str = "flash.heterocloud.io/generation";
const ASSIGNED_NODES_ANNOTATION: &str = "networking.heteronetwork.io/assigned-nodes";
const PERSISTENT_HOME_VOLUME: &str = "persistent-home";
const PERSISTENT_HOME_MOUNT_PATH: &str = "/root";
const MIB_BYTES: u64 = 1024 * 1024;
const MIN_PERSISTENT_STORAGE_BYTES: u64 = 64 * MIB_BYTES;
const MIN_ROOTFS_STORAGE_BYTES: u64 = 64 * MIB_BYTES;
const MAX_ROOTFS_STORAGE_BYTES: u64 = GIB_BYTES;
const MAX_ADMIN_VOLUME_MOUNTS_PER_SERVICE: usize = 8;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdminVolumeMount {
    pub name: String,
    pub claim_name: String,
    pub mount_path: String,
    #[serde(default)]
    pub read_only: bool,
}

pub type AdminVolumeMounts = BTreeMap<String, Vec<AdminVolumeMount>>;

pub fn validate_admin_volume_mounts(mounts: &AdminVolumeMounts) -> Result<()> {
    if mounts.len() > 256 {
        anyhow::bail!("admin volume mounts must target at most 256 services");
    }
    for (service_instance_id, service_mounts) in mounts {
        Uuid::parse_str(service_instance_id)
            .with_context(|| format!("invalid admin volume service ID {service_instance_id}"))?;
        if service_mounts.is_empty() || service_mounts.len() > MAX_ADMIN_VOLUME_MOUNTS_PER_SERVICE {
            anyhow::bail!(
                "admin volume service {service_instance_id} must contain between one and {MAX_ADMIN_VOLUME_MOUNTS_PER_SERVICE} mounts"
            );
        }
        let mut names = BTreeSet::new();
        let mut paths = BTreeSet::new();
        for mount in service_mounts {
            if mount.name == PERSISTENT_HOME_VOLUME || !valid_dns_subdomain(&mount.name, 63) {
                anyhow::bail!("invalid admin volume name for service {service_instance_id}");
            }
            if !valid_dns_subdomain(&mount.claim_name, 253) {
                anyhow::bail!("invalid admin volume claim for service {service_instance_id}");
            }
            if !valid_admin_mount_path(&mount.mount_path) {
                anyhow::bail!("invalid admin volume path for service {service_instance_id}");
            }
            if !names.insert(&mount.name) || !paths.insert(&mount.mount_path) {
                anyhow::bail!(
                    "duplicate admin volume name or path for service {service_instance_id}"
                );
            }
        }
    }
    Ok(())
}

fn valid_dns_subdomain(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label.bytes().all(|character| {
                    character.is_ascii_lowercase()
                        || character.is_ascii_digit()
                        || character == b'-'
                })
        })
}

fn valid_admin_mount_path(value: &str) -> bool {
    (value.starts_with("/root/") || value.starts_with("/mnt/"))
        && value.len() <= 4_096
        && !value.contains("//")
        && value
            .split('/')
            .skip(1)
            .all(|component| !component.is_empty() && component != "." && component != "..")
}

#[derive(Clone)]
pub struct ControllerContext {
    client: Client,
    namespace: String,
    image_inspector: ImageInspector,
    registry_pull_secret: Option<String>,
    persistent_storage_class: Option<String>,
    admin_volume_mounts: AdminVolumeMounts,
    additional_protected_networks: Vec<IpNet>,
    dns_networks: Vec<IpNet>,
    public_domain: Option<String>,
}

impl ControllerContext {
    #[must_use]
    pub fn new(
        client: Client,
        namespace: String,
        image_inspector: ImageInspector,
        registry_pull_secret: Option<String>,
        persistent_storage_class: Option<String>,
        admin_volume_mounts: AdminVolumeMounts,
        additional_protected_networks: Vec<IpNet>,
        dns_networks: Vec<IpNet>,
        public_domain: Option<String>,
    ) -> Self {
        Self {
            client,
            namespace,
            image_inspector,
            registry_pull_secret,
            persistent_storage_class,
            admin_volume_mounts,
            additional_protected_networks,
            dns_networks,
            public_domain,
        }
    }
}

pub async fn run_controller(
    client: Client,
    namespace: String,
    image_inspector: ImageInspector,
    registry_pull_secret: Option<String>,
    persistent_storage_class: Option<String>,
    admin_volume_mounts: AdminVolumeMounts,
    additional_protected_networks: Vec<IpNet>,
    dns_networks: Vec<IpNet>,
    public_domain: Option<String>,
) -> Result<()> {
    let services = Api::<FlashService>::namespaced(client.clone(), &namespace);
    let deployments = Api::<Deployment>::namespaced(client.clone(), &namespace);
    let network_services = Api::<Service>::namespaced(client.clone(), &namespace);
    let network_policies = Api::<NetworkPolicy>::namespaced(client.clone(), &namespace);
    let persistent_volume_claims =
        Api::<PersistentVolumeClaim>::namespaced(client.clone(), &namespace);
    let pods = Api::<Pod>::namespaced(client.clone(), &namespace);
    let autoscalers = Api::<HorizontalPodAutoscaler>::namespaced(client.clone(), &namespace);
    let context = Arc::new(ControllerContext::new(
        client,
        namespace,
        image_inspector,
        registry_pull_secret,
        persistent_storage_class,
        admin_volume_mounts,
        additional_protected_networks,
        dns_networks,
        public_domain,
    ));

    info!("FlashService controller started");
    Controller::new(services, watcher::Config::default())
        .owns(deployments, watcher::Config::default())
        .owns(autoscalers, watcher::Config::default())
        .owns(network_services, watcher::Config::default())
        .owns(network_policies, watcher::Config::default())
        .owns(persistent_volume_claims, watcher::Config::default())
        .watches(pods, watcher::Config::default(), flash_service_for_pod)
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

    if let Err(error) = flash
        .spec
        .workload
        .validate()
        .map_err(ReconcileError::from)
        .and_then(|()| public_hostname(&flash, context.public_domain.as_deref()).map(|_| ()))
    {
        patch_status_if_changed(
            &services,
            &flash,
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
                    &flash,
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
                    &flash,
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
                    &flash,
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
    let storage = storage_allocation(
        inspection.writable_storage_bytes,
        context.persistent_storage_class.is_some(),
    )?;
    let persistent_volume_claim = context
        .persistent_storage_class
        .as_deref()
        .map(|storage_class| {
            desired_persistent_volume_claim(&flash, &owner, storage_class, storage.persistent_bytes)
        })
        .transpose()?;
    let deployment = desired_deployment(
        &flash,
        &owner,
        &inspection.resolved_image,
        storage.rootfs_bytes,
        persistent_volume_claim
            .as_ref()
            .and_then(|claim| claim.metadata.name.as_deref()),
        context.registry_pull_secret.as_deref(),
        context
            .admin_volume_mounts
            .get(&flash.spec.service_instance_id)
            .map(Vec::as_slice)
            .unwrap_or_default(),
    )?;
    let hostname = public_hostname(&flash, context.public_domain.as_deref())?;
    let desired_network_service =
        desired_service_with_hostname(&flash, &owner, hostname.as_deref())?;
    let network_services = Api::<Service>::namespaced(context.client.clone(), &context.namespace);
    let network_policies =
        Api::<NetworkPolicy>::namespaced(context.client.clone(), &context.namespace);
    let persistent_volume_claims =
        Api::<PersistentVolumeClaim>::namespaced(context.client.clone(), &context.namespace);
    let params = PatchParams::apply(FIELD_MANAGER).force();
    let current_network_service = network_services.get_opt(&name).await?;
    let forwarded_ingress_networks = if flash.spec.workload.exposure.kind == ExposureType::Public
        && flash.spec.workload.exposure.traffic_mode == TrafficMode::Forwarded
    {
        assigned_forwarder_networks(&context.client, current_network_service.as_ref()).await?
    } else {
        Vec::new()
    };
    let network_policy = desired_network_policy_with_networks(
        &flash,
        &owner,
        &context.additional_protected_networks,
        &context.dns_networks,
        &forwarded_ingress_networks,
    )?;

    if let Some(persistent_volume_claim) = &persistent_volume_claim {
        persistent_volume_claims
            .patch(
                &persistent_volume_claim.name_any(),
                &params,
                &Patch::Apply(persistent_volume_claim),
            )
            .await?;
    }
    network_policies
        .patch(&name, &params, &Patch::Apply(&network_policy))
        .await?;
    let applied_deployment = reconcile_scaling(
        &context.client,
        &context.namespace,
        &flash,
        &owner,
        deployment,
    )
    .await?;
    let desired_replicas = applied_deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.replicas)
        .unwrap_or(desired_replicas);
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
    let endpoints = network_service
        .as_ref()
        .map(|service| service_endpoints(&flash, service, hostname.as_deref()))
        .unwrap_or_default();
    let endpoint_ready = flash.spec.workload.ports.is_empty() || !endpoints.is_empty();
    let pods = Api::<Pod>::namespaced(context.client.clone(), &context.namespace)
        .list(&ListParams::default().labels(&format!(
            "flash.heterocloud.io/instance={},{}={}",
            flash.spec.service_instance_id, GENERATION_LABEL, flash.spec.desired_generation
        )))
        .await?;
    let mut status = FlashServiceStatus {
        observed_generation: flash.spec.desired_generation,
        desired_replicas,
        runtime_class: RUNTIME_CLASS_NAME.into(),
        endpoints,
        resolved_image: Some(inspection.resolved_image),
        image_size_bytes: Some(inspection.image_size_bytes),
        writable_storage_bytes: Some(inspection.writable_storage_bytes),
        ..FlashServiceStatus::default()
    };
    let action = update_workload_status(
        &mut status,
        &pods.items,
        endpoint_ready,
        !flash.spec.workload.ports.is_empty(),
    );
    if hostname.is_some()
        && !status.endpoints.is_empty()
        && status.phase == FlashServicePhase::Ready
    {
        let note = "Load balancer allocated; DNS publication/resolution is not verified";
        status.message = Some(match status.message.take() {
            Some(message) => format!("{message}; {note}"),
            None => note.into(),
        });
    }
    patch_status_if_changed(&services, &flash, status).await?;
    Ok(action)
}

fn update_workload_status(
    status: &mut FlashServiceStatus,
    pods: &[Pod],
    endpoint_ready: bool,
    needs_endpoint: bool,
) -> Action {
    let desired_replicas = status.desired_replicas;
    status.ready_replicas =
        i32::try_from(pods.iter().filter(|pod| pod_is_ready(pod)).count()).unwrap_or(i32::MAX);
    let ready = status.ready_replicas == desired_replicas && endpoint_ready;
    let failure = workload_failure_message(pods);
    status.phase = if failure.is_some() {
        FlashServicePhase::Error
    } else if ready {
        FlashServicePhase::Ready
    } else {
        FlashServicePhase::Provisioning
    };
    status.message = failure.or_else(|| {
        if ready {
            workload_restart_message(pods)
        } else if let Some(message) = workload_pending_message(pods) {
            Some(message)
        } else if !needs_endpoint {
            Some(format!("waiting for {desired_replicas} gVisor replicas"))
        } else {
            Some(format!(
                "waiting for {desired_replicas} gVisor replicas and a routable service endpoint"
            ))
        }
    });
    if status.phase == FlashServicePhase::Ready {
        Action::await_change()
    } else {
        Action::requeue(Duration::from_secs(5))
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

fn status_patch(flash: &FlashService, status: &FlashServiceStatus) -> Value {
    let mut patch = json!({"status": status});
    // Merge patches must explicitly clear fields omitted by status serialization.
    patch["status"]["resolved_image"] = json!(status.resolved_image);
    patch["status"]["image_size_bytes"] = json!(status.image_size_bytes);
    patch["status"]["writable_storage_bytes"] = json!(status.writable_storage_bytes);
    if let Some(version) = &flash.metadata.resource_version {
        patch["metadata"] = json!({"resourceVersion": version});
    }
    patch
}

async fn patch_status_if_changed(
    services: &Api<FlashService>,
    flash: &FlashService,
    status: FlashServiceStatus,
) -> Result<(), ReconcileError> {
    if flash.status.as_ref() != Some(&status) {
        services
            .patch_status(
                &flash.name_any(),
                &PatchParams::default(),
                &Patch::Merge(status_patch(flash, &status)),
            )
            .await?;
    }
    Ok(())
}

fn desired_deployment(
    flash: &FlashService,
    owner: &OwnerReference,
    resolved_image: &str,
    rootfs_storage_bytes: u64,
    persistent_volume_claim: Option<&str>,
    registry_pull_secret: Option<&str>,
    admin_volume_mounts: &[AdminVolumeMount],
) -> Result<Deployment, ReconcileError> {
    let name = flash.name_any();
    let workload = &flash.spec.workload;
    let mut labels = base_labels(flash);
    labels.insert(
        GENERATION_LABEL.into(),
        flash.spec.desired_generation.to_string(),
    );
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
                "ephemeral-storage": rootfs_storage_bytes.to_string(),
            },
            "limits": {
                "cpu": format!("{}m", workload.cpu_millis),
                "memory": format!("{}Mi", workload.memory_mib),
                "ephemeral-storage": rootfs_storage_bytes.to_string(),
            }
        },
        "securityContext": {
            "runAsNonRoot": false,
            "runAsUser": 0,
            "runAsGroup": 0,
            "allowPrivilegeEscalation": false,
            "capabilities": {"drop": ["NET_RAW"]},
        }
    });
    if !workload.command.is_empty() {
        container["command"] = json!(workload.command);
    }
    if !workload.args.is_empty() {
        container["args"] = json!(workload.args);
    }
    let mut volume_mounts = Vec::new();
    if persistent_volume_claim.is_some() {
        volume_mounts.push(json!({
            "name": PERSISTENT_HOME_VOLUME,
            "mountPath": PERSISTENT_HOME_MOUNT_PATH,
        }));
    }
    volume_mounts.extend(admin_volume_mounts.iter().map(|mount| {
        json!({
            "name": mount.name,
            "mountPath": mount.mount_path,
            "readOnly": mount.read_only,
        })
    }));
    if !volume_mounts.is_empty() {
        container["volumeMounts"] = Value::Array(volume_mounts);
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
    let mut volumes = Vec::new();
    if let Some(claim_name) = persistent_volume_claim {
        volumes.push(json!({
            "name": PERSISTENT_HOME_VOLUME,
            "persistentVolumeClaim": {"claimName": claim_name},
        }));
    }
    volumes.extend(admin_volume_mounts.iter().map(|mount| {
        json!({
            "name": mount.name,
            "persistentVolumeClaim": {"claimName": mount.claim_name},
        })
    }));
    if !volumes.is_empty() {
        pod_spec["volumes"] = Value::Array(volumes);
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

#[derive(Clone, Copy, Debug, PartialEq)]
struct StorageAllocation {
    rootfs_bytes: u64,
    persistent_bytes: u64,
}

fn storage_allocation(
    writable_storage_bytes: u64,
    persistent: bool,
) -> Result<StorageAllocation, ReconcileError> {
    if !persistent {
        return Ok(StorageAllocation {
            rootfs_bytes: writable_storage_bytes,
            persistent_bytes: 0,
        });
    }
    let minimum = MIN_ROOTFS_STORAGE_BYTES
        .checked_add(MIN_PERSISTENT_STORAGE_BYTES)
        .ok_or(ReconcileError::StorageBudgetOverflow)?;
    if writable_storage_bytes < minimum {
        return Err(ReconcileError::InsufficientWritableStorage);
    }
    let rootfs_bytes = (writable_storage_bytes / 10)
        .clamp(MIN_ROOTFS_STORAGE_BYTES, MAX_ROOTFS_STORAGE_BYTES)
        .min(writable_storage_bytes - MIN_PERSISTENT_STORAGE_BYTES);
    Ok(StorageAllocation {
        rootfs_bytes,
        persistent_bytes: writable_storage_bytes - rootfs_bytes,
    })
}

fn desired_persistent_volume_claim(
    flash: &FlashService,
    owner: &OwnerReference,
    storage_class: &str,
    storage_bytes: u64,
) -> Result<PersistentVolumeClaim, ReconcileError> {
    from_value(json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": format!("{}-home", flash.name_any()),
            "labels": base_labels(flash),
            "ownerReferences": [owner],
        },
        "spec": {
            "accessModes": ["ReadWriteMany"],
            "storageClassName": storage_class,
            "volumeMode": "Filesystem",
            "resources": {
                "requests": {"storage": storage_bytes.to_string()},
            },
        },
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
    let autoscalers = Api::<HorizontalPodAutoscaler>::namespaced(client.clone(), namespace);
    if !delete_autoscaler(&autoscalers, name).await? {
        return Err(anyhow::anyhow!("waiting for autoscaler deletion before suspension").into());
    }
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

pub fn validate_public_domain(domain: &str) -> Result<()> {
    if !valid_dns_subdomain(domain, 214) || domain.parse::<std::net::IpAddr>().is_ok() {
        anyhow::bail!(
            "publicDomain must be a lowercase DNS suffix, without scheme, port, wildcard or trailing dot (maximum 214 characters)"
        );
    }
    Ok(())
}

fn public_hostname(
    flash: &FlashService,
    domain: Option<&str>,
) -> Result<Option<String>, ReconcileError> {
    if flash.spec.workload.exposure.endpoint_mode != EndpointMode::LoadBalancer {
        return Ok(None);
    }
    let domain =
        domain.ok_or_else(|| anyhow::anyhow!("load_balancer requires provider publicDomain"))?;
    validate_public_domain(domain)?;
    let id = Uuid::parse_str(&flash.spec.service_instance_id)
        .context("invalid service instance UUID")?;
    Ok(Some(format!("f-{id}.{domain}")))
}

fn desired_autoscaler(
    flash: &FlashService,
    owner: &OwnerReference,
) -> Result<Option<HorizontalPodAutoscaler>, ReconcileError> {
    let Some(scaling) = &flash.spec.workload.autoscaling else {
        return Ok(None);
    };
    let metrics = [("cpu", scaling.target_cpu_utilization_percent), ("memory", scaling.target_memory_utilization_percent)]
        .into_iter().filter_map(|(name, target)| target.map(|target| json!({
            "type": "Resource", "resource": {"name": name, "target": {"type": "Utilization", "averageUtilization": target}}
        }))).collect::<Vec<_>>();
    Ok(Some(from_value(json!({
        "apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
        "metadata": {"name": flash.name_any(), "labels": base_labels(flash), "ownerReferences": [owner]},
        "spec": {
            "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": flash.name_any()},
            "minReplicas": scaling.min_replicas, "maxReplicas": scaling.max_replicas,
            "metrics": metrics, "behavior": {"scaleDown": {"stabilizationWindowSeconds": 300}}
        }
    }))?))
}

async fn delete_autoscaler(
    api: &Api<HorizontalPodAutoscaler>,
    name: &str,
) -> Result<bool, ReconcileError> {
    if let Some(hpa) = api.get_opt(name).await? {
        if hpa.metadata.deletion_timestamp.is_none() {
            let params = DeleteParams {
                preconditions: Some(kube::api::Preconditions {
                    uid: hpa.metadata.uid,
                    resource_version: hpa.metadata.resource_version,
                }),
                ..DeleteParams::default()
            };
            match api.delete(name, &params).await {
                Ok(_) => {}
                Err(kube::Error::Api(response)) if response.code == 404 => {}
                Err(error) => return Err(error.into()),
            }
        }
        return Ok(api.get_opt(name).await?.is_none());
    }
    Ok(true)
}

fn legacy_owns_replicas(deployment: &Deployment) -> bool {
    deployment
        .metadata
        .managed_fields
        .iter()
        .flatten()
        .any(|entry| {
            entry.manager.as_deref() == Some(FIELD_MANAGER)
                && entry
                    .fields_v1
                    .as_ref()
                    .is_some_and(|fields| fields.0.pointer("/f:spec/f:replicas").is_some())
        })
}

async fn reconcile_scaling(
    client: &Client,
    namespace: &str,
    flash: &FlashService,
    owner: &OwnerReference,
    mut desired: Deployment,
) -> Result<Deployment, ReconcileError> {
    let name = flash.name_any();
    let deployments = Api::<Deployment>::namespaced(client.clone(), namespace);
    let autoscalers = Api::<HorizontalPodAutoscaler>::namespaced(client.clone(), namespace);
    let hpa = desired_autoscaler(flash, owner)?;
    if hpa.is_none() && !delete_autoscaler(&autoscalers, &name).await? {
        return Err(anyhow::anyhow!("waiting for autoscaler deletion before fixed scaling").into());
    }
    let params = PatchParams::apply(FIELD_MANAGER).force();
    let mut current = match deployments.get_opt(&name).await? {
        Some(current) => current,
        // Initialize once at the requested count, then relinquish main-manager ownership.
        None => {
            deployments
                .create(
                    &kube::api::PostParams {
                        field_manager: Some(FIELD_MANAGER.into()),
                        ..Default::default()
                    },
                    &desired,
                )
                .await?
        }
    };
    if hpa.is_none() || legacy_owns_replicas(&current) {
        let replicas = if hpa.is_none() {
            i32::try_from(flash.spec.workload.replicas)
                .map_err(|_| ReconcileError::InvalidReplicaCount)?
        } else {
            current
                .spec
                .as_ref()
                .and_then(|spec| spec.replicas)
                .unwrap_or(1)
        };
        // Share the live count before omission so SSA cannot default it to one.
        // The resourceVersion fences concurrent HPA writes; this manager stays dormant under HPA.
        current = deployments.patch(&name, &PatchParams::apply(REPLICAS_MANAGER).force(), &Patch::Apply(json!({
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": name, "resourceVersion": current.metadata.resource_version},
            "spec": {"replicas": replicas}
        }))).await?;
    }
    if let Some(spec) = desired.spec.as_mut() {
        spec.replicas = None;
    }
    desired.metadata.resource_version = current.metadata.resource_version;
    let applied = deployments
        .patch(&name, &params, &Patch::Apply(&desired))
        .await?;
    if let Some(hpa) = hpa {
        autoscalers
            .patch(&name, &params, &Patch::Apply(&hpa))
            .await?;
    }
    Ok(applied)
}

fn pod_is_ready(pod: &Pod) -> bool {
    pod.metadata.deletion_timestamp.is_none()
        && pod
            .status
            .as_ref()
            .and_then(|status| status.conditions.as_ref())
            .is_some_and(|conditions| {
                conditions
                    .iter()
                    .any(|condition| condition.type_ == "Ready" && condition.status == "True")
            })
}

fn workload_pending_message(pods: &[Pod]) -> Option<String> {
    for pod in pods
        .iter()
        .filter(|pod| pod.metadata.deletion_timestamp.is_none())
    {
        let Some(status) = pod.status.as_ref() else {
            continue;
        };
        if let Some(condition) = status.conditions.as_ref().and_then(|conditions| {
            conditions
                .iter()
                .find(|condition| condition.type_ == "PodScheduled" && condition.status == "False")
        }) {
            return Some(format!(
                "waiting for pod {} to be scheduled ({}): {}",
                pod.name_any(),
                condition.reason.as_deref().unwrap_or("Pending"),
                condition.message.as_deref().unwrap_or("no eligible node")
            ));
        }
    }
    None
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
        for container in status.container_statuses.iter().flatten() {
            let current_terminated = container
                .state
                .as_ref()
                .and_then(|state| state.terminated.as_ref());
            let crash_loop_terminated = container
                .state
                .as_ref()
                .and_then(|state| state.waiting.as_ref())
                .filter(|waiting| waiting.reason.as_deref() == Some("CrashLoopBackOff"))
                .and_then(|_| container.last_state.as_ref())
                .and_then(|state| state.terminated.as_ref());
            if let Some(terminated) = current_terminated.or(crash_loop_terminated) {
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

fn workload_restart_message(pods: &[Pod]) -> Option<String> {
    for pod in pods.iter().filter(|pod| pod_is_ready(pod)) {
        let status = pod.status.as_ref()?;
        for container in status.container_statuses.iter().flatten() {
            let Some(terminated) = container
                .last_state
                .as_ref()
                .and_then(|state| state.terminated.as_ref())
            else {
                continue;
            };
            let reason = terminated.reason.as_deref().unwrap_or("process failure");
            return Some(format!(
                "pod {} recovered after container restart {} ({reason}, exit code {})",
                pod.name_any(),
                container.restart_count,
                terminated.exit_code
            ));
        }
    }
    None
}

fn flash_service_for_pod(pod: Pod) -> Option<ObjectRef<FlashService>> {
    let service_instance_id = pod
        .metadata
        .labels
        .as_ref()?
        .get("flash.heterocloud.io/instance")?;
    let namespace = pod.namespace()?;
    Some(ObjectRef::new(&format!("flash-{service_instance_id}")).within(&namespace))
}

#[cfg(test)]
fn desired_service(
    flash: &FlashService,
    owner: &OwnerReference,
) -> Result<Option<Service>, ReconcileError> {
    desired_service_with_hostname(flash, owner, None)
}

fn desired_service_with_hostname(
    flash: &FlashService,
    owner: &OwnerReference,
    hostname: Option<&str>,
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
    let mut labels = base_labels(flash);
    if workload.exposure.endpoint_mode == EndpointMode::LoadBalancer {
        let hostname = hostname
            .ok_or_else(|| anyhow::anyhow!("load_balancer requires provider publicDomain"))?;
        annotations.insert(DNS_HOSTNAME, hostname);
        annotations.insert("external-dns.alpha.kubernetes.io/ttl", "60");
        annotations.insert(
            "external-dns.alpha.kubernetes.io/cloudflare-proxied",
            "false",
        );
        labels.insert(DNS_PUBLISH_LABEL.into(), "true".into());
    }
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
            "labels": labels,
            "annotations": annotations,
            "ownerReferences": [owner],
        },
        "spec": spec,
    }))?))
}

#[cfg(test)]
fn desired_network_policy(
    flash: &FlashService,
    owner: &OwnerReference,
) -> Result<NetworkPolicy, ReconcileError> {
    desired_network_policy_with_networks(flash, owner, &[], &[], &[])
}

fn desired_network_policy_with_networks(
    flash: &FlashService,
    owner: &OwnerReference,
    additional_protected_networks: &[IpNet],
    dns_networks: &[IpNet],
    forwarded_ingress_networks: &[IpNet],
) -> Result<NetworkPolicy, ReconcileError> {
    let exposure = &flash.spec.workload.exposure;
    let mut seen_ports = BTreeSet::new();
    let ports = flash
        .spec
        .workload
        .ports
        .iter()
        .filter(|port| seen_ports.insert((port.container_port, port.protocol)))
        .map(|port| {
            json!({
                "protocol": port.protocol.as_kubernetes(),
                "port": port.container_port,
            })
        })
        .collect::<Vec<_>>();
    let ingress = if ports.is_empty() {
        Vec::new()
    } else {
        match (exposure.kind, exposure.traffic_mode) {
            (ExposureType::Internal, _) => vec![json!({
                "from": [{
                    "podSelector": {"matchLabels": {
                        "flash.heterocloud.io/organization": flash.spec.organization_id
                    }}
                }],
                "ports": ports,
            })],
            (ExposureType::Public, mode) => {
                let mut sources = exposure
                    .public_source_ip_blocks()?
                    .into_iter()
                    .map(|block| {
                        json!({"ipBlock": {
                            "cidr": block.cidr.to_string(),
                            "except": block.except.into_iter().map(|network| network.to_string()).collect::<Vec<_>>(),
                        }})
                    })
                    .collect::<Vec<_>>();
                if mode == TrafficMode::Forwarded {
                    // Cross-node kube-proxy forwarding is source-NATed to the
                    // assigned public node's Flannel address. Permit only
                    // those host /32s, never the complete Pod CIDR.
                    sources.extend(
                        forwarded_ingress_networks
                            .iter()
                            .map(|network| json!({"ipBlock": {"cidr": network.to_string()}})),
                    );
                }
                if sources.is_empty() {
                    Vec::new()
                } else {
                    vec![json!({"from": sources, "ports": ports})]
                }
            }
        }
    };

    let mut egress = Vec::new();
    let mut dns_destinations = dns_networks
        .iter()
        .map(|network| json!({"ipBlock": {"cidr": network.to_string()}}))
        .collect::<Vec<_>>();
    dns_destinations.push(json!({
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}},
        "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
    }));
    egress.push(json!({
        "to": dns_destinations,
        "ports": [
            {"protocol": "UDP", "port": 53},
            {"protocol": "TCP", "port": 53}
        ],
    }));
    if flash.spec.workload.egress.allow_same_organization {
        egress.push(json!({
            "to": [{
                "podSelector": {"matchLabels": {
                    "flash.heterocloud.io/organization": flash.spec.organization_id
                }}
            }]
        }));
    }
    let destinations = flash
        .spec
        .workload
        .egress
        .destination_ip_blocks(additional_protected_networks)?
        .into_iter()
        .map(|block| {
            json!({"ipBlock": {
                "cidr": block.cidr.to_string(),
                "except": block.except.into_iter().map(|network| network.to_string()).collect::<Vec<_>>(),
            }})
        })
        .collect::<Vec<_>>();
    if !destinations.is_empty() {
        egress.push(json!({"to": destinations}));
    }

    let mut policy: NetworkPolicy = from_value(json!({
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
            "policyTypes": ["Ingress", "Egress"],
            "ingress": ingress,
            "egress": egress,
        }
    }))?;
    normalize_network_policy(&mut policy);
    Ok(policy)
}

fn omit_empty<T>(values: &mut Option<Vec<T>>) {
    if values.as_ref().is_some_and(Vec::is_empty) {
        *values = None;
    }
}

fn normalize_network_policy(policy: &mut NetworkPolicy) {
    let Some(spec) = policy.spec.as_mut() else {
        return;
    };
    // Kubernetes omits empty slices but compares NetworkPolicy specs with reflect.DeepEqual.
    // Sending Some([]) repeatedly therefore increments generation and retriggers our watch.
    omit_empty(&mut spec.ingress);
    omit_empty(&mut spec.egress);
    omit_empty(&mut spec.policy_types);
    for rule in spec.ingress.iter_mut().flatten() {
        omit_empty(&mut rule.ports);
        omit_empty(&mut rule.from);
        for peer in rule.from.iter_mut().flatten() {
            if let Some(block) = peer.ip_block.as_mut() {
                omit_empty(&mut block.except);
            }
        }
    }
    for rule in spec.egress.iter_mut().flatten() {
        omit_empty(&mut rule.ports);
        omit_empty(&mut rule.to);
        for peer in rule.to.iter_mut().flatten() {
            if let Some(block) = peer.ip_block.as_mut() {
                omit_empty(&mut block.except);
            }
        }
    }
}

#[derive(Deserialize)]
struct AssignedIngressNode {
    name: String,
}

async fn assigned_forwarder_networks(
    client: &Client,
    service: Option<&Service>,
) -> Result<Vec<IpNet>, ReconcileError> {
    let Some(encoded) = service
        .and_then(|service| service.metadata.annotations.as_ref())
        .and_then(|annotations| annotations.get(ASSIGNED_NODES_ANNOTATION))
    else {
        return Ok(Vec::new());
    };
    let assigned = serde_json::from_str::<Vec<AssignedIngressNode>>(encoded)
        .context("parse assigned HeteroNetwork ingress nodes")?;
    let assigned_names = assigned
        .into_iter()
        .map(|node| node.name)
        .collect::<BTreeSet<_>>();
    let nodes = Api::<Node>::all(client.clone())
        .list(&ListParams::default())
        .await?;
    let mut networks = BTreeSet::new();
    for node in nodes
        .items
        .iter()
        .filter(|node| assigned_names.contains(&node.name_any()))
    {
        for value in node
            .spec
            .as_ref()
            .and_then(|spec| spec.pod_cidrs.as_deref())
            .unwrap_or_default()
        {
            let cidr = value.parse::<IpNet>().with_context(|| {
                format!("node {} has invalid Pod CIDR {value}", node.name_any())
            })?;
            networks.insert(IpNet::from(cidr.network()));
            if let Some(first_host) = cidr.hosts().next() {
                networks.insert(IpNet::from(first_host));
            }
        }
        for address in node
            .status
            .as_ref()
            .and_then(|status| status.addresses.as_deref())
            .unwrap_or_default()
            .iter()
            .filter(|address| address.type_ == "InternalIP")
        {
            let address = address
                .address
                .parse::<std::net::IpAddr>()
                .with_context(|| format!("node {} has invalid internal IP", node.name_any()))?;
            networks.insert(IpNet::from(address));
        }
    }
    Ok(networks.into_iter().collect())
}

fn service_endpoints(
    flash: &FlashService,
    service: &Service,
    hostname: Option<&str>,
) -> Vec<FlashEndpoint> {
    let mut hosts = match flash.spec.workload.exposure.kind {
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
    if flash.spec.workload.exposure.endpoint_mode == EndpointMode::LoadBalancer {
        hosts = if hosts.is_empty() {
            Vec::new()
        } else {
            hostname.into_iter().map(str::to_owned).collect()
        };
    }
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
    #[error("disk budget leaves less than 128 MiB for writable storage")]
    InsufficientWritableStorage,
    #[error(transparent)]
    Kubernetes(#[from] kube::Error),
    #[error(transparent)]
    Resource(#[from] anyhow::Error),
    #[error(transparent)]
    Validation(#[from] ValidationError),
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use k8s_openapi::api::core::v1::{Pod, Service};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
    use kube::runtime::controller::Action;
    use serde_json::{Value, json};

    use super::{
        AdminVolumeMount, desired_deployment, desired_network_policy,
        desired_network_policy_with_networks, desired_persistent_volume_claim, desired_service,
        storage_allocation, update_workload_status, validate_admin_volume_mounts,
        workload_failure_message, workload_restart_message,
    };
    use crate::{
        LOAD_BALANCER_CLASS, RUNTIME_CLASS_NAME,
        crd::{FlashService, FlashServicePhase, FlashServiceSpec, FlashServiceStatus},
        domain::{
            ExposureType, FlashEgress, FlashExposure, FlashPort, FlashSpec, TrafficMode,
            TransportProtocol,
        },
    };

    #[tokio::test]
    async fn status_clear_converges_and_stale_replica_cannot_restore_image_cache()
    -> Result<(), Box<dyn std::error::Error>> {
        use kube::{Api, Client, client::Body};
        use std::sync::{Arc, Mutex};

        let mut flash = service(TrafficMode::Forwarded);
        flash.metadata.resource_version = Some("10".into());
        flash.status = Some(FlashServiceStatus {
            observed_generation: 1,
            resolved_image: Some("example.invalid/old@sha256:old".into()),
            image_size_bytes: Some(1024),
            writable_storage_bytes: Some(10 * crate::image::GIB_BYTES - 1024),
            ..FlashServiceStatus::default()
        });
        flash.spec.desired_generation = 2;
        let state = Arc::new(Mutex::new((serde_json::to_value(&flash)?, 0usize)));
        let server_state = state.clone();
        let client = Client::new(
            tower::service_fn(move |request: http::Request<Body>| {
                let state = server_state.clone();
                async move {
                    assert_eq!(request.method(), http::Method::PATCH);
                    assert!(request.uri().path().ends_with("/status"));
                    assert_eq!(
                        request.headers()[http::header::CONTENT_TYPE],
                        "application/merge-patch+json"
                    );
                    let patch: Value =
                        serde_json::from_slice(&request.into_body().collect_bytes().await?)?;
                    let mut state = state
                        .lock()
                        .map_err(|_| std::io::Error::other("poisoned state"))?;
                    state.1 += 1;
                    let response = if patch["metadata"]["resourceVersion"]
                        != state.0["metadata"]["resourceVersion"]
                    {
                        http::Response::builder().status(409).body(Body::from(serde_json::to_vec(&json!({
                        "apiVersion": "v1", "kind": "Status", "status": "Failure",
                        "reason": "Conflict", "message": "stale resourceVersion", "code": 409
                    }))?))?
                    } else {
                        // Status has scalar/array fields only; emulate JSON merge deletion.
                        for (key, value) in patch["status"]
                            .as_object()
                            .ok_or_else(|| std::io::Error::other("missing status"))?
                        {
                            if value.is_null() {
                                state.0["status"]
                                    .as_object_mut()
                                    .ok_or_else(|| std::io::Error::other("missing status"))?
                                    .remove(key);
                            } else {
                                state.0["status"][key] = value.clone();
                            }
                        }
                        state.0["metadata"]["resourceVersion"] = json!("11");
                        http::Response::builder().body(Body::from(serde_json::to_vec(&state.0)?))?
                    };
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(response)
                }
            }),
            "test",
        );
        let api = Api::<FlashService>::namespaced(client, "test");
        let desired = FlashServiceStatus {
            observed_generation: 2,
            message: Some("waiting for image inspection".into()),
            ..FlashServiceStatus::default()
        };
        super::patch_status_if_changed(&api, &flash, desired.clone()).await?;
        let updated: FlashService = serde_json::from_value(
            state
                .lock()
                .map_err(|_| std::io::Error::other("poisoned state"))?
                .0
                .clone(),
        )?;
        assert_eq!(updated.status.as_ref(), Some(&desired));
        assert!(super::cached_image_inspection(&updated, 10 * crate::image::GIB_BYTES).is_none());
        super::patch_status_if_changed(&api, &updated, desired).await?;
        assert_eq!(
            state
                .lock()
                .map_err(|_| std::io::Error::other("poisoned state"))?
                .1,
            1
        );

        let stale_desired = FlashServiceStatus {
            phase: FlashServicePhase::Ready,
            ..flash
                .status
                .clone()
                .ok_or_else(|| std::io::Error::other("missing status"))?
        };
        assert!(matches!(
            super::patch_status_if_changed(&api, &flash, stale_desired).await,
            Err(super::ReconcileError::Kubernetes(kube::Error::Api(response))) if response.code == 409
        ));
        let persisted: FlashService = serde_json::from_value(
            state
                .lock()
                .map_err(|_| std::io::Error::other("poisoned state"))?
                .0
                .clone(),
        )?;
        assert_eq!(persisted.status, updated.status);
        Ok(())
    }

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
                    autoscaling: None,
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
                        endpoint_mode: crate::domain::EndpointMode::Ip,
                        kind: ExposureType::Public,
                        traffic_mode: mode,
                        allowed_source_cidrs: Vec::new(),
                        denied_source_cidrs: Vec::new(),
                    },
                    egress: FlashEgress::default(),
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
            None,
            Some("heterocloud-registry-pull"),
            &[],
        )?)?;
        assert_eq!(
            value.pointer("/spec/template/spec/runtimeClassName"),
            Some(&json!(RUNTIME_CLASS_NAME))
        );
        assert_eq!(
            value.pointer("/spec/template/metadata/labels/flash.heterocloud.io~1generation"),
            Some(&json!("1"))
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
            Some(&json!(false))
        );
        assert_eq!(
            value.pointer("/spec/template/spec/containers/0/securityContext/capabilities/drop/0"),
            Some(&json!("NET_RAW"))
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
            None,
            &[],
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
    fn persistent_home_and_rootfs_share_the_writable_disk_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let allocation = storage_allocation(10 * 1024 * 1024 * 1024, true)?;
        assert_eq!(allocation.rootfs_bytes, 1024 * 1024 * 1024);
        assert_eq!(allocation.persistent_bytes, 9 * 1024 * 1024 * 1024);

        let flash = service(TrafficMode::Forwarded);
        let claim = desired_persistent_volume_claim(
            &flash,
            &owner(),
            "longhorn-static",
            allocation.persistent_bytes,
        )?;
        let claim = serde_json::to_value(claim)?;
        assert_eq!(
            claim.pointer("/spec/accessModes/0"),
            Some(&json!("ReadWriteMany"))
        );
        assert_eq!(
            claim.pointer("/spec/storageClassName"),
            Some(&json!("longhorn-static"))
        );
        assert_eq!(
            claim.pointer("/spec/resources/requests/storage"),
            Some(&json!(allocation.persistent_bytes.to_string()))
        );

        let deployment = serde_json::to_value(desired_deployment(
            &flash,
            &owner(),
            "example.invalid/udp@sha256:verified",
            allocation.rootfs_bytes,
            Some("flash-test-home"),
            None,
            &[],
        )?)?;
        assert_eq!(
            deployment.pointer("/spec/template/spec/containers/0/volumeMounts/0/mountPath"),
            Some(&json!("/root"))
        );
        assert_eq!(
            deployment.pointer("/spec/template/spec/volumes/0/persistentVolumeClaim/claimName"),
            Some(&json!("flash-test-home"))
        );
        assert_eq!(
            deployment
                .pointer("/spec/template/spec/containers/0/resources/limits/ephemeral-storage"),
            Some(&json!(allocation.rootfs_bytes.to_string()))
        );
        Ok(())
    }

    #[test]
    fn pending_pvc_recovers_without_a_user_update() -> Result<(), Box<dyn std::error::Error>> {
        let mut flash = service(TrafficMode::Forwarded);
        flash.spec.workload.replicas = 1;
        let original_spec = flash.spec.clone();
        let mut status = FlashServiceStatus {
            observed_generation: flash.spec.desired_generation,
            desired_replicas: 1,
            ..FlashServiceStatus::default()
        };
        let mut pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "flash-test-abc"},
            "status": {
                "phase": "Pending",
                "conditions": [{
                    "type": "PodScheduled", "status": "False", "reason": "Unschedulable",
                    "message": "0/3 nodes are available: pod has unbound immediate PersistentVolumeClaims."
                }]
            }
        }))?;
        for _ in 0..2 {
            assert_eq!(workload_failure_message(std::slice::from_ref(&pod)), None);
            assert_eq!(
                update_workload_status(&mut status, std::slice::from_ref(&pod), true, true),
                Action::requeue(Duration::from_secs(5))
            );
            assert_eq!(status.phase, FlashServicePhase::Provisioning);
            assert_eq!(status.ready_replicas, 0);
            let message = status.message.as_deref().unwrap_or_default();
            assert!(message.contains("flash-test-abc"));
            assert!(message.contains("Unschedulable"));
            assert!(message.contains("unbound immediate PersistentVolumeClaims"));
        }

        // PVC binding and scheduling change only pod status, not the desired workload.
        pod.status = Some(serde_json::from_value(json!({
            "phase": "Pending",
            "conditions": [{"type": "PodScheduled", "status": "True"}]
        }))?);
        assert_eq!(
            update_workload_status(&mut status, std::slice::from_ref(&pod), true, true),
            Action::requeue(Duration::from_secs(5))
        );
        assert_eq!(status.phase, FlashServicePhase::Provisioning);
        assert!(
            !status
                .message
                .as_deref()
                .unwrap_or_default()
                .contains("Unschedulable")
        );

        pod.status = Some(serde_json::from_value(json!({
            "phase": "Running",
            "conditions": [
                {"type": "PodScheduled", "status": "True"},
                {"type": "Ready", "status": "True"}
            ]
        }))?);
        assert_eq!(
            update_workload_status(&mut status, std::slice::from_ref(&pod), false, true),
            Action::requeue(Duration::from_secs(5))
        );
        assert_eq!(status.phase, FlashServicePhase::Provisioning);
        assert_eq!(
            update_workload_status(&mut status, &[pod], true, true),
            Action::await_change()
        );
        assert_eq!(status.phase, FlashServicePhase::Ready);
        assert_eq!(status.ready_replicas, 1);
        assert_eq!(status.message, None);
        assert_eq!(status.observed_generation, original_spec.desired_generation);
        assert_eq!(flash.spec, original_spec);
        Ok(())
    }

    #[test]
    fn scheduling_delays_remain_provisioning() -> Result<(), Box<dyn std::error::Error>> {
        for conditions in [
            json!([]),
            json!([{"type": "PodScheduled", "status": "False"}]),
            json!([{
                "type": "PodScheduled", "status": "False", "reason": "Unschedulable",
                "message": "0/3 nodes are available: insufficient cpu"
            }]),
            json!([{"type": "PodScheduled", "status": "False", "reason": "SchedulingGated"}]),
        ] {
            let pod: Pod = serde_json::from_value(json!({
                "metadata": {"name": "flash-pending"},
                "status": {"phase": "Pending", "conditions": conditions}
            }))?;
            let mut status = FlashServiceStatus {
                desired_replicas: 1,
                ..FlashServiceStatus::default()
            };
            assert_eq!(
                update_workload_status(&mut status, &[pod], true, false),
                Action::requeue(Duration::from_secs(5))
            );
            assert_eq!(status.phase, FlashServicePhase::Provisioning);
            assert!(
                status
                    .message
                    .as_deref()
                    .unwrap_or_default()
                    .starts_with("waiting for")
            );
        }
        Ok(())
    }

    #[test]
    fn permanent_errors_take_precedence_over_scheduling_delays()
    -> Result<(), Box<dyn std::error::Error>> {
        let pending: Pod = serde_json::from_value(json!({
            "metadata": {"name": "flash-pending"},
            "status": {"conditions": [{
                "type": "PodScheduled", "status": "False", "reason": "Unschedulable",
                "message": "unbound immediate PersistentVolumeClaims"
            }]}
        }))?;
        for state in [
            json!({"waiting": {"reason": "CreateContainerConfigError"}}),
            json!({"waiting": {"reason": "CrashLoopBackOff"}}),
            json!({"waiting": {"reason": "ErrImagePull"}}),
            json!({"waiting": {"reason": "ImagePullBackOff"}}),
            json!({"waiting": {"reason": "InvalidImageName"}}),
            json!({"waiting": {"reason": "RunContainerError"}}),
            json!({"terminated": {"exitCode": 1, "reason": "Error"}}),
            json!({"terminated": {"exitCode": 0, "reason": "Completed"}}),
        ] {
            let failed: Pod = serde_json::from_value(json!({
                "metadata": {"name": "flash-failed"},
                "status": {"containerStatuses": [{
                    "name": "workload", "image": "example.invalid/test:v1", "imageID": "",
                    "ready": false, "restartCount": 0, "state": state
                }]}
            }))?;
            let mut status = FlashServiceStatus {
                desired_replicas: 2,
                ..FlashServiceStatus::default()
            };
            assert_eq!(
                update_workload_status(&mut status, &[pending.clone(), failed], true, false),
                Action::requeue(Duration::from_secs(5))
            );
            assert_eq!(status.phase, FlashServicePhase::Error);
            assert!(
                status
                    .message
                    .as_deref()
                    .unwrap_or_default()
                    .contains("flash-failed")
            );
        }
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
    fn trusted_admin_volume_is_mounted_without_exposing_its_credentials()
    -> Result<(), Box<dyn std::error::Error>> {
        let mount = AdminVolumeMount {
            name: "syouyu-workspace".into(),
            claim_name: "escape-syouyu-workspace".into(),
            mount_path: "/root/syouyu".into(),
            read_only: false,
        };
        let mut configured = BTreeMap::new();
        configured.insert(
            "00000000-0000-0000-0000-000000000001".into(),
            vec![mount.clone()],
        );
        validate_admin_volume_mounts(&configured)?;

        let deployment = serde_json::to_value(desired_deployment(
            &service(TrafficMode::Forwarded),
            &owner(),
            "example.invalid/udp@sha256:verified",
            1024,
            Some("flash-test-home"),
            None,
            &[mount],
        )?)?;
        assert_eq!(
            deployment.pointer("/spec/template/spec/containers/0/volumeMounts/1"),
            Some(&json!({
                "name": "syouyu-workspace",
                "mountPath": "/root/syouyu",
                "readOnly": false
            }))
        );
        assert_eq!(
            deployment.pointer("/spec/template/spec/volumes/1"),
            Some(&json!({
                "name": "syouyu-workspace",
                "persistentVolumeClaim": {"claimName": "escape-syouyu-workspace"}
            }))
        );
        assert_eq!(
            deployment.pointer("/spec/template/spec/containers/0/env"),
            Some(&json!([]))
        );
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
    fn reports_a_recovered_oom_without_marking_the_running_pod_failed()
    -> Result<(), Box<dyn std::error::Error>> {
        let pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "flash-test-abc"},
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}],
                "containerStatuses": [{
                    "name": "workload",
                    "image": "ubuntu:24.04",
                    "imageID": "example",
                    "ready": true,
                    "restartCount": 1,
                    "started": true,
                    "state": {"running": {"startedAt": "2026-09-01T22:29:36Z"}},
                    "lastState": {
                        "terminated": {
                            "containerID": "containerd://example",
                            "exitCode": 128,
                            "finishedAt": "2026-09-01T22:29:16Z",
                            "reason": "OOMKilled",
                            "startedAt": "2026-09-01T03:30:22Z"
                        }
                    }
                }]
            }
        }))?;
        assert_eq!(workload_failure_message(std::slice::from_ref(&pod)), None);
        let warning = workload_restart_message(&[pod])
            .ok_or_else(|| std::io::Error::other("missing restart warning"))?;
        assert!(warning.contains("OOMKilled"));
        assert!(warning.contains("restart 1"));
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
    fn domain_service_annotations_status_and_isolation() -> Result<(), Box<dyn std::error::Error>> {
        let mut flash = service(TrafficMode::Forwarded);
        flash.spec.workload.exposure.allowed_source_cidrs = vec!["192.0.2.0/24".into()];
        flash.spec.workload.exposure.denied_source_cidrs = vec!["192.0.2.128/25".into()];
        let ip_service = exposed_service(&flash)?;
        let policy = desired_network_policy(&flash, &owner())?;
        flash.spec.workload.exposure.endpoint_mode = crate::domain::EndpointMode::LoadBalancer;
        assert!(super::public_hostname(&flash, None).is_err());
        for invalid in [
            "https://example.com",
            "*.example.com",
            "example.com.",
            "example.com:443",
            "127.0.0.1",
            "UPPER.example",
            "bad..example",
        ] {
            assert!(super::public_hostname(&flash, Some(invalid)).is_err());
        }
        let hostname = super::public_hostname(&flash, Some("flash.heterocloud.mizuame.app"))?
            .ok_or("missing hostname")?;
        assert_eq!(
            hostname,
            format!(
                "f-{}.flash.heterocloud.mizuame.app",
                flash.spec.service_instance_id
            )
        );
        flash.spec.display_name = "tenant-controlled-name".into();
        assert_eq!(
            super::public_hostname(&flash, Some("flash.heterocloud.mizuame.app"))?,
            Some(hostname.clone())
        );
        let mut lb = super::desired_service_with_hostname(&flash, &owner(), Some(&hostname))?
            .ok_or("missing service")?;
        assert_eq!(lb.spec, ip_service.spec);
        assert_eq!(desired_network_policy(&flash, &owner())?, policy);
        let annotations = lb
            .metadata
            .annotations
            .as_ref()
            .ok_or("missing annotations")?;
        assert_eq!(annotations.get(super::DNS_HOSTNAME), Some(&hostname));
        assert_eq!(
            annotations
                .get("external-dns.alpha.kubernetes.io/ttl")
                .map(String::as_str),
            Some("60")
        );
        assert_eq!(
            annotations
                .get("external-dns.alpha.kubernetes.io/cloudflare-proxied")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            lb.metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(super::DNS_PUBLISH_LABEL))
                .map(String::as_str),
            Some("true")
        );
        assert!(super::service_endpoints(&flash, &lb, Some(&hostname)).is_empty());
        lb.status = Some(serde_json::from_value(
            json!({"loadBalancer": {"ingress": [{"ip": "192.0.2.1"}, {"ip": "192.0.2.2"}]}}),
        )?);
        let endpoints = super::service_endpoints(&flash, &lb, Some(&hostname));
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].host, hostname);
        assert_eq!(endpoints[0].port, 7777);
        assert_eq!(endpoints[0].protocol, TransportProtocol::Udp);
        assert!(super::service_endpoints(&flash, &lb, None).is_empty());
        flash.spec.workload.exposure.endpoint_mode = crate::domain::EndpointMode::Ip;
        let fixed = exposed_service(&flash)?;
        assert!(
            !fixed
                .metadata
                .labels
                .ok_or("missing labels")?
                .contains_key(super::DNS_PUBLISH_LABEL)
        );
        assert!(
            !fixed
                .metadata
                .annotations
                .ok_or("missing annotations")?
                .keys()
                .any(|key| key.starts_with("external-dns."))
        );
        Ok(())
    }

    #[test]
    fn hpa_independent_resource_metrics() -> Result<(), Box<dyn std::error::Error>> {
        for (cpu, memory, names) in [
            (Some(60), None, vec!["cpu"]),
            (None, Some(80), vec!["memory"]),
            (Some(60), Some(80), vec!["cpu", "memory"]),
        ] {
            let mut flash = service(TrafficMode::Forwarded);
            flash.spec.workload.autoscaling = Some(crate::domain::FlashAutoscaling {
                min_replicas: 2,
                max_replicas: 10,
                target_cpu_utilization_percent: cpu,
                target_memory_utilization_percent: memory,
            });
            let hpa = serde_json::to_value(
                super::desired_autoscaler(&flash, &owner())?.ok_or("missing HPA")?,
            )?;
            assert_eq!(hpa["apiVersion"], "autoscaling/v2");
            assert_eq!(hpa["spec"]["minReplicas"], 2);
            assert_eq!(hpa["spec"]["maxReplicas"], 10);
            assert_eq!(
                hpa["spec"]["behavior"]["scaleDown"]["stabilizationWindowSeconds"],
                300
            );
            let metrics = hpa["spec"]["metrics"].as_array().ok_or("missing metrics")?;
            assert_eq!(metrics.len(), names.len());
            for (metric, name) in metrics.iter().zip(names) {
                assert_eq!(metric["resource"]["name"], name);
                assert_eq!(metric["resource"]["target"]["type"], "Utilization");
            }
        }
        assert!(super::desired_autoscaler(&service(TrafficMode::Forwarded), &owner())?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn scaling_handoff_and_fixed_transitions() -> Result<(), Box<dyn std::error::Error>> {
        use kube::{Client, client::Body};
        use std::sync::{Arc, Mutex};
        let mut flash = service(TrafficMode::Forwarded);
        let deployment = desired_deployment(
            &flash,
            &owner(),
            "example.invalid/test@sha256:verified",
            1024,
            None,
            None,
            &[],
        )?;
        let mut live = serde_json::to_value(&deployment)?;
        live["metadata"]["resourceVersion"] = json!("1");
        live["metadata"]["managedFields"] = json!([{
            "manager": super::FIELD_MANAGER, "fieldsV1": {"f:spec": {"f:replicas": {}}}
        }]);
        // A request-level fake verifies ordering/payloads, not Kubernetes' SSA implementation.
        let state = Arc::new(Mutex::new((live, None::<Value>, Vec::<String>::new())));
        let server_state = state.clone();
        let client = Client::new(
            tower::service_fn(move |request: http::Request<Body>| {
                let state = server_state.clone();
                async move {
                    let method = request.method().clone();
                    let uri = request.uri().to_string();
                    let body = request.into_body().collect_bytes().await?;
                    let mut state = state
                        .lock()
                        .map_err(|_| std::io::Error::other("poisoned state"))?;
                    let hpa = uri.contains("horizontalpodautoscalers");
                    let mut code = 200;
                    let result = match method {
                    http::Method::GET if hpa => state.1.clone().unwrap_or_else(|| {
                        code = 404;
                        json!({"apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": "NotFound", "message": "absent", "code": 404})
                    }),
                    http::Method::GET => state.0.clone(),
                    http::Method::DELETE => {
                        assert!(hpa);
                        state.2.push("delete-hpa".into());
                        state.1 = None;
                        json!({"apiVersion": "v1", "kind": "Status", "status": "Success"})
                    },
                    http::Method::PATCH => {
                        let patch: Value = serde_json::from_slice(&body)?;
                        if hpa {
                            state.2.push("apply-hpa".into());
                            state.1 = Some(patch.clone());
                            patch
                        } else {
                            assert_eq!(patch["metadata"]["resourceVersion"], state.0["metadata"]["resourceVersion"]);
                            if uri.contains(super::REPLICAS_MANAGER) {
                                state.2.push(format!("replicas:{}", patch["spec"]["replicas"]));
                                state.0["spec"]["replicas"] = patch["spec"]["replicas"].clone();
                            } else {
                                assert!(patch["spec"].get("replicas").is_none());
                                state.2.push("apply-workload-without-replicas".into());
                                state.0["metadata"]["managedFields"] = json!([]);
                            }
                            let revision = state.0["metadata"]["resourceVersion"].as_str().ok_or("missing revision")?.parse::<u32>()? + 1;
                            state.0["metadata"]["resourceVersion"] = json!(revision.to_string());
                            state.0.clone()
                        }
                    },
                    _ => return Err("unexpected request".into()),
                };
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                        http::Response::builder()
                            .status(code)
                            .body(Body::from(serde_json::to_vec(&result)?))?,
                    )
                }
            }),
            "test",
        );
        let scaling = crate::domain::FlashAutoscaling {
            min_replicas: 2,
            max_replicas: 10,
            target_cpu_utilization_percent: Some(60),
            target_memory_utilization_percent: None,
        };
        flash.spec.workload.autoscaling = Some(scaling.clone());
        super::reconcile_scaling(&client, "test", &flash, &owner(), deployment.clone()).await?;
        {
            let mut state = state.lock().map_err(|_| "poisoned state")?;
            assert_eq!(
                state.2,
                ["replicas:3", "apply-workload-without-replicas", "apply-hpa"]
            );
            state.2.clear();
            state.0["spec"]["replicas"] = json!(7);
        }
        let applied =
            super::reconcile_scaling(&client, "test", &flash, &owner(), deployment.clone()).await?;
        assert_eq!(applied.spec.and_then(|spec| spec.replicas), Some(7));
        {
            let mut state = state.lock().map_err(|_| "poisoned state")?;
            assert_eq!(state.2, ["apply-workload-without-replicas", "apply-hpa"]);
            state.2.clear();
        }
        flash.spec.workload.autoscaling = None;
        super::reconcile_scaling(&client, "test", &flash, &owner(), deployment.clone()).await?;
        {
            let mut state = state.lock().map_err(|_| "poisoned state")?;
            assert_eq!(
                state.2,
                [
                    "delete-hpa",
                    "replicas:3",
                    "apply-workload-without-replicas"
                ]
            );
            state.2.clear();
        }
        flash.spec.workload.autoscaling = Some(scaling);
        super::reconcile_scaling(&client, "test", &flash, &owner(), deployment).await?;
        assert_eq!(
            state.lock().map_err(|_| "poisoned state")?.2,
            ["apply-workload-without-replicas", "apply-hpa"]
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

        let policy = desired_network_policy(&flash, &owner())?;
        let policy = serde_json::to_value(policy)?;
        assert_eq!(
            policy.pointer("/spec/policyTypes"),
            Some(&json!(["Ingress", "Egress"]))
        );
        assert_eq!(
            policy.pointer("/spec/ingress/0/from/0/ipBlock/cidr"),
            Some(&json!("192.0.2.0/24"))
        );
        assert_eq!(
            policy.pointer("/spec/ingress/0/from/0/ipBlock/except/0"),
            Some(&json!("192.0.2.128/25"))
        );
        assert_eq!(
            policy.pointer("/spec/ingress/0/ports/0"),
            Some(&json!({"protocol": "UDP", "port": 7777}))
        );
        Ok(())
    }

    #[test]
    fn service_without_source_policy_is_still_isolated() -> Result<(), Box<dyn std::error::Error>> {
        let flash = service(TrafficMode::Forwarded);
        let service = serde_json::to_value(exposed_service(&flash)?)?;
        assert_eq!(service.pointer("/spec/loadBalancerSourceRanges"), None);
        let policy = serde_json::to_value(desired_network_policy(&flash, &owner())?)?;
        assert_eq!(
            policy.pointer("/spec/ingress/0/ports/0/port"),
            Some(&json!(7777))
        );
        assert!(
            policy
                .pointer("/spec/ingress/0/from/0/ipBlock/except")
                .and_then(Value::as_array)
                .is_some_and(|networks| networks.contains(&json!("10.0.0.0/8")))
        );
        assert_eq!(
            policy.pointer("/spec/egress/0/ports/0"),
            Some(&json!({"protocol": "UDP", "port": 53}))
        );
        assert_eq!(
            policy.pointer("/spec/egress/1/to/0/ipBlock/cidr"),
            Some(&json!("0.0.0.0/0"))
        );
        assert!(
            policy
                .pointer("/spec/egress/1/to/0/ipBlock/except")
                .and_then(Value::as_array)
                .is_some_and(|networks| networks.contains(&json!("10.0.0.0/8")))
        );
        Ok(())
    }

    #[test]
    fn forwarded_public_policy_is_enforced_before_source_nat()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut flash = service(TrafficMode::Forwarded);
        flash.spec.workload.exposure.denied_source_cidrs = vec!["198.51.100.0/24".into()];
        let service = serde_json::to_value(exposed_service(&flash)?)?;
        assert!(service.pointer("/spec/loadBalancerSourceRanges").is_some());
        let policy = serde_json::to_value(desired_network_policy_with_networks(
            &flash,
            &owner(),
            &[],
            &[],
            &["10.244.2.0/32".parse()?],
        )?)?;
        assert_eq!(
            policy.pointer("/spec/ingress/0/from/2/ipBlock/cidr"),
            Some(&json!("10.244.2.0/32"))
        );
        assert!(
            policy
                .pointer("/spec/ingress/0/from/0/ipBlock/except")
                .and_then(Value::as_array)
                .is_some_and(|networks| networks.contains(&json!("198.51.100.0/24")))
        );
        assert_eq!(
            policy.pointer("/spec/ingress/0/ports/0/port"),
            Some(&json!(7777))
        );
        Ok(())
    }

    #[test]
    fn service_without_endpoints_still_has_default_deny_network_policy()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut flash = service(TrafficMode::Forwarded);
        flash.spec.workload.ports.clear();

        assert!(desired_service(&flash, &owner())?.is_none());
        let policy = serde_json::to_value(desired_network_policy(&flash, &owner())?)?;
        assert_eq!(policy.pointer("/spec/ingress"), None);
        assert_eq!(policy["spec"]["policyTypes"], json!(["Ingress", "Egress"]));
        assert!(policy.pointer("/spec/egress/0").is_some());
        let deployment = serde_json::to_value(desired_deployment(
            &flash,
            &owner(),
            "example.invalid/udp@sha256:verified",
            1024,
            None,
            None,
            &[],
        )?)?;
        assert_eq!(
            deployment.pointer("/spec/template/spec/containers/0/ports"),
            Some(&json!([]))
        );
        Ok(())
    }

    #[test]
    fn network_policy_omits_empty_optional_arrays() -> Result<(), Box<dyn std::error::Error>> {
        fn assert_no_empty_arrays(value: &Value) {
            match value {
                Value::Array(items) => {
                    assert!(!items.is_empty(), "optional empty arrays must be omitted");
                    for item in items {
                        assert_no_empty_arrays(item);
                    }
                }
                Value::Object(fields) => {
                    for value in fields.values() {
                        assert_no_empty_arrays(value);
                    }
                }
                _ => {}
            }
        }
        for mode in [TrafficMode::Direct, TrafficMode::Forwarded] {
            for no_ports in [false, true] {
                let mut flash = service(mode);
                if no_ports {
                    flash.spec.workload.ports.clear();
                }
                let policy = desired_network_policy(&flash, &owner())?;
                let value = serde_json::to_value(&policy)?;
                assert_no_empty_arrays(&value);
                assert_eq!(value["spec"]["policyTypes"], json!(["Ingress", "Egress"]));
                if no_ports {
                    assert!(
                        policy
                            .spec
                            .as_ref()
                            .ok_or("missing spec")?
                            .ingress
                            .is_none()
                    );
                } else {
                    assert!(
                        value
                            .pointer("/spec/ingress/0/from/1/ipBlock/except")
                            .is_none()
                    );
                }
                assert!(
                    value
                        .pointer("/spec/egress/1/to/1/ipBlock/except")
                        .is_none()
                );
                // Protected IPv4 exclusions must remain, not be normalized away.
                assert!(
                    value
                        .pointer("/spec/egress/1/to/0/ipBlock/except")
                        .and_then(Value::as_array)
                        .is_some_and(|items| items.contains(&json!("10.0.0.0/8")))
                );
            }
        }
        Ok(())
    }

    #[test]
    fn network_policy_normalization_preserves_rule_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut policy: k8s_openapi::api::networking::v1::NetworkPolicy = serde_json::from_value(
            json!({
                "spec": {
                    "podSelector": {}, "policyTypes": ["Ingress", "Egress"],
                    "ingress": [
                        {"ports": [], "from": []},
                        {"ports": [{"port": 7777, "protocol": "UDP"}], "from": [
                            {"ipBlock": {"cidr": "2000::/3", "except": []}},
                            {"namespaceSelector": {}, "podSelector": {}}
                        ]}
                    ],
                    "egress": [
                        {"ports": [], "to": []},
                        {"to": [{"ipBlock": {"cidr": "192.0.2.0/24", "except": ["192.0.2.128/25"]}}]}
                    ]
                }
            }),
        )?;
        super::normalize_network_policy(&mut policy);
        let value = serde_json::to_value(&policy)?;
        // An existing empty rule allows all; it must not be removed or synthesized.
        assert_eq!(value["spec"]["ingress"][0], json!({}));
        assert_eq!(value["spec"]["egress"][0], json!({}));
        assert_eq!(value["spec"]["podSelector"], json!({}));
        assert_eq!(
            value["spec"]["ingress"][1]["from"][1],
            json!({"namespaceSelector": {}, "podSelector": {}})
        );
        assert_eq!(
            value["spec"]["ingress"][1]["ports"],
            json!([{"port": 7777, "protocol": "UDP"}])
        );
        assert!(
            value
                .pointer("/spec/ingress/1/from/0/ipBlock/except")
                .is_none()
        );
        assert_eq!(
            value["spec"]["egress"][1]["to"][0]["ipBlock"]["except"],
            json!(["192.0.2.128/25"])
        );
        let once = policy.clone();
        super::normalize_network_policy(&mut policy);
        assert_eq!(policy, once);
        let spec = policy.spec.as_mut().ok_or("missing spec")?;
        spec.ingress = Some(Vec::new());
        spec.egress = Some(Vec::new());
        super::normalize_network_policy(&mut policy);
        let value = serde_json::to_value(policy)?;
        assert!(value.pointer("/spec/ingress").is_none());
        assert!(value.pointer("/spec/egress").is_none());
        assert_eq!(value["spec"]["policyTypes"], json!(["Ingress", "Egress"]));
        Ok(())
    }

    #[test]
    fn network_policy_converges_after_server_omits_empty_slices()
    -> Result<(), Box<dyn std::error::Error>> {
        use k8s_openapi::api::networking::v1::NetworkPolicy;
        // Model Go's omitempty wire representation for these generated specs.
        // This models wire normalization and generation comparison, not the full SSA engine.
        fn server_roundtrip(policy: &NetworkPolicy) -> Result<NetworkPolicy, serde_json::Error> {
            fn omit_arrays(value: &mut Value) {
                match value {
                    Value::Object(fields) => {
                        fields.retain(|_, value| !value.as_array().is_some_and(Vec::is_empty));
                        for value in fields.values_mut() {
                            omit_arrays(value);
                        }
                    }
                    Value::Array(items) => {
                        for item in items {
                            omit_arrays(item);
                        }
                    }
                    _ => {}
                }
            }
            let mut value = serde_json::to_value(policy)?;
            omit_arrays(&mut value);
            serde_json::from_value(value)
        }
        for no_ports in [false, true] {
            let mut flash = service(TrafficMode::Forwarded);
            if no_ports {
                flash.spec.workload.ports.clear();
            }
            let desired = desired_network_policy(&flash, &owner())?;
            let mut old_wire = serde_json::to_value(&desired)?;
            old_wire["spec"]["egress"][1]["to"][1]["ipBlock"]["except"] = json!([]);
            if no_ports {
                old_wire["spec"]["ingress"] = json!([]);
            }
            let old_desired: NetworkPolicy = serde_json::from_value(old_wire)?;
            let mut persisted = server_roundtrip(&old_desired)?;
            let mut generation = 1;
            // The old desired representation differs again after each serialization.
            for _ in 0..2 {
                if persisted.spec != old_desired.spec {
                    generation += 1;
                }
                persisted = server_roundtrip(&old_desired)?;
            }
            assert_eq!(generation, 3);
            for _ in 0..10 {
                if persisted.spec != desired.spec {
                    generation += 1;
                }
                persisted = server_roundtrip(&desired)?;
            }
            assert_eq!(generation, 3);
            assert_eq!(persisted.spec, desired.spec);
        }
        Ok(())
    }

    #[test]
    fn optional_same_organization_access_uses_tenant_label()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut flash = service(TrafficMode::Forwarded);
        flash.spec.workload.egress.allow_same_organization = true;
        let policy = serde_json::to_value(desired_network_policy(&flash, &owner())?)?;
        assert_eq!(
            policy.pointer(
                "/spec/egress/1/to/0/podSelector/matchLabels/flash.heterocloud.io~1organization"
            ),
            Some(&json!("00000000-0000-0000-0000-000000000002"))
        );
        Ok(())
    }
}
