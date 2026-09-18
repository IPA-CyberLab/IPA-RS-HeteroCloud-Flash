use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use futures_util::StreamExt;
use k8s_openapi::api::core::v1::Node;
use kube::{
    Api, Client, ResourceExt,
    api::{ListParams, Patch, PatchParams, PostParams},
    runtime::{
        controller::{Action, Controller},
        watcher,
    },
};
use serde::Serialize;
use serde_json::json;
use thiserror::Error;
use tracing::{error, info};

use crate::{
    GPU_READY_LABEL, GPU_RESOURCE_NAME, GPU_TYPE_LABEL,
    crd::{
        FlashGpuAssignment, FlashGpuDevice, FlashGpuDeviceStatus, FlashGpuHealth, FlashGpuJob,
        FlashGpuJobPhase, FlashGpuJobStatus, FlashGpuReservation, FlashGpuVisibility, FlashService,
    },
};

pub const GPU_JOB_FINALIZER: &str = "flash.heterocloud.io/gpu-reservation";
pub const GPU_LEASE_SECONDS: i64 = 90;
const GPU_LEASE_RENEW_SECONDS: i64 = 30;
const SCHEDULER_REQUEUE_SECONDS: u64 = 5;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GpuTypeCatalogEntry {
    pub gpu_type: String,
    pub display_name: String,
    pub access: FlashGpuVisibility,
    pub total: u32,
    pub available: u32,
}

/// Builds the user-facing catalog without exposing node names, inventory
/// names, physical IDs, or device health details.
pub fn visible_gpu_catalog(
    devices: &[FlashGpuDevice],
    subject_id: &str,
    now: i64,
) -> Result<Vec<GpuTypeCatalogEntry>, GpuCatalogError> {
    validate_gpu_inventory_models(devices)?;
    let mut catalog = BTreeMap::<String, GpuTypeCatalogEntry>::new();
    for device in devices
        .iter()
        .filter(|device| device_visible_to(device, subject_id))
    {
        let entry = catalog
            .entry(device.spec.gpu_type.clone())
            .or_insert_with(|| GpuTypeCatalogEntry {
                gpu_type: device.spec.gpu_type.clone(),
                display_name: device.spec.model.clone(),
                access: device.spec.visibility,
                total: 0,
                available: 0,
            });
        if device.spec.visibility == FlashGpuVisibility::Open {
            entry.access = FlashGpuVisibility::Open;
        }
        entry.total = entry.total.saturating_add(1);
        if gpu_device_available(device, now) {
            entry.available = entry.available.saturating_add(1);
        }
    }
    Ok(catalog.into_values().collect())
}

pub fn validate_gpu_inventory_models(devices: &[FlashGpuDevice]) -> Result<(), GpuCatalogError> {
    if let Some(gpu_type) = inconsistent_gpu_types(devices).into_iter().next() {
        return Err(GpuCatalogError::ModelMismatch(gpu_type));
    }
    Ok(())
}

fn inconsistent_gpu_types(devices: &[FlashGpuDevice]) -> BTreeSet<String> {
    let mut models = BTreeMap::<&str, &str>::new();
    let mut inconsistent = BTreeSet::new();
    for device in devices {
        if let Some(existing) = models.insert(&device.spec.gpu_type, &device.spec.model)
            && existing != device.spec.model
        {
            inconsistent.insert(device.spec.gpu_type.clone());
        }
    }
    inconsistent
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum GpuCatalogError {
    #[error("GPU type {0} has inconsistent model names in inventory")]
    ModelMismatch(String),
}

#[must_use]
pub fn device_visible_to(device: &FlashGpuDevice, subject_id: &str) -> bool {
    device.spec.visibility == FlashGpuVisibility::Open
        || device
            .spec
            .private_assignments
            .iter()
            .any(|assigned| assigned == subject_id)
}

#[must_use]
pub fn gpu_device_available(device: &FlashGpuDevice, now: i64) -> bool {
    device.status.as_ref().is_some_and(|status| {
        status.health == FlashGpuHealth::Healthy
            && status
                .reservation
                .as_ref()
                .is_none_or(|reservation| reservation.lease_expires_at <= now)
    })
}

fn candidate_devices<'a>(
    devices: &'a [FlashGpuDevice],
    job: &FlashGpuJob,
    now: i64,
) -> Vec<&'a FlashGpuDevice> {
    let mut candidates = devices
        .iter()
        .filter(|device| device_visible_to(device, &job.spec.subject_id))
        .filter(|device| {
            job.spec
                .gpu_type
                .as_ref()
                .is_none_or(|requested| requested == &device.spec.gpu_type)
        })
        .filter(|device| gpu_device_available(device, now))
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.status
            .as_ref()
            .and_then(|status| status.last_allocated_at)
            .cmp(
                &right
                    .status
                    .as_ref()
                    .and_then(|status| status.last_allocated_at),
            )
            .then_with(|| left.name_any().cmp(&right.name_any()))
    });
    candidates
}

fn pending_job(job: &FlashGpuJob) -> bool {
    job.metadata.deletion_timestamp.is_none()
        && job.spec.count == 1
        && job.spec.quota_remaining_seconds > 0
        && job.status.as_ref().is_none_or(|status| {
            matches!(
                status.phase,
                FlashGpuJobPhase::Queued | FlashGpuJobPhase::Retry
            ) || status.assignment.is_none()
        })
}

fn job_order(left: &FlashGpuJob, right: &FlashGpuJob) -> Ordering {
    left.spec
        .queued_at
        .cmp(&right.spec.queued_at)
        .then_with(|| left.name_any().cmp(&right.name_any()))
}

/// Returns the oldest pending job that can use at least one currently
/// available device. Stable timestamps and names make placement deterministic.
fn next_schedulable_job<'a>(
    jobs: &'a [FlashGpuJob],
    devices: &[FlashGpuDevice],
    now: i64,
) -> Option<&'a FlashGpuJob> {
    let mut pending = jobs
        .iter()
        .filter(|job| pending_job(job))
        .filter(|job| !candidate_devices(devices, job, now).is_empty())
        .collect::<Vec<_>>();
    pending.sort_by(|left, right| job_order(left, right));
    let mut principals = BTreeSet::new();
    pending.retain(|job| {
        principals.insert((
            job.spec.organization_id.as_str(),
            job.spec.subject_id.as_str(),
        ))
    });
    pending.sort_by(|left, right| {
        principal_last_served(devices, left)
            .cmp(&principal_last_served(devices, right))
            .then_with(|| job_order(left, right))
    });
    pending.into_iter().next()
}

fn principal_last_served(
    devices: &[FlashGpuDevice],
    job: &FlashGpuJob,
) -> (Option<i64>, Option<i64>) {
    let organization = devices
        .iter()
        .filter_map(|device| device.status.as_ref())
        .filter(|status| {
            status.last_organization_id.as_deref() == Some(job.spec.organization_id.as_str())
        })
        .filter_map(|status| status.last_allocated_at)
        .max();
    let subject = devices
        .iter()
        .filter_map(|device| device.status.as_ref())
        .filter(|status| {
            status.last_organization_id.as_deref() == Some(job.spec.organization_id.as_str())
                && status.last_subject_id.as_deref() == Some(job.spec.subject_id.as_str())
        })
        .filter_map(|status| status.last_allocated_at)
        .max();
    (organization, subject)
}

#[derive(Clone)]
struct SchedulerContext {
    client: Client,
    namespace: String,
}

pub async fn run_gpu_scheduler(client: Client, namespace: String) -> Result<()> {
    let jobs = Api::<FlashGpuJob>::namespaced(client.clone(), &namespace);
    let maintenance_client = client.clone();
    let context = Arc::new(SchedulerContext { client, namespace });
    info!("Flash GPU job scheduler started");
    tokio::spawn(async move {
        let devices = Api::<FlashGpuDevice>::all(maintenance_client.clone());
        loop {
            if let Err(error) = refresh_inventory_health(&maintenance_client, &devices).await {
                error!(error = %error, "failed to refresh GPU inventory health");
            }
            if let Err(error) = reclaim_stale_leases(&devices, chrono::Utc::now().timestamp()).await
            {
                error!(error = %error, "failed to reclaim stale GPU leases");
            }
            tokio::time::sleep(Duration::from_secs(GPU_LEASE_RENEW_SECONDS as u64)).await;
        }
    });
    Controller::new(jobs, watcher::Config::default())
        .run(reconcile_job, error_policy, context)
        .for_each(|result| async move {
            match result {
                Ok((object, _)) => info!(name = %object.name, "FlashGpuJob reconciled"),
                Err(error) => error!(error = %error, "FlashGpuJob reconciliation failed"),
            }
        })
        .await;
    Ok(())
}

async fn reconcile_job(
    job: Arc<FlashGpuJob>,
    context: Arc<SchedulerContext>,
) -> Result<Action, SchedulerError> {
    let jobs = Api::<FlashGpuJob>::namespaced(context.client.clone(), &context.namespace);
    let devices = Api::<FlashGpuDevice>::all(context.client.clone());
    let now = chrono::Utc::now().timestamp();

    if job.metadata.deletion_timestamp.is_some() {
        release_assignment(&devices, &job, now).await?;
        patch_job_status(
            &jobs,
            &job,
            FlashGpuJobStatus {
                phase: FlashGpuJobPhase::Cancelled,
                attempts: job.status.as_ref().map_or(0, |status| status.attempts),
                message: Some("GPU request cancelled and reservation released".into()),
                updated_at: now,
                ..FlashGpuJobStatus::default()
            },
        )
        .await?;
        remove_finalizer(&jobs, &job).await?;
        return Ok(Action::await_change());
    }
    if ensure_finalizer(&jobs, &job).await? {
        // Continue from the watch event carrying the new resourceVersion so
        // all subsequent status transitions can be fenced against races.
        return Ok(Action::await_change());
    }
    refresh_inventory_health(&context.client, &devices).await?;
    reclaim_stale_leases(&devices, now).await?;

    if job.spec.count != 1 {
        patch_job_status(
            &jobs,
            &job,
            FlashGpuJobStatus {
                phase: FlashGpuJobPhase::Rejected,
                message: Some("a Flash VM must request exactly one GPU".into()),
                updated_at: now,
                ..FlashGpuJobStatus::default()
            },
        )
        .await?;
        return Ok(Action::await_change());
    }
    if job.spec.quota_remaining_seconds == 0 {
        release_assignment(&devices, &job, now).await?;
        patch_job_status(
            &jobs,
            &job,
            FlashGpuJobStatus {
                phase: FlashGpuJobPhase::Rejected,
                message: Some("weekly GPU runtime limit reached".into()),
                updated_at: now,
                ..FlashGpuJobStatus::default()
            },
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(
            SCHEDULER_REQUEUE_SECONDS,
        )));
    }

    if let Some(assignment) = job
        .status
        .as_ref()
        .and_then(|status| status.assignment.as_ref())
    {
        if assignment_is_valid(&devices, &job, assignment, now).await? {
            let renewed = renew_assignment(&devices, &job, assignment, now).await?;
            let running = service_is_running(&context.client, &context.namespace, &job).await?;
            let mut status = job.status.clone().unwrap_or_default();
            status.phase = if running {
                FlashGpuJobPhase::Running
            } else {
                FlashGpuJobPhase::Reserved
            };
            status.assignment = Some(renewed);
            status.message = None;
            status.updated_at = now;
            patch_job_status(&jobs, &job, status).await?;
            return Ok(Action::requeue(Duration::from_secs(
                u64::try_from(GPU_LEASE_RENEW_SECONDS).unwrap_or(30),
            )));
        }
        release_assignment(&devices, &job, now).await?;
        let attempts = job
            .status
            .as_ref()
            .map_or(1, |status| status.attempts.saturating_add(1));
        patch_job_status(
            &jobs,
            &job,
            FlashGpuJobStatus {
                phase: FlashGpuJobPhase::Retry,
                attempts,
                message: Some("GPU lease was lost; request returned to the queue".into()),
                updated_at: now,
                ..FlashGpuJobStatus::default()
            },
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(
            SCHEDULER_REQUEUE_SECONDS,
        )));
    }

    let inventory = devices.list(&ListParams::default()).await?.items;
    let all_jobs = jobs.list(&ListParams::default()).await?.items;
    let is_next = next_schedulable_job(&all_jobs, &inventory, now)
        .is_some_and(|next| next.name_any() == job.name_any());
    if !is_next {
        patch_queued(&jobs, &job, now, "waiting for a visible healthy GPU").await?;
        return Ok(Action::requeue(Duration::from_secs(
            SCHEDULER_REQUEUE_SECONDS,
        )));
    }
    let Some(device) = candidate_devices(&inventory, &job, now).into_iter().next() else {
        patch_queued(&jobs, &job, now, "waiting for a visible healthy GPU").await?;
        return Ok(Action::requeue(Duration::from_secs(
            SCHEDULER_REQUEUE_SECONDS,
        )));
    };
    match reserve_device(&devices, device, &job, now).await {
        Ok(assignment) => {
            let attempts = job
                .status
                .as_ref()
                .map_or(1, |status| status.attempts.saturating_add(1));
            if let Err(error) = patch_job_status(
                &jobs,
                &job,
                FlashGpuJobStatus {
                    phase: FlashGpuJobPhase::Reserved,
                    attempts,
                    assignment: Some(assignment.clone()),
                    updated_at: now,
                    ..FlashGpuJobStatus::default()
                },
            )
            .await
            {
                // Two scheduler replicas can reserve different devices for
                // the same stale job snapshot. Only the resourceVersion
                // winner may publish its assignment; the loser must release
                // the reservation it just acquired.
                release_device_assignment(&devices, &job, &assignment, now).await?;
                return Err(error.into());
            }
        }
        Err(kube::Error::Api(response)) if response.code == 409 => {
            // Another scheduler replica won the resourceVersion race. Do not
            // overwrite a Reserved status it may be publishing for this job.
        }
        Err(error) => return Err(error.into()),
    }
    Ok(Action::requeue(Duration::from_secs(
        SCHEDULER_REQUEUE_SECONDS,
    )))
}

async fn patch_queued(
    jobs: &Api<FlashGpuJob>,
    job: &FlashGpuJob,
    now: i64,
    message: &str,
) -> Result<(), kube::Error> {
    let attempts = job.status.as_ref().map_or(0, |status| status.attempts);
    patch_job_status(
        jobs,
        job,
        FlashGpuJobStatus {
            phase: FlashGpuJobPhase::Queued,
            attempts,
            message: Some(message.into()),
            updated_at: now,
            ..FlashGpuJobStatus::default()
        },
    )
    .await
}

async fn patch_job_status(
    jobs: &Api<FlashGpuJob>,
    job: &FlashGpuJob,
    status: FlashGpuJobStatus,
) -> Result<(), kube::Error> {
    if job.status.as_ref() != Some(&status) {
        let patch = job_status_patch(job, &status);
        jobs.patch_status(
            &job.name_any(),
            &PatchParams::default(),
            &Patch::Merge(patch),
        )
        .await?;
    }
    Ok(())
}

fn job_status_patch(job: &FlashGpuJob, status: &FlashGpuJobStatus) -> serde_json::Value {
    let mut patch = json!({"status": status});
    // Merge patches must explicitly clear fields omitted by status
    // serialization, otherwise a lost assignment remains visible forever.
    patch["status"]["assignment"] = json!(status.assignment);
    patch["status"]["message"] = json!(status.message);
    if let Some(version) = &job.metadata.resource_version {
        patch["metadata"] = json!({"resourceVersion": version});
    }
    patch
}

async fn ensure_finalizer(jobs: &Api<FlashGpuJob>, job: &FlashGpuJob) -> Result<bool, kube::Error> {
    if !job
        .finalizers()
        .iter()
        .any(|value| value == GPU_JOB_FINALIZER)
    {
        let mut finalizers = job.finalizers().to_vec();
        finalizers.push(GPU_JOB_FINALIZER.into());
        jobs.patch(
            &job.name_any(),
            &PatchParams::default(),
            &Patch::Merge(json!({"metadata": {"finalizers": finalizers}})),
        )
        .await?;
        return Ok(true);
    }
    Ok(false)
}

async fn remove_finalizer(jobs: &Api<FlashGpuJob>, job: &FlashGpuJob) -> Result<(), kube::Error> {
    let finalizers = job
        .finalizers()
        .iter()
        .filter(|value| value.as_str() != GPU_JOB_FINALIZER)
        .cloned()
        .collect::<Vec<_>>();
    jobs.patch(
        &job.name_any(),
        &PatchParams::default(),
        &Patch::Merge(json!({"metadata": {"finalizers": finalizers}})),
    )
    .await?;
    Ok(())
}

async fn reserve_device(
    devices: &Api<FlashGpuDevice>,
    device: &FlashGpuDevice,
    job: &FlashGpuJob,
    now: i64,
) -> Result<FlashGpuAssignment, kube::Error> {
    let lease_expires_at = now.saturating_add(GPU_LEASE_SECONDS);
    let mut replacement = device.clone();
    replacement.status = Some(FlashGpuDeviceStatus {
        health: device
            .status
            .as_ref()
            .map_or(FlashGpuHealth::Unknown, |status| status.health),
        reservation: Some(FlashGpuReservation {
            job_namespace: job.namespace().unwrap_or_default(),
            job_name: job.name_any(),
            subject_id: job.spec.subject_id.clone(),
            service_instance_id: job.spec.service_instance_id.clone(),
            reserved_at: now,
            lease_expires_at,
        }),
        last_allocated_at: Some(now),
        last_organization_id: Some(job.spec.organization_id.clone()),
        last_subject_id: Some(job.spec.subject_id.clone()),
    });
    devices
        .replace_status(&device.name_any(), &PostParams::default(), &replacement)
        .await?;
    Ok(FlashGpuAssignment {
        inventory_name: device.name_any(),
        node_name: device.spec.node_name.clone(),
        gpu_type: device.spec.gpu_type.clone(),
        model: device.spec.model.clone(),
        lease_expires_at,
    })
}

async fn assignment_is_valid(
    devices: &Api<FlashGpuDevice>,
    job: &FlashGpuJob,
    assignment: &FlashGpuAssignment,
    now: i64,
) -> Result<bool, kube::Error> {
    let Some(device) = devices.get_opt(&assignment.inventory_name).await? else {
        return Ok(false);
    };
    Ok(device_visible_to(&device, &job.spec.subject_id)
        && device.spec.node_name == assignment.node_name
        && job
            .spec
            .gpu_type
            .as_ref()
            .is_none_or(|requested| requested == &device.spec.gpu_type)
        && device.status.as_ref().is_some_and(|status| {
            status.health == FlashGpuHealth::Healthy
                && status.reservation.as_ref().is_some_and(|reservation| {
                    reservation.job_namespace == job.namespace().unwrap_or_default()
                        && reservation.job_name == job.name_any()
                        && reservation.lease_expires_at > now
                })
        }))
}

async fn renew_assignment(
    devices: &Api<FlashGpuDevice>,
    job: &FlashGpuJob,
    assignment: &FlashGpuAssignment,
    now: i64,
) -> Result<FlashGpuAssignment, kube::Error> {
    if assignment.lease_expires_at.saturating_sub(now) > GPU_LEASE_RENEW_SECONDS {
        return Ok(assignment.clone());
    }
    let device = devices.get(&assignment.inventory_name).await?;
    reserve_device(devices, &device, job, now).await
}

async fn release_assignment(
    devices: &Api<FlashGpuDevice>,
    job: &FlashGpuJob,
    now: i64,
) -> Result<(), kube::Error> {
    let Some(assignment) = job
        .status
        .as_ref()
        .and_then(|status| status.assignment.as_ref())
    else {
        return Ok(());
    };
    release_device_assignment(devices, job, assignment, now).await
}

async fn release_device_assignment(
    devices: &Api<FlashGpuDevice>,
    job: &FlashGpuJob,
    assignment: &FlashGpuAssignment,
    now: i64,
) -> Result<(), kube::Error> {
    let Some(mut device) = devices.get_opt(&assignment.inventory_name).await? else {
        return Ok(());
    };
    let owned = device
        .status
        .as_ref()
        .and_then(|status| status.reservation.as_ref())
        .is_some_and(|reservation| {
            reservation.job_namespace == job.namespace().unwrap_or_default()
                && reservation.job_name == job.name_any()
        });
    if owned {
        let mut status = device.status.clone().unwrap_or_default();
        status.reservation = None;
        status.last_allocated_at = Some(now);
        device.status = Some(status);
        match devices
            .replace_status(&device.name_any(), &PostParams::default(), &device)
            .await
        {
            Ok(_) => {}
            Err(kube::Error::Api(response)) if response.code == 404 => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

async fn reclaim_stale_leases(devices: &Api<FlashGpuDevice>, now: i64) -> Result<(), kube::Error> {
    for mut device in devices.list(&ListParams::default()).await?.items {
        let stale = device
            .status
            .as_ref()
            .and_then(|status| status.reservation.as_ref())
            .is_some_and(|reservation| reservation.lease_expires_at <= now);
        if stale {
            let mut status = device.status.clone().unwrap_or_default();
            status.reservation = None;
            device.status = Some(status);
            match devices
                .replace_status(&device.name_any(), &PostParams::default(), &device)
                .await
            {
                Ok(_) => {}
                Err(kube::Error::Api(response)) if response.code == 409 || response.code == 404 => {
                }
                Err(error) => return Err(error),
            }
        }
    }
    Ok(())
}

async fn refresh_inventory_health(
    client: &Client,
    devices: &Api<FlashGpuDevice>,
) -> Result<(), kube::Error> {
    let nodes = Api::<Node>::all(client.clone());
    let node_map = nodes
        .list(&ListParams::default())
        .await?
        .items
        .into_iter()
        .map(|node| (node.name_any(), node))
        .collect::<BTreeMap<_, _>>();
    let inventory = devices.list(&ListParams::default()).await?.items;
    let inconsistent_types = inconsistent_gpu_types(&inventory);
    let mut inventory_counts = BTreeMap::<(String, String), u64>::new();
    for device in &inventory {
        let key = (device.spec.node_name.clone(), device.spec.gpu_type.clone());
        *inventory_counts.entry(key).or_default() += 1;
    }
    for mut device in inventory {
        let expected_capacity = inventory_counts
            .get(&(device.spec.node_name.clone(), device.spec.gpu_type.clone()))
            .copied()
            .unwrap_or(1);
        let desired = node_map
            .get(&device.spec.node_name)
            .filter(|node| {
                !inconsistent_types.contains(&device.spec.gpu_type)
                    && node_is_gpu_ready(node, &device.spec.gpu_type, expected_capacity)
            })
            .map_or(FlashGpuHealth::Unhealthy, |_| FlashGpuHealth::Healthy);
        if device
            .status
            .as_ref()
            .map_or(FlashGpuHealth::Unknown, |status| status.health)
            != desired
        {
            let mut status = device.status.clone().unwrap_or_default();
            status.health = desired;
            device.status = Some(status);
            match devices
                .replace_status(&device.name_any(), &PostParams::default(), &device)
                .await
            {
                Ok(_) => {}
                Err(kube::Error::Api(response)) if response.code == 409 || response.code == 404 => {
                }
                Err(error) => return Err(error),
            }
        }
    }
    Ok(())
}

fn node_is_gpu_ready(node: &Node, gpu_type: &str, expected_capacity: u64) -> bool {
    let labels = node.metadata.labels.as_ref();
    let labeled = labels.is_some_and(|labels| {
        labels
            .get(GPU_READY_LABEL)
            .is_some_and(|value| value == "true")
            && labels
                .get(GPU_TYPE_LABEL)
                .is_some_and(|value| value == gpu_type)
    });
    let ready = node
        .status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == "Ready" && condition.status == "True")
        });
    let allocatable = node
        .status
        .as_ref()
        .and_then(|status| status.allocatable.as_ref())
        .and_then(|resources| resources.get(GPU_RESOURCE_NAME))
        .and_then(|quantity| quantity.0.parse::<u64>().ok())
        .is_some_and(|capacity| capacity >= expected_capacity);
    labeled && ready && allocatable
}

async fn service_is_running(
    client: &Client,
    namespace: &str,
    job: &FlashGpuJob,
) -> Result<bool, kube::Error> {
    let services = Api::<FlashService>::namespaced(client.clone(), namespace);
    Ok(services
        .get_opt(&job.name_any())
        .await?
        .is_some_and(|service| {
            service.status.as_ref().is_some_and(|status| {
                status.observed_generation == job.spec.service_generation
                    && status.ready_replicas > 0
            })
        }))
}

fn error_policy(
    _job: Arc<FlashGpuJob>,
    error: &SchedulerError,
    _context: Arc<SchedulerContext>,
) -> Action {
    error!(error = %error, "GPU scheduler will retry");
    Action::requeue(Duration::from_secs(SCHEDULER_REQUEUE_SECONDS))
}

#[derive(Debug, Error)]
enum SchedulerError {
    #[error(transparent)]
    Kubernetes(#[from] kube::Error),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::crd::{FlashGpuDeviceSpec, FlashGpuJobSpec};
    use k8s_openapi::{
        api::core::v1::{NodeCondition, NodeStatus},
        apimachinery::pkg::api::resource::Quantity,
    };

    fn device(name: &str, visibility: FlashGpuVisibility, subjects: &[&str]) -> FlashGpuDevice {
        let mut device = FlashGpuDevice::new(
            name,
            FlashGpuDeviceSpec {
                node_name: "gpu-node".into(),
                physical_id: format!("GPU-{name}"),
                gpu_type: "nvidia-geforce-gtx-1080-ti".into(),
                model: "NVIDIA GeForce GTX 1080 Ti".into(),
                memory_mib: 11_264,
                visibility,
                private_assignments: subjects.iter().map(|value| (*value).into()).collect(),
            },
        );
        device.status = Some(FlashGpuDeviceStatus {
            health: FlashGpuHealth::Healthy,
            ..FlashGpuDeviceStatus::default()
        });
        device
    }

    fn job(name: &str, subject: &str, queued_at: i64) -> FlashGpuJob {
        let mut job = FlashGpuJob::new(
            name,
            FlashGpuJobSpec {
                service_instance_id: name.into(),
                service_generation: 1,
                subject_id: subject.into(),
                organization_id: "organization".into(),
                project_id: "project".into(),
                gpu_type: Some("nvidia-geforce-gtx-1080-ti".into()),
                count: 1,
                quota_remaining_seconds: 3_600,
                queued_at,
            },
        );
        job.metadata.namespace = Some("flash".into());
        job
    }

    #[test]
    fn catalog_hides_private_devices_and_physical_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let devices = vec![
            device("open", FlashGpuVisibility::Open, &[]),
            device("private-a", FlashGpuVisibility::Private, &["alice"]),
            device("private-b", FlashGpuVisibility::Private, &["bob"]),
        ];
        let catalog = visible_gpu_catalog(&devices, "alice", 100)?;
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog[0].total, 2);
        assert_eq!(catalog[0].available, 2);
        assert_eq!(catalog[0].access, FlashGpuVisibility::Open);
        assert_eq!(catalog[0].display_name, "NVIDIA GeForce GTX 1080 Ti");
        let encoded = serde_json::to_string(&catalog)?;
        assert!(!encoded.contains("physical"));
        assert!(!encoded.contains("gpu-node"));
        assert!(!encoded.contains("private-a"));

        let service_principal_catalog = visible_gpu_catalog(&devices, "", 100)?;
        assert_eq!(service_principal_catalog.len(), 1);
        assert_eq!(service_principal_catalog[0].total, 1);
        Ok(())
    }

    #[test]
    fn queue_is_oldest_first_and_device_choice_is_stable() {
        let devices = vec![
            device("gpu-b", FlashGpuVisibility::Open, &[]),
            device("gpu-a", FlashGpuVisibility::Open, &[]),
        ];
        let jobs = vec![job("newer", "alice", 20), job("older", "alice", 10)];
        assert_eq!(
            next_schedulable_job(&jobs, &devices, 100).map(ResourceExt::name_any),
            Some("older".into())
        );
        assert_eq!(
            candidate_devices(&devices, &jobs[1], 100)[0].name_any(),
            "gpu-a"
        );
    }

    #[test]
    fn private_assignment_and_active_lease_gate_capacity() {
        let mut private = device("private", FlashGpuVisibility::Private, &["alice"]);
        if let Some(status) = private.status.as_mut() {
            status.reservation = Some(FlashGpuReservation {
                job_namespace: "flash".into(),
                job_name: "other".into(),
                subject_id: "alice".into(),
                service_instance_id: "other".into(),
                reserved_at: 90,
                lease_expires_at: 200,
            });
        }
        assert!(candidate_devices(&[private.clone()], &job("a", "bob", 1), 100).is_empty());
        assert!(candidate_devices(&[private.clone()], &job("a", "alice", 1), 100).is_empty());
        assert_eq!(
            candidate_devices(&[private], &job("a", "alice", 1), 201).len(),
            1
        );
    }

    #[test]
    fn empty_private_assignment_is_visible_to_nobody() {
        let private = device("isolated", FlashGpuVisibility::Private, &[]);
        assert!(!device_visible_to(&private, "alice"));
        assert!(matches!(
            visible_gpu_catalog(&[private], "alice", 100),
            Ok(catalog) if catalog.is_empty()
        ));
    }

    #[test]
    fn catalog_rejects_inconsistent_models_for_one_type() {
        let first = device("first", FlashGpuVisibility::Open, &[]);
        let mut second = device("second", FlashGpuVisibility::Open, &[]);
        second.spec.model = "Different card".into();
        assert_eq!(
            visible_gpu_catalog(&[first, second], "alice", 100),
            Err(GpuCatalogError::ModelMismatch(
                "nvidia-geforce-gtx-1080-ti".into()
            ))
        );
    }

    #[test]
    fn fairness_rotates_organizations_then_subjects() {
        let mut gpu = device("gpu", FlashGpuVisibility::Open, &[]);
        if let Some(status) = &mut gpu.status {
            status.last_allocated_at = Some(100);
            status.last_organization_id = Some("organization-a".into());
            status.last_subject_id = Some("alice".into());
        }
        let mut alice_old = job("alice-old", "alice", 1);
        alice_old.spec.organization_id = "organization-a".into();
        let mut alice_next = job("alice-next", "alice", 2);
        alice_next.spec.organization_id = "organization-a".into();
        let mut bob = job("bob", "bob", 20);
        bob.spec.organization_id = "organization-b".into();
        assert_eq!(
            next_schedulable_job(&[alice_old, alice_next, bob], &[gpu.clone()], 200)
                .map(ResourceExt::name_any),
            Some("bob".into())
        );

        let mut charlie = job("charlie", "charlie", 30);
        charlie.spec.organization_id = "organization-a".into();
        let mut alice = job("alice", "alice", 1);
        alice.spec.organization_id = "organization-a".into();
        assert_eq!(
            next_schedulable_job(&[alice, charlie], &[gpu], 200).map(ResourceExt::name_any),
            Some("charlie".into())
        );
    }

    #[test]
    fn quota_zero_is_never_schedulable() {
        let devices = vec![device("gpu", FlashGpuVisibility::Open, &[])];
        let mut request = job("job", "alice", 1);
        request.spec.quota_remaining_seconds = 0;
        assert!(next_schedulable_job(&[request], &devices, 100).is_none());
    }

    #[test]
    fn retry_status_patch_is_fenced_and_clears_the_old_assignment() {
        let mut request = job("job", "alice", 1);
        request.metadata.resource_version = Some("17".into());
        let patch = job_status_patch(
            &request,
            &FlashGpuJobStatus {
                phase: FlashGpuJobPhase::Retry,
                attempts: 2,
                message: Some("lease lost".into()),
                updated_at: 100,
                ..FlashGpuJobStatus::default()
            },
        );
        assert_eq!(patch["metadata"]["resourceVersion"], "17");
        assert_eq!(patch["status"]["assignment"], serde_json::Value::Null);
        assert_eq!(patch["status"]["message"], "lease lost");
    }

    #[test]
    fn node_health_requires_ready_labels_and_full_allocatable_inventory() {
        let mut node = Node::default();
        node.metadata.labels = Some(BTreeMap::from([
            (GPU_READY_LABEL.into(), "true".into()),
            (GPU_TYPE_LABEL.into(), "nvidia-geforce-gtx-1080-ti".into()),
        ]));
        node.status = Some(NodeStatus {
            allocatable: Some(BTreeMap::from([(
                GPU_RESOURCE_NAME.into(),
                Quantity("2".into()),
            )])),
            conditions: Some(vec![NodeCondition {
                last_heartbeat_time: None,
                last_transition_time: None,
                message: Some("ready".into()),
                reason: Some("KubeletReady".into()),
                status: "True".into(),
                type_: "Ready".into(),
            }]),
            ..NodeStatus::default()
        });
        assert!(node_is_gpu_ready(&node, "nvidia-geforce-gtx-1080-ti", 2));
        assert!(!node_is_gpu_ready(&node, "nvidia-geforce-gtx-1080-ti", 3));
    }
}
