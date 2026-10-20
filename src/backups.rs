//! Tenant-scoped disk backups backed by CSI *backups*, not local snapshots.
//! A restore only provisions a new PVC; the source PVC is never written/deleted.
use std::{collections::BTreeMap, time::Duration};

use k8s_openapi::api::core::v1::{PersistentVolume, PersistentVolumeClaim};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{
        ApiResource, DeleteParams, DynamicObject, GroupVersionKind, ListParams, Patch, PatchParams,
        PostParams,
    },
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{auth::ProviderClaims, crd::FlashService};

const FINALIZER: &str = "flash.heterocloud.io/backup-protection";
const MANAGER: &str = "heterocloud-flash-backup";

pub fn validated_crd() -> anyhow::Result<Value> {
    use kube::CustomResourceExt;
    let mut value = serde_json::to_value(FlashBackup::crd())?;
    value["metadata"]["annotations"] =
        json!({"argocd.argoproj.io/sync-options":"Prune=false","helm.sh/resource-policy":"keep"});
    value["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]["x-kubernetes-validations"] =
        json!([{"rule":"self == oldSelf","message":"backup source and ownership are immutable"}]);
    Ok(value)
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackupRequest {
    pub id: Uuid,
    pub name: String,
    pub source_pvc_uid: String,
    pub volume_size_bytes: u64,
}

#[derive(Clone, Debug, kube::CustomResource, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "flash.heterocloud.io",
    version = "v1alpha1",
    kind = "FlashBackup",
    plural = "flashbackups",
    namespaced,
    status = "FlashBackupStatus"
)]
#[serde(deny_unknown_fields)]
pub struct FlashBackupSpec {
    pub id: Uuid,
    pub name: String,
    pub organization_id: Uuid,
    pub project_id: Uuid,
    pub service_instance_id: Uuid,
    pub source_pvc: String,
    pub source_pvc_uid: String,
    pub volume_size_bytes: u64,
    pub snapshot_class: String,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlashBackupStatus {
    #[serde(default)]
    pub phase: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<i64>,
}

#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error("backup storage is not configured")]
    Disabled,
    #[error("backup or persistent disk was not found")]
    NotFound,
    #[error("backup scope does not match")]
    Forbidden,
    #[error("{0}")]
    Conflict(&'static str),
    #[error("{0}")]
    Invalid(&'static str),
    #[error("backup storage request failed")]
    Storage(#[from] kube::Error),
    #[error("backup resource could not be decoded")]
    Decode(#[from] serde_json::Error),
}

pub fn backup_name(id: Uuid) -> String {
    format!("flash-backup-{}", id.simple())
}

fn snapshot_resource() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind::gvk(
        "snapshot.storage.k8s.io",
        "v1",
        "VolumeSnapshot",
    ))
}

pub fn snapshots(client: Client, namespace: &str) -> Api<DynamicObject> {
    Api::namespaced_with(client, namespace, &snapshot_resource())
}

pub fn view(backup: &FlashBackup) -> Value {
    let status = backup.status.clone().unwrap_or_default();
    let verification = backup.annotations().get("flash.heterocloud.io/restore-verification").and_then(|v|serde_json::from_str::<Value>(v).ok()).map(|value| {
        json!({"phase":value["phase"],"started_at":value["started_at"],"completed_at":value["completed_at"],"sampled_files":value["sampled_files"],"sampled_bytes":value["sampled_bytes"],"sha256":value["sha256"],"cleanup_verified":value["cleanup_verified"]})
    });
    json!({"id":backup.spec.id,"name":backup.spec.name,
        "phase":if backup.metadata.deletion_timestamp.is_some() {"deleting"} else if status.phase.is_empty() {"pending"} else {&status.phase},
        "message":status.message,"completed_at":status.completed_at,
        "volume_size_bytes":backup.spec.volume_size_bytes,"restore_verification":verification})
}

pub async fn request_verification(
    client: Client,
    namespace: &str,
    id: Uuid,
    claims: &ProviderClaims,
) -> Result<(), BackupError> {
    let backup = get(client.clone(), namespace, id, claims).await?;
    if backup.metadata.deletion_timestamp.is_some()
        || backup.status.as_ref().is_none_or(|s| s.phase != "ready")
    {
        return Err(BackupError::Conflict(
            "backup is not ready for verification",
        ));
    }
    let existing = backup
        .annotations()
        .get("flash.heterocloud.io/restore-verification")
        .and_then(|v| serde_json::from_str::<Value>(v).ok());
    if existing.as_ref().is_some_and(|v| {
        matches!(
            v["phase"].as_str(),
            Some("queued" | "restoring" | "cleaning")
        )
    }) {
        return Ok(());
    }
    let annotation =
        json!({"phase":"queued","started_at":chrono::Utc::now().to_rfc3339()}).to_string();
    Api::<FlashBackup>::namespaced(client,namespace).patch(&backup.name_any(),&PatchParams::default(),&Patch::Merge(json!({"metadata":{"resourceVersion":backup.resource_version(),"annotations":{
        "flash.heterocloud.io/verify-request":Uuid::now_v7().to_string(),"flash.heterocloud.io/restore-verification":annotation}}}))).await?;
    Ok(())
}

pub fn check_scope(backup: &FlashBackup, claims: &ProviderClaims) -> Result<(), BackupError> {
    if backup.spec.organization_id != claims.organization_id
        || backup.spec.project_id != claims.project_id
        || backup.spec.service_instance_id != claims.service_instance_id
    {
        return Err(BackupError::Forbidden);
    }
    Ok(())
}

pub async fn source(
    client: Client,
    namespace: &str,
    claims: &ProviderClaims,
) -> Result<Value, BackupError> {
    let services: Api<FlashService> = Api::namespaced(client.clone(), namespace);
    let flash = services
        .get_opt(&format!("flash-{}", claims.service_instance_id))
        .await?
        .ok_or(BackupError::NotFound)?;
    if flash.spec.organization_id != claims.organization_id.to_string()
        || flash.spec.project_id != claims.project_id.to_string()
    {
        return Err(BackupError::Forbidden);
    }
    if flash.spec.desired_generation != claims.generation
        || flash.metadata.deletion_timestamp.is_some()
    {
        return Err(BackupError::Conflict(
            "source service is changing; retry when its update completes",
        ));
    }
    let claims_api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
    let pvc = claims_api
        .get_opt(&format!("{}-home", flash.name_any()))
        .await?
        .ok_or(BackupError::NotFound)?;
    if pvc.metadata.deletion_timestamp.is_some()
        || pvc.status.as_ref().and_then(|s| s.phase.as_deref()) != Some("Bound")
    {
        return Err(BackupError::Conflict("persistent disk is not bound"));
    }
    let owner = pvc
        .metadata
        .owner_references
        .as_ref()
        .is_some_and(|owners| {
            owners
                .iter()
                .any(|o| o.uid == flash.uid().unwrap_or_default() && o.kind == "FlashService")
        });
    if !owner {
        return Err(BackupError::Forbidden);
    }
    let pv_name = pvc
        .spec
        .as_ref()
        .and_then(|s| s.volume_name.as_deref())
        .ok_or(BackupError::NotFound)?;
    let pv: PersistentVolume = Api::all(client).get(pv_name).await?;
    if pv
        .spec
        .as_ref()
        .and_then(|s| s.csi.as_ref())
        .map(|c| c.driver.as_str())
        != Some("driver.longhorn.io")
    {
        return Err(BackupError::Invalid(
            "this disk driver does not support Flash backups",
        ));
    }
    let quantity = pvc
        .status
        .as_ref()
        .and_then(|s| s.capacity.as_ref())
        .and_then(|s| s.get("storage"))
        .ok_or(BackupError::NotFound)?;
    let size = quantity_bytes(&quantity.0)?;
    let requested = pvc
        .spec
        .as_ref()
        .and_then(|s| s.resources.as_ref())
        .and_then(|r| r.requests.as_ref())
        .and_then(|r| r.get("storage"))
        .ok_or(BackupError::NotFound)?;
    let provisioned = pv
        .spec
        .as_ref()
        .and_then(|s| s.capacity.as_ref())
        .and_then(|c| c.get("storage"))
        .ok_or(BackupError::NotFound)?;
    if quantity_bytes(&requested.0)? != size
        || quantity_bytes(&provisioned.0)? != size
        || pvc
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .is_some_and(|conditions| {
                conditions.iter().any(|c| {
                    c.status == "True"
                        && matches!(c.type_.as_str(), "Resizing" | "FileSystemResizePending")
                })
            })
    {
        return Err(BackupError::Conflict("persistent disk is still resizing"));
    }
    Ok(
        json!({"source_pvc_uid":pvc.uid().ok_or(BackupError::NotFound)?,"volume_size_bytes":size,"generation":flash.spec.desired_generation,
        "resolved_image":flash.status.as_ref().and_then(|s|s.resolved_image.clone())}),
    )
}

pub fn quantity_bytes(value: &str) -> Result<u64, BackupError> {
    let suffixes = [
        ("Ki", 1024u64),
        ("Mi", 1024u64.pow(2)),
        ("Gi", 1024u64.pow(3)),
        ("Ti", 1024u64.pow(4)),
        ("K", 1000),
        ("M", 1000u64.pow(2)),
        ("G", 1000u64.pow(3)),
        ("", 1),
    ];
    for (suffix, scale) in suffixes {
        if let Some(number) = value.strip_suffix(suffix)
            && let Ok(number) = number.parse::<u64>()
            && let Some(bytes) = number.checked_mul(scale)
            && bytes > 0
        {
            return Ok(bytes);
        }
    }
    Err(BackupError::Invalid("invalid persistent disk capacity"))
}

pub async fn create(
    client: Client,
    namespace: &str,
    snapshot_class: &str,
    claims: &ProviderClaims,
    request: BackupRequest,
) -> Result<FlashBackup, BackupError> {
    if snapshot_class.is_empty() {
        return Err(BackupError::Disabled);
    }
    if request.name.trim().is_empty() || request.name.len() > 128 || request.id.is_nil() {
        return Err(BackupError::Invalid("invalid backup name or identifier"));
    }
    let api: Api<FlashBackup> = Api::namespaced(client.clone(), namespace);
    let name = backup_name(request.id);
    if let Some(existing) = api.get_opt(&name).await? {
        check_scope(&existing, claims)?;
        if existing.spec.name != request.name
            || existing.spec.source_pvc_uid != request.source_pvc_uid
            || existing.spec.volume_size_bytes != request.volume_size_bytes
            || existing.metadata.deletion_timestamp.is_some()
        {
            return Err(BackupError::Conflict(
                "backup identifier was already used for another request",
            ));
        }
        return Ok(existing);
    }
    let observed = source(client.clone(), namespace, claims).await?;
    if observed["source_pvc_uid"].as_str() != Some(&request.source_pvc_uid)
        || observed["volume_size_bytes"].as_u64() != Some(request.volume_size_bytes)
    {
        return Err(BackupError::Conflict(
            "source persistent disk changed before backup",
        ));
    }
    let resource = ApiResource::from_gvk(&GroupVersionKind::gvk(
        "snapshot.storage.k8s.io",
        "v1",
        "VolumeSnapshotClass",
    ));
    let class = Api::<DynamicObject>::all_with(client.clone(), &resource)
        .get(snapshot_class)
        .await?;
    if class.data["driver"] != "driver.longhorn.io"
        || class.data["parameters"]["type"] != "bak"
        || class.data["deletionPolicy"] != "Delete"
    {
        return Err(BackupError::Invalid(
            "backup class must use durable CSI backups with managed deletion",
        ));
    }
    let mut backup = FlashBackup::new(
        &name,
        FlashBackupSpec {
            id: request.id,
            name: request.name,
            organization_id: claims.organization_id,
            project_id: claims.project_id,
            service_instance_id: claims.service_instance_id,
            source_pvc: format!("flash-{}-home", claims.service_instance_id),
            source_pvc_uid: request.source_pvc_uid,
            volume_size_bytes: request.volume_size_bytes,
            snapshot_class: snapshot_class.to_owned(),
        },
    );
    backup.metadata.finalizers = Some(vec![FINALIZER.to_owned()]);
    // No source owner reference: deleting a service must not delete its backups.
    backup.metadata.labels = Some(BTreeMap::from([
        (
            "flash.heterocloud.io/organization".into(),
            claims.organization_id.to_string(),
        ),
        (
            "flash.heterocloud.io/instance".into(),
            claims.service_instance_id.to_string(),
        ),
    ]));
    match api.create(&PostParams::default(), &backup).await {
        Ok(backup) => Ok(backup),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            Box::pin(create(
                client,
                namespace,
                snapshot_class,
                claims,
                BackupRequest {
                    id: backup.spec.id,
                    name: backup.spec.name,
                    source_pvc_uid: backup.spec.source_pvc_uid,
                    volume_size_bytes: backup.spec.volume_size_bytes,
                },
            ))
            .await
        }
        Err(error) => Err(error.into()),
    }
}

pub async fn get(
    client: Client,
    namespace: &str,
    id: Uuid,
    claims: &ProviderClaims,
) -> Result<FlashBackup, BackupError> {
    let backup = Api::<FlashBackup>::namespaced(client, namespace)
        .get_opt(&backup_name(id))
        .await?
        .ok_or(BackupError::NotFound)?;
    check_scope(&backup, claims)?;
    Ok(backup)
}

pub async fn delete(
    client: Client,
    namespace: &str,
    id: Uuid,
    claims: &ProviderClaims,
) -> Result<bool, BackupError> {
    let backup = match get(client.clone(), namespace, id, claims).await {
        Ok(b) => b,
        Err(BackupError::NotFound) => return Ok(true),
        Err(e) => return Err(e),
    };
    if offsite_locked(&backup) {
        return Err(BackupError::Conflict(
            "an offsite copy is using this backup",
        ));
    }
    if restore_pending(client.clone(), namespace, id).await? {
        return Err(BackupError::Conflict(
            "a disk restore is still using this backup",
        ));
    }
    if backup.metadata.deletion_timestamp.is_none() {
        let params = DeleteParams {
            preconditions: Some(kube::api::Preconditions {
                uid: backup.uid(),
                resource_version: backup.resource_version(),
            }),
            ..Default::default()
        };
        Api::<FlashBackup>::namespaced(client, namespace)
            .delete(&backup.name_any(), &params)
            .await?;
    }
    Ok(false)
}

fn offsite_locked(backup: &FlashBackup) -> bool {
    backup
        .annotations()
        .get("flash.heterocloud.io/offsite-lock-until")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .is_some_and(|until| until.timestamp() > chrono::Utc::now().timestamp())
}

async fn restore_pending(client: Client, namespace: &str, id: Uuid) -> Result<bool, BackupError> {
    if !Api::<PersistentVolumeClaim>::namespaced(client.clone(), namespace)
        .list(
            &ListParams::default()
                .labels(&format!("flash.heterocloud.io/backup-verification={id}")),
        )
        .await?
        .items
        .is_empty()
    {
        return Ok(true);
    }
    let services = Api::<FlashService>::namespaced(client.clone(), namespace)
        .list(&ListParams::default())
        .await?;
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
    for flash in services {
        if flash.spec.workload.restore_from_backup_id != Some(id)
            || flash.metadata.deletion_timestamp.is_some()
        {
            continue;
        }
        let pvc = pvcs.get_opt(&format!("{}-home", flash.name_any())).await?;
        if match pvc {
            Some(pvc) => !disk_restore_complete(client.clone(), &pvc).await?,
            None => true,
        } {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Longhorn CreateVolume returns before copying backup data. PVC Bound alone
/// cannot release the source backup or report a stopped restore as complete.
async fn disk_restore_complete(
    client: Client,
    pvc: &PersistentVolumeClaim,
) -> Result<bool, BackupError> {
    if pvc.status.as_ref().and_then(|s| s.phase.as_deref()) != Some("Bound") {
        return Ok(false);
    }
    let name = pvc
        .spec
        .as_ref()
        .and_then(|s| s.volume_name.as_deref())
        .ok_or(BackupError::NotFound)?;
    let pv = Api::<PersistentVolume>::all(client.clone())
        .get(name)
        .await?;
    let csi = pv
        .spec
        .as_ref()
        .and_then(|s| s.csi.as_ref())
        .ok_or(BackupError::Invalid("restore disk must use CSI"))?;
    if csi.driver != "driver.longhorn.io" {
        return Err(BackupError::Invalid(
            "restore disk must use the backup storage driver",
        ));
    }
    let resource =
        ApiResource::from_gvk(&GroupVersionKind::gvk("longhorn.io", "v1beta2", "Volume"));
    let storage_namespace = std::env::var("FLASH_BACKUP_STORAGE_NAMESPACE")
        .unwrap_or_else(|_| "longhorn-system".into());
    let volume = Api::<DynamicObject>::namespaced_with(client, &storage_namespace, &resource)
        .get(&csi.volume_handle)
        .await?;
    Ok(volume.data["status"]["restoreInitiated"] == true
        && volume.data["status"]["restoreRequired"] == false)
}

pub async fn ensure_restored_home(
    client: Client,
    namespace: &str,
    flash: &FlashService,
    storage_class: &str,
) -> Result<bool, BackupError> {
    let id = flash
        .spec
        .workload
        .restore_from_backup_id
        .ok_or(BackupError::Invalid("restore source is missing"))?;
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
    let name = format!("{}-home", flash.name_any());
    if let Some(existing) = api.get_opt(&name).await? {
        let source = existing.spec.as_ref().and_then(|s| s.data_source.as_ref());
        if source.map(|s| s.name.as_str()) != Some(backup_name(id).as_str())
            || existing
                .metadata
                .owner_references
                .as_ref()
                .is_none_or(|o| !o.iter().any(|r| r.uid == flash.uid().unwrap_or_default()))
        {
            return Err(BackupError::Conflict(
                "existing disk has a different owner or restore source",
            ));
        }
        return disk_restore_complete(client, &existing).await;
    }
    let backups: Api<FlashBackup> = Api::namespaced(client.clone(), namespace);
    let backup = backups
        .get_opt(&backup_name(id))
        .await?
        .ok_or(BackupError::NotFound)?;
    if backup.spec.organization_id.to_string() != flash.spec.organization_id {
        return Err(BackupError::Forbidden);
    }
    if backup.metadata.deletion_timestamp.is_some()
        || backup.status.as_ref().is_none_or(|s| s.phase != "ready")
    {
        return Err(BackupError::Conflict("backup is not ready for restoration"));
    }
    let mut claim: PersistentVolumeClaim = serde_json::from_value(json!({
        "apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":name,
            "ownerReferences":[flash.controller_owner_ref(&()).ok_or(BackupError::NotFound)?],
            "labels":{"flash.heterocloud.io/instance":flash.spec.service_instance_id,"flash.heterocloud.io/organization":flash.spec.organization_id}},
        "spec":{"storageClassName":storage_class,"accessModes":["ReadWriteMany"],"volumeMode":"Filesystem",
            "resources":{"requests":{"storage":backup.spec.volume_size_bytes.to_string()}},
            "dataSource":{"apiGroup":"snapshot.storage.k8s.io","kind":"VolumeSnapshot","name":backup_name(id)}}
    }))?;
    claim.metadata.namespace = Some(namespace.to_owned());
    match api.create(&PostParams::default(), &claim).await {
        Ok(_) => {}
        Err(kube::Error::Api(e)) if e.code == 409 => {}
        Err(e) => return Err(e.into()),
    }
    Ok(false)
}

async fn status(
    api: &Api<FlashBackup>,
    backup: &FlashBackup,
    value: FlashBackupStatus,
) -> Result<(), BackupError> {
    if backup.status.as_ref() != Some(&value) {
        api.patch_status(
            &backup.name_any(),
            &PatchParams::default(),
            &Patch::Merge(
                json!({"metadata":{"resourceVersion":backup.resource_version()},"status":value}),
            ),
        )
        .await?;
    }
    Ok(())
}

async fn reconcile(
    client: Client,
    namespace: &str,
    backup: FlashBackup,
) -> Result<(), BackupError> {
    let api: Api<FlashBackup> = Api::namespaced(client.clone(), namespace);
    let snapshots = snapshots(client.clone(), namespace);
    let name = backup.name_any();
    if backup.metadata.deletion_timestamp.is_some() {
        if offsite_locked(&backup) {
            return Ok(());
        }
        if restore_pending(client, namespace, backup.spec.id).await? {
            return Ok(());
        }
        if snapshots.get_opt(&name).await?.is_some() {
            match snapshots.delete(&name, &DeleteParams::default()).await {
                Ok(_) => {}
                Err(kube::Error::Api(e)) if e.code == 404 => {}
                Err(e) => return Err(e.into()),
            }
            return Ok(());
        }
        let remaining: Vec<_> = backup
            .finalizers()
            .iter()
            .filter(|f| f.as_str() != FINALIZER)
            .cloned()
            .collect();
        api.patch(&name,&PatchParams::default(),&Patch::Merge(json!({"metadata":{"resourceVersion":backup.resource_version(),"finalizers":remaining}}))).await?;
        return Ok(());
    }
    let snapshot = if let Some(snapshot) = snapshots.get_opt(&name).await? {
        snapshot
    } else {
        // Never silently recreate a completed backup from a later source state.
        if backup
            .status
            .as_ref()
            .is_some_and(|s| s.phase == "ready" || s.phase == "error")
        {
            return status(
                &api,
                &backup,
                FlashBackupStatus {
                    phase: "error".into(),
                    message: Some("backup_storage_missing".into()),
                    completed_at: None,
                },
            )
            .await;
        }
        let pvc = Api::<PersistentVolumeClaim>::namespaced(client, namespace)
            .get_opt(&backup.spec.source_pvc)
            .await?
            .ok_or(BackupError::NotFound)?;
        if pvc.uid().as_deref() != Some(&backup.spec.source_pvc_uid) {
            return Err(BackupError::Conflict("source disk identity changed"));
        }
        let desired: DynamicObject = serde_json::from_value(
            json!({"apiVersion":"snapshot.storage.k8s.io/v1","kind":"VolumeSnapshot",
            "metadata":{"name":name,"namespace":namespace,"ownerReferences":[backup.controller_owner_ref(&()).ok_or(BackupError::NotFound)?]},
            "spec":{"volumeSnapshotClassName":backup.spec.snapshot_class,"source":{"persistentVolumeClaimName":backup.spec.source_pvc}}}),
        )?;
        snapshots
            .patch(&name, &PatchParams::apply(MANAGER), &Patch::Apply(&desired))
            .await?
    };
    let value = &snapshot.data["status"];
    let expired = backup
        .metadata
        .creation_timestamp
        .as_ref()
        .is_some_and(|t| {
            chrono::Utc::now()
                .timestamp()
                .saturating_sub(t.0.as_second())
                >= 1800
        });
    let phase = if value["readyToUse"] == true {
        "ready"
    } else if expired {
        "error"
    } else {
        "creating"
    };
    status(
        &api,
        &backup,
        FlashBackupStatus {
            phase: phase.into(),
            message: (phase == "error").then(|| "backup_storage_error".into()),
            completed_at: if phase == "ready" {
                Some(
                    backup
                        .status
                        .as_ref()
                        .and_then(|s| s.completed_at)
                        .unwrap_or_else(|| chrono::Utc::now().timestamp()),
                )
            } else {
                None
            },
        },
    )
    .await
}

pub async fn run_controller(client: Client, namespace: String) -> anyhow::Result<()> {
    let api: Api<FlashBackup> = Api::namespaced(client.clone(), &namespace);
    let mut timer = tokio::time::interval(Duration::from_secs(5));
    loop {
        timer.tick().await;
        let backups = api.list(&ListParams::default()).await?;
        for backup in backups {
            let id = backup.spec.id;
            if let Err(error) = reconcile(client.clone(), &namespace, backup.clone()).await {
                if matches!(
                    &error,
                    BackupError::NotFound
                        | BackupError::Conflict(_)
                        | BackupError::Invalid(_)
                        | BackupError::Forbidden
                ) {
                    let _ = status(
                        &api,
                        &backup,
                        FlashBackupStatus {
                            phase: "error".into(),
                            message: Some("backup_source_unavailable".into()),
                            completed_at: None,
                        },
                    )
                    .await;
                }
                // Backend errors may contain provider URLs. Keep them out of logs.
                tracing::warn!(backup=%id,"Flash disk backup reconciliation will retry");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn test_claims() -> anyhow::Result<ProviderClaims> {
        Ok(serde_json::from_value(
            json!({"iss":"test","aud":"flash","sub":Uuid::from_u128(4),
            "organization_id":Uuid::from_u128(1),"project_id":Uuid::from_u128(2),"service_instance_id":Uuid::from_u128(3),
            "action":"flash.backups.get","generation":1,"jti":Uuid::from_u128(5),"iat":0,"nbf":0,"exp":60}),
        )?)
    }
    #[test]
    fn a_signed_command_must_match_all_three_backup_scopes() -> anyhow::Result<()> {
        let claims = test_claims()?;
        let backup = FlashBackup::new(
            "scoped",
            FlashBackupSpec {
                id: Uuid::from_u128(8),
                name: "checkpoint".into(),
                organization_id: claims.organization_id,
                project_id: claims.project_id,
                service_instance_id: claims.service_instance_id,
                source_pvc: "source".into(),
                source_pvc_uid: "uid".into(),
                volume_size_bytes: 1024,
                snapshot_class: "backup".into(),
            },
        );
        check_scope(&backup, &claims)?;
        for field in ["organization_id", "project_id", "service_instance_id"] {
            let mut value = serde_json::to_value(&claims)?;
            value[field] = json!(Uuid::from_u128(99));
            assert!(matches!(
                check_scope(&backup, &serde_json::from_value(value)?),
                Err(BackupError::Forbidden)
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn bound_restore_survives_backup_removal_but_never_overwrites_an_existing_disk()
    -> anyhow::Result<()> {
        use kube::client::Body;
        for (correct_owner, copy_complete) in [(true, true), (true, false), (false, false)] {
            let flash: FlashService = serde_json::from_value(
                json!({"metadata":{"name":"restored","uid":"new-service-uid"},"spec":{
                    "desired_generation":1,"display_name":"restored","organization_id":Uuid::from_u128(1).to_string(),"project_id":Uuid::from_u128(2).to_string(),"service_instance_id":Uuid::from_u128(3).to_string(),
                    "workload":{"region":"test","image":"example/app:v1","replicas":1,"cpu_millis":100,"memory_mib":128,"stopped":true,"restore_from_backup_id":Uuid::from_u128(8),
                        "exposure":{"type":"internal","traffic_mode":"forwarded"},"ports":[],"env":{},"command":[],"args":[],"metadata":{}}
                }}),
            )?;
            let client = Client::new(
                tower::service_fn(move |request: http::Request<Body>| async move {
                    assert_eq!(
                        request.method(),
                        http::Method::GET,
                        "restoration must never replace or mutate an existing claim"
                    );
                    let pvc = json!({"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":"restored-home","ownerReferences":[{"apiVersion":"flash.heterocloud.io/v1alpha1","kind":"FlashService","name":"restored","uid":if correct_owner {"new-service-uid"} else {"another-service"}}]},
                    "spec":{"volumeName":"new-pv","dataSource":{"apiGroup":"snapshot.storage.k8s.io","kind":"VolumeSnapshot","name":backup_name(Uuid::from_u128(8))}},"status":{"phase":"Bound"}});
                    let value = match request.uri().path() {
                        "/api/v1/namespaces/test/persistentvolumeclaims/restored-home" => pvc,
                        "/api/v1/persistentvolumes/new-pv" => {
                            json!({"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"new-pv"},"spec":{"csi":{"driver":"driver.longhorn.io","volumeHandle":"new-volume"}}})
                        }
                        "/apis/longhorn.io/v1beta2/namespaces/longhorn-system/volumes/new-volume" =>
                        {
                            json!({"apiVersion":"longhorn.io/v1beta2","kind":"Volume","metadata":{"name":"new-volume"},"status":{"restoreInitiated":true,"restoreRequired":!copy_complete}})
                        }
                        other => {
                            return Err(std::io::Error::other(format!(
                                "unexpected storage access {other}; completed restores must not read deleted backups"
                            )));
                        }
                    };
                    Ok::<_, std::io::Error>(http::Response::new(Body::from(
                        value.to_string().into_bytes(),
                    )))
                }),
                "test",
            );
            let result = ensure_restored_home(client, "test", &flash, "longhorn").await;
            if correct_owner {
                assert_eq!(
                    result?, copy_complete,
                    "PVC Bound cannot mark a stopped restore complete while data is still copying"
                );
            } else {
                assert!(matches!(result, Err(BackupError::Conflict(_))));
            }
        }
        Ok(())
    }
    #[test]
    fn backup_view_does_not_disclose_storage_paths_or_credentials() -> anyhow::Result<()> {
        let backup = FlashBackup::new(
            "test",
            FlashBackupSpec {
                id: Uuid::from_u128(1),
                name: "daily".into(),
                organization_id: Uuid::nil(),
                project_id: Uuid::nil(),
                service_instance_id: Uuid::nil(),
                source_pvc: "private-pvc".into(),
                source_pvc_uid: "private-uid".into(),
                volume_size_bytes: 1024,
                snapshot_class: "private-class".into(),
            },
        );
        let rendered = view(&backup).to_string();
        assert!(!rendered.contains("private-"));
        assert_eq!(view(&backup)["phase"], "pending");
        Ok(())
    }
    #[test]
    fn source_disk_capacity_must_be_positive_and_bounded() -> anyhow::Result<()> {
        assert_eq!(quantity_bytes("2Gi")?, 2 * 1024u64.pow(3));
        assert_eq!(quantity_bytes("1073741824")?, 1024u64.pow(3));
        assert!(quantity_bytes("0").is_err());
        assert!(quantity_bytes("18446744073709551615Ti").is_err());
        assert!(quantity_bytes("-1Gi").is_err());
        Ok(())
    }
}
