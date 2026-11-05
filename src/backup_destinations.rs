//! Tenant-scoped Longhorn backup destinations. Credentials are only included
//! in the returned Kubernetes Secret, never in target metadata or status.
use std::net::IpAddr;

use hmac::{Hmac, Mac};
use ipnet::IpNet;
use k8s_openapi::api::{core::v1::Secret, storage::v1::StorageClass};
use kube::{
    Api, Client,
    api::{ApiResource, DynamicObject, GroupVersionKind, Patch, PatchParams},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use url::{Host, Url};
use uuid::Uuid;

const TOKEN_DOMAIN: &[u8] = b"heterocloud-backup-s3-proxy-v1\0";

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Destination {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub virtual_hosted: bool,
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DestinationError {
    #[error("invalid S3 destination configuration")]
    Invalid,
    #[error("backup destination proxy is unavailable")]
    ProxyUnavailable,
    #[error("backup storage class is invalid")]
    InvalidStorageClass,
    #[error("backup destination ownership or configuration conflicts")]
    Conflict,
    #[error("backup destination storage is unavailable")]
    StorageUnavailable,
}

#[derive(Clone, Debug, Serialize)]
pub struct DestinationView {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub target_name: String,
    pub ready: bool,
}

pub struct Manifests {
    pub secret: Value,
    pub target: Value,
    pub restore_class: Value,
    pub verification_class: Value,
}

fn labels_match(
    labels: Option<&std::collections::BTreeMap<String, String>>,
    organization: Uuid,
    id: Uuid,
) -> bool {
    labels.is_some_and(|labels| {
        labels.get("flash.heterocloud.io/organization") == Some(&organization.to_string())
            && labels.get("flash.heterocloud.io/backup-destination") == Some(&id.to_string())
    })
}

fn target_api(client: Client, namespace: &str) -> Api<DynamicObject> {
    let resource = ApiResource::from_gvk(&GroupVersionKind::gvk(
        "longhorn.io",
        "v1beta2",
        "BackupTarget",
    ));
    Api::namespaced_with(client, namespace, &resource)
}

/// Register or rotate credentials without changing the target URL of an
/// existing destination. Changing an existing target could orphan backups.
#[allow(clippy::too_many_arguments)]
pub async fn put(
    client: Client,
    organization: Uuid,
    id: Uuid,
    input: &S3Destination,
    proxy_origin: &str,
    proxy_key: &[u8],
    storage_namespace: &str,
    restore_template: &str,
    verification_template: &str,
) -> Result<DestinationView, DestinationError> {
    let classes: Api<StorageClass> = Api::all(client.clone());
    let restore = classes
        .get(restore_template)
        .await
        .map_err(|_| DestinationError::InvalidStorageClass)?;
    let verification = classes
        .get(verification_template)
        .await
        .map_err(|_| DestinationError::InvalidStorageClass)?;
    let restore =
        serde_json::to_value(restore).map_err(|_| DestinationError::InvalidStorageClass)?;
    let verification =
        serde_json::to_value(verification).map_err(|_| DestinationError::InvalidStorageClass)?;
    let desired = manifests(
        organization,
        id,
        input,
        proxy_origin,
        proxy_key,
        storage_namespace,
        &restore,
        &verification,
    )?;
    let name = target_name(id);
    let targets = target_api(client.clone(), storage_namespace);
    if let Some(existing) = targets
        .get_opt(&name)
        .await
        .map_err(|_| DestinationError::StorageUnavailable)?
        && (!labels_match(existing.metadata.labels.as_ref(), organization, id)
            || existing.data["spec"]["backupTargetURL"] != desired.target["spec"]["backupTargetURL"]
            || existing.data["spec"]["credentialSecret"] != name)
    {
        return Err(DestinationError::Conflict);
    }
    let secrets: Api<Secret> = Api::namespaced(client, storage_namespace);
    if let Some(existing) = secrets
        .get_opt(&name)
        .await
        .map_err(|_| DestinationError::StorageUnavailable)?
        && !labels_match(existing.metadata.labels.as_ref(), organization, id)
    {
        return Err(DestinationError::Conflict);
    }
    for class in [&desired.restore_class, &desired.verification_class] {
        let class_name = class["metadata"]["name"]
            .as_str()
            .ok_or(DestinationError::InvalidStorageClass)?;
        if let Some(existing) = classes
            .get_opt(class_name)
            .await
            .map_err(|_| DestinationError::StorageUnavailable)?
        {
            let serialized = serde_json::to_value(&existing)
                .map_err(|_| DestinationError::InvalidStorageClass)?;
            if !labels_match(existing.metadata.labels.as_ref(), organization, id)
                || serialized["provisioner"] != class["provisioner"]
                || serialized["parameters"] != class["parameters"]
                || serialized["reclaimPolicy"] != class["reclaimPolicy"]
            {
                return Err(DestinationError::Conflict);
            }
        }
    }
    let params = PatchParams::apply("heterocloud-flash-backup-destination");
    secrets
        .patch(&name, &params, &Patch::Apply(&desired.secret))
        .await
        .map_err(|_| DestinationError::StorageUnavailable)?;
    for class in [&desired.restore_class, &desired.verification_class] {
        let class_name = class["metadata"]["name"]
            .as_str()
            .ok_or(DestinationError::InvalidStorageClass)?;
        classes
            .patch(class_name, &params, &Patch::Apply(class))
            .await
            .map_err(|_| DestinationError::StorageUnavailable)?;
    }
    let target = targets
        .patch(&name, &params, &Patch::Apply(&desired.target))
        .await
        .map_err(|_| DestinationError::StorageUnavailable)?;
    Ok(DestinationView {
        id,
        organization_id: organization,
        target_name: name,
        ready: target.data["status"]["available"] == true,
    })
}

/// Return only availability and ownership, not bucket credentials or backend errors.
pub async fn get(
    client: Client,
    namespace: &str,
    organization: Uuid,
    id: Uuid,
) -> Result<DestinationView, DestinationError> {
    let name = target_name(id);
    let target = target_api(client, namespace)
        .get(&name)
        .await
        .map_err(|_| DestinationError::StorageUnavailable)?;
    if !labels_match(target.metadata.labels.as_ref(), organization, id) {
        return Err(DestinationError::Conflict);
    }
    Ok(DestinationView {
        id,
        organization_id: organization,
        target_name: name,
        ready: target.data["status"]["available"] == true,
    })
}

#[must_use]
pub fn target_name(id: Uuid) -> String {
    format!("hc-backup-{}", id.simple())
}

#[must_use]
pub fn verification_class_name(id: Uuid) -> String {
    format!("hc-verify-{}", id.simple())
}

fn protected_ip(address: IpAddr) -> bool {
    // The authenticated egress proxy performs the definitive DNS/IP check on
    // every connection. This also prevents Go's localhost proxy bypass.
    const NETWORKS: &[&str] = &[
        "0.0.0.0/8",
        "10.0.0.0/8",
        "100.64.0.0/10",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "224.0.0.0/4",
        "240.0.0.0/4",
        "::/128",
        "::1/128",
        "::ffff:0:0/96",
        "fc00::/7",
        "fe80::/10",
        "ff00::/8",
    ];
    NETWORKS.iter().any(|network| {
        network
            .parse::<IpNet>()
            .is_ok_and(|net| net.contains(&address))
    })
}

fn endpoint(value: &str) -> Result<(Url, String, u16), DestinationError> {
    let url = Url::parse(value).map_err(|_| DestinationError::Invalid)?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || !matches!(url.path(), "" | "/")
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(DestinationError::Invalid);
    }
    let host = match url.host().ok_or(DestinationError::Invalid)? {
        Host::Domain(host) => {
            let normalized = host.trim_end_matches('.').to_ascii_lowercase();
            if normalized.is_empty()
                || normalized == "localhost"
                || [
                    ".localhost",
                    ".local",
                    ".internal",
                    ".svc",
                    ".cluster.local",
                ]
                .iter()
                .any(|suffix| normalized.ends_with(suffix))
            {
                return Err(DestinationError::Invalid);
            }
            normalized
        }
        Host::Ipv4(ip) => {
            if protected_ip(IpAddr::V4(ip)) {
                return Err(DestinationError::Invalid);
            }
            ip.to_string()
        }
        Host::Ipv6(ip) => {
            if protected_ip(IpAddr::V6(ip)) {
                return Err(DestinationError::Invalid);
            }
            ip.to_string()
        }
    };
    let port = url
        .port_or_known_default()
        .ok_or(DestinationError::Invalid)?;
    Ok((url, host, port))
}

fn valid_bucket(value: &str) -> bool {
    (3..=63).contains(&value.len())
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'.'))
        && !value.contains("..")
        && !value.contains(".-")
        && !value.contains("-.")
        && value.parse::<std::net::Ipv4Addr>().is_err()
}

fn valid_prefix(value: &str) -> bool {
    value.len() <= 512
        && !value.starts_with('/')
        && value.split('/').all(|part| part != "." && part != "..")
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'/'))
}

fn valid_credential(value: &str) -> bool {
    !value.is_empty() && value.len() <= 4096 && !value.chars().any(char::is_control)
}

fn proxy_token(
    key: &[u8],
    identity: &str,
    host: &str,
    port: u16,
) -> Result<String, DestinationError> {
    if key.len() < 32 {
        return Err(DestinationError::ProxyUnavailable);
    }
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key).map_err(|_| DestinationError::ProxyUnavailable)?;
    mac.update(TOKEN_DOMAIN);
    mac.update(identity.as_bytes());
    mac.update(&[0]);
    mac.update(host.as_bytes());
    mac.update(&[0]);
    mac.update(port.to_string().as_bytes());
    Ok(mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn storage_class(
    template: &Value,
    name: &str,
    labels: &Value,
    target: &str,
) -> Result<Value, DestinationError> {
    if template["provisioner"] != "driver.longhorn.io" {
        return Err(DestinationError::InvalidStorageClass);
    }
    let mut parameters = template["parameters"]
        .as_object()
        .cloned()
        .ok_or(DestinationError::InvalidStorageClass)?;
    if parameters
        .get("fromBackup")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.is_empty())
    {
        return Err(DestinationError::InvalidStorageClass);
    }
    parameters.insert("backupTargetName".into(), json!(target));
    let mut result = json!({"apiVersion":"storage.k8s.io/v1", "kind":"StorageClass",
        "metadata":{"name":name,"labels":labels}, "provisioner":"driver.longhorn.io", "parameters":parameters});
    for key in [
        "reclaimPolicy",
        "volumeBindingMode",
        "allowVolumeExpansion",
        "mountOptions",
        "allowedTopologies",
    ] {
        if let Some(value) = template.get(key) {
            result[key] = value.clone();
        }
    }
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
pub fn manifests(
    organization: Uuid,
    id: Uuid,
    input: &S3Destination,
    proxy_origin: &str,
    proxy_key: &[u8],
    storage_namespace: &str,
    restore_template: &Value,
    verification_template: &Value,
) -> Result<Manifests, DestinationError> {
    if organization.is_nil()
        || id.is_nil()
        || !valid_bucket(&input.bucket)
        || !valid_prefix(&input.prefix)
        || input.region.is_empty()
        || input.region.len() > 128
        || !input
            .region
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        || !valid_credential(&input.access_key)
        || !valid_credential(&input.secret_key)
    {
        return Err(DestinationError::Invalid);
    }
    let (endpoint, host, port) = endpoint(&input.endpoint)?;
    let host = if input.virtual_hosted {
        if host.parse::<IpAddr>().is_ok() {
            return Err(DestinationError::Invalid);
        }
        format!("{}.{host}", input.bucket)
    } else {
        host
    };
    let identity = format!("{}.{}", organization.simple(), id.simple());
    let token = proxy_token(proxy_key, &identity, &host, port)?;
    let mut proxy = Url::parse(proxy_origin).map_err(|_| DestinationError::ProxyUnavailable)?;
    if proxy.scheme() != "http"
        || proxy.host_str().is_none()
        || !proxy.username().is_empty()
        || proxy.password().is_some()
        || !matches!(proxy.path(), "" | "/")
        || proxy.query().is_some()
        || proxy.fragment().is_some()
    {
        return Err(DestinationError::ProxyUnavailable);
    }
    proxy
        .set_username(&identity)
        .map_err(|_| DestinationError::ProxyUnavailable)?;
    proxy
        .set_password(Some(&token))
        .map_err(|_| DestinationError::ProxyUnavailable)?;
    let name = target_name(id);
    let labels = json!({"flash.heterocloud.io/backup-destination":id.to_string(),
        "flash.heterocloud.io/organization":organization.to_string()});
    let prefix = input.prefix.trim_end_matches('/');
    let root = if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    };
    let target_url = format!(
        "s3://{}@{}/{}heterocloud/{}/{}",
        input.bucket,
        input.region,
        root,
        organization.simple(),
        id.simple()
    );
    let proxy_url = proxy.to_string();
    let secret = json!({"apiVersion":"v1","kind":"Secret","metadata":{"name":name,"namespace":storage_namespace,"labels":labels},
        "type":"Opaque","stringData":{"AWS_ACCESS_KEY_ID":input.access_key,"AWS_SECRET_ACCESS_KEY":input.secret_key,
        "AWS_ENDPOINTS":endpoint.as_str(),"VIRTUAL_HOSTED_STYLE":input.virtual_hosted.to_string(),
        "HTTPS_PROXY":proxy_url,"HTTP_PROXY":proxy_url,"NO_PROXY":""}});
    let target = json!({"apiVersion":"longhorn.io/v1beta2","kind":"BackupTarget",
        "metadata":{"name":name,"namespace":storage_namespace,"labels":labels},
        "spec":{"backupTargetURL":target_url,"credentialSecret":name,"pollInterval":"1m"}});
    Ok(Manifests {
        secret,
        target,
        restore_class: storage_class(restore_template, &name, &labels, &name)?,
        verification_class: storage_class(
            verification_template,
            &verification_class_name(id),
            &labels,
            &name,
        )?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> S3Destination {
        S3Destination {
            endpoint: "https://storage.example:443".into(),
            bucket: "sample-bucket".into(),
            region: "us-east-1".into(),
            prefix: "backups".into(),
            virtual_hosted: false,
            access_key: "fake-access-key".into(),
            secret_key: "fake-secret-key".into(),
        }
    }

    #[test]
    fn proxy_signature_matches_the_proxy_implementation() -> Result<(), DestinationError> {
        let identity = format!("{}.{}", "a".repeat(32), "b".repeat(32));
        assert_eq!(
            proxy_token(
                b"test-only-proxy-signing-key-32-bytes",
                &identity,
                "storage.example",
                443
            )?,
            "9d6083e50448421fdde92aa1d0ae92f7530b9c023676a63959554fbd480b2535"
        );
        Ok(())
    }

    #[test]
    fn credentials_only_enter_the_secret_and_destination_is_org_scoped()
    -> Result<(), DestinationError> {
        let org = Uuid::from_u128(1);
        let id = Uuid::from_u128(2);
        let sc = json!({"provisioner":"driver.longhorn.io","parameters":{"numberOfReplicas":"2"},"reclaimPolicy":"Delete"});
        let made = manifests(
            org,
            id,
            &fixture(),
            "http://backup-proxy.ns.svc:8088",
            &[3; 32],
            "longhorn-system",
            &sc,
            &sc,
        )?;
        assert_eq!(
            made.secret["stringData"]["AWS_ACCESS_KEY_ID"],
            "fake-access-key"
        );
        assert_eq!(
            made.secret["stringData"]["AWS_SECRET_ACCESS_KEY"],
            "fake-secret-key"
        );
        for value in [&made.target, &made.restore_class, &made.verification_class] {
            let text = value.to_string();
            assert!(!text.contains("fake-access-key"));
            assert!(!text.contains("fake-secret-key"));
        }
        assert!(
            made.target["spec"]["backupTargetURL"]
                .as_str()
                .is_some_and(|v| v.contains(&org.simple().to_string())
                    && v.contains(&id.simple().to_string()))
        );
        assert_eq!(
            made.restore_class["parameters"]["backupTargetName"],
            target_name(id)
        );
        assert_eq!(
            made.verification_class["parameters"]["backupTargetName"],
            target_name(id)
        );
        Ok(())
    }

    #[test]
    fn internal_origins_and_unsafe_object_paths_are_rejected() {
        let sc = json!({"provisioner":"driver.longhorn.io","parameters":{}});
        for value in [
            "http://storage.example",
            "https://localhost",
            "https://169.254.169.254",
            "https://service.ns.svc.cluster.local",
            "https://user:secret@storage.example",
            "https://storage.example/path",
        ] {
            let mut input = fixture();
            input.endpoint = value.into();
            assert!(
                manifests(
                    Uuid::from_u128(1),
                    Uuid::from_u128(2),
                    &input,
                    "http://proxy.ns.svc:8088",
                    &[3; 32],
                    "longhorn-system",
                    &sc,
                    &sc
                )
                .is_err()
            );
        }
        for prefix in ["/absolute", "one/../two", "one/./two", "contains spaces"] {
            let mut input = fixture();
            input.prefix = prefix.into();
            assert!(
                manifests(
                    Uuid::from_u128(1),
                    Uuid::from_u128(2),
                    &input,
                    "http://proxy.ns.svc:8088",
                    &[3; 32],
                    "longhorn-system",
                    &sc,
                    &sc
                )
                .is_err()
            );
        }
    }
}
