//! Domain aliases are separate resources: adding a name never changes a Pod.
use crate::{
    crd::FlashService,
    lb_auth::{self, SecurityPolicy},
    web::{HTTPRoute, route_is_ready},
};
use anyhow::Result;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::{
    Api, Client, CustomResource, ResourceExt,
    api::{DeleteParams, ListParams, Patch, PatchParams},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const LIST_ACTION: &str = "flash.domains.list";
pub const WRITE_ACTION: &str = "flash.domains.write";

pub fn validate_hostname(value: &str) -> Result<()> {
    anyhow::ensure!(
        value.len() >= 3
            && value.len() <= 253
            && value.contains('.')
            && value.parse::<std::net::IpAddr>().is_err(),
        "invalid public hostname"
    );
    anyhow::ensure!(
        ![".internal", ".localhost", ".local"]
            .iter()
            .any(|s| value.ends_with(s)),
        "private hostnames are not supported"
    );
    for label in value.split('.') {
        anyhow::ensure!(
            !label.is_empty()
                && label.len() <= 63
                && label.as_bytes()[0].is_ascii_alphanumeric()
                && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                && label
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
            "hostname must be canonical lowercase DNS without scheme, path, port or wildcard"
        );
    }
    Ok(())
}

pub fn resource_name(hostname: &str) -> String {
    format!(
        "flash-domain-{}",
        Uuid::new_v5(&Uuid::NAMESPACE_DNS, hostname.as_bytes()).simple()
    )
}

#[derive(CustomResource, Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "flash.heterocloud.io",
    version = "v1alpha1",
    kind = "FlashDomain",
    plural = "flashdomains",
    namespaced,
    status = "FlashDomainStatus"
)]
#[serde(deny_unknown_fields)]
pub struct FlashDomainSpec {
    pub hostname: String,
    pub cname_target: String,
    pub binding_id: String,
    pub verification_value: String,
    pub service_instance_id: String,
    pub organization_id: String,
    pub project_id: String,
    pub service_uid: String,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FlashDomainStatus {
    #[serde(default)]
    pub observed_generation: i64,
    #[serde(default)]
    pub phase: String,
    #[serde(default)]
    pub dns_verified: bool,
    #[serde(default)]
    pub tls_ready: bool,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub certificate_expires_at: Option<String>,
}

pub fn owned_by(alias: &FlashDomain, flash: &FlashService) -> bool {
    alias.spec.service_instance_id == flash.spec.service_instance_id
        && alias.spec.organization_id == flash.spec.organization_id
        && alias.spec.project_id == flash.spec.project_id
        && flash.metadata.uid.as_deref() == Some(&alias.spec.service_uid)
        && alias
            .metadata
            .owner_references
            .as_ref()
            .is_some_and(|refs| {
                refs.iter().any(|r| {
                    r.kind == "FlashService" && Some(&r.uid) == flash.metadata.uid.as_ref()
                })
            })
}

pub async fn reconcile_routes(
    client: Client,
    namespace: &str,
    flash: &FlashService,
    owner: &OwnerReference,
    base: Option<&HTTPRoute>,
) -> Result<()> {
    let aliases = Api::<FlashDomain>::namespaced(client.clone(), namespace)
        .list(&ListParams::default().labels(&format!(
            "flash.heterocloud.io/instance={}",
            flash.spec.service_instance_id
        )))
        .await?;
    let routes = Api::<HTTPRoute>::namespaced(client.clone(), namespace);
    let policies = Api::<SecurityPolicy>::namespaced(client.clone(), namespace);
    for alias in aliases.items {
        anyhow::ensure!(owned_by(&alias, flash), "custom hostname owner differs");
        validate_hostname(&alias.spec.hostname)?;
        anyhow::ensure!(
            alias.name_any() == resource_name(&alias.spec.hostname),
            "custom hostname resource name differs"
        );
        let ready = alias.metadata.deletion_timestamp.is_none()
            && alias.status.as_ref().is_some_and(|s| {
                s.dns_verified
                    && s.tls_ready
                    && s.observed_generation == alias.metadata.generation.unwrap_or_default()
            });
        let name = alias.name_any();
        if !ready || base.is_none() {
            for api in [true, false] {
                if api {
                    if let Some(current) = routes.get_opt(&name).await? {
                        ensure_owner(&current.metadata.owner_references, owner)?;
                        routes.delete(&name, &DeleteParams::default()).await?;
                    }
                } else if let Some(current) = policies.get_opt(&name).await? {
                    ensure_owner(&current.metadata.owner_references, owner)?;
                    policies.delete(&name, &DeleteParams::default()).await?;
                }
            }
            continue;
        }
        let Some(mut route) = base.cloned() else {
            continue;
        };
        route.metadata.name = Some(name.clone());
        route.spec.hostnames = vec![alias.spec.hostname.clone()];
        route
            .metadata
            .labels
            .get_or_insert_default()
            .insert("flash.heterocloud.io/custom-domain".into(), name.clone());
        if let Some(current) = routes.get_opt(&name).await? {
            ensure_owner(&current.metadata.owner_references, owner)?;
        }
        let mut identity = flash.clone();
        identity.metadata.name = Some(name.clone());
        let authentication = lb_auth::prepare(
            client.clone(),
            namespace,
            &identity,
            owner,
            Some(&alias.spec.hostname),
            Some(&route),
        )
        .await?;
        if authentication.ready {
            routes
                .patch(
                    &name,
                    &PatchParams::apply("heterocloud-flash-domains").force(),
                    &Patch::Apply(&route),
                )
                .await?;
        }
    }
    Ok(())
}

fn ensure_owner(refs: &Option<Vec<OwnerReference>>, owner: &OwnerReference) -> Result<()> {
    anyhow::ensure!(
        refs.as_ref()
            .is_some_and(|r| r.iter().any(|x| x.uid == owner.uid && x.kind == owner.kind)),
        "domain route owner differs"
    );
    Ok(())
}

pub async fn status_view(
    client: Client,
    namespace: &str,
    flash: &FlashService,
) -> Result<Vec<serde_json::Value>> {
    let aliases = Api::<FlashDomain>::namespaced(client.clone(), namespace)
        .list(&ListParams::default().labels(&format!(
            "flash.heterocloud.io/instance={}",
            flash.spec.service_instance_id
        )))
        .await?;
    let routes = Api::<HTTPRoute>::namespaced(client.clone(), namespace);
    let policies = Api::<SecurityPolicy>::namespaced(client, namespace);
    let mut result = vec![];
    for alias in aliases.items {
        anyhow::ensure!(owned_by(&alias, flash), "custom hostname owner differs");
        let status = alias.status.clone().unwrap_or_default();
        let routed = routes
            .get_opt(&alias.name_any())
            .await?
            .as_ref()
            .is_some_and(route_is_ready);
        let authenticated = if flash.spec.workload.exposure.authentication.is_some() {
            policies
                .get_opt(&alias.name_any())
                .await?
                .as_ref()
                .is_some_and(lb_auth::policy_is_ready)
        } else {
            true
        };
        let phase = if alias.metadata.deletion_timestamp.is_some() {
            "deleting"
        } else if status.tls_ready && routed && authenticated {
            "ready"
        } else if status.tls_ready {
            "routing"
        } else if status.phase.is_empty() {
            "pending_dns"
        } else {
            &status.phase
        };
        result.push(serde_json::json!({"hostname":alias.spec.hostname,"cname_target":alias.spec.cname_target,"phase":phase,"message":status.message,"certificate_expires_at":status.certificate_expires_at,"verification":{"type":"TXT","name":format!("_heterocloud.{}",alias.spec.hostname),"value":alias.spec.verification_value},"oidc_callback_url":format!("https://{}/_heterocloud/oidc/callback",alias.spec.hostname)}));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn names_are_stable_and_not_caddy_syntax() {
        assert_eq!(
            resource_name("a.example.org"),
            resource_name("a.example.org")
        );
        assert_ne!(
            resource_name("a.example.org"),
            resource_name("b.example.org")
        );
        for h in [
            "a.example.org\n{respond 200}",
            "*.example.org",
            "a.example.org:443",
            "127.0.0.1",
            "a.heteronetwork.internal",
            "UPPER.example.org",
        ] {
            assert!(validate_hostname(h).is_err());
        }
        assert!(validate_hostname("example.org").is_ok());
    }
}
