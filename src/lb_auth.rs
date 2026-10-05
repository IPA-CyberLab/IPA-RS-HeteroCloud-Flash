//! Optional authentication for one service's HTTP load balancer.
//! Credentials never enter FlashService specifications or workload Pods.
use anyhow::Result;
use k8s_openapi::{api::core::v1::Secret, apimachinery::pkg::apis::meta::v1::OwnerReference};
use kube::{
    Api, Client, CustomResource, ResourceExt,
    api::{DeleteParams, Patch, PatchParams},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    crd::FlashService,
    web::{BackendReference, GATEWAY_NAME, GATEWAY_NAMESPACE, HTTPRoute},
};

pub const SECRET_ACTION: &str = "flash.load-balancer.secret.write";
pub const CALLBACK_PATH: &str = "/_heterocloud/oidc/callback";
pub const LOGOUT_PATH: &str = "/_heterocloud/oidc/logout";
const MANAGER: &str = "heterocloud-flash-oidc";

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize)]
#[kube(
    group = "gateway.envoyproxy.io",
    version = "v1alpha1",
    kind = "SecurityPolicy",
    plural = "securitypolicies",
    namespaced,
    schema = "disabled",
    status = "Value"
)]
#[serde(rename_all = "camelCase")]
pub struct SecurityPolicySpec {
    #[serde(default)]
    pub target_refs: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oidc: Option<Value>,
}

/// Secret names are derived only from the signed service ID and a validated reference.
pub fn secret_name(service_id: Uuid, reference: &str) -> String {
    format!(
        "flash-{}-oidc-{}",
        service_id.simple(),
        &Uuid::new_v5(&service_id, reference.as_bytes())
            .simple()
            .to_string()[..12]
    )
}

pub(crate) fn policy_is_ready(policy: &SecurityPolicy) -> bool {
    let Some(generation) = policy.metadata.generation else {
        return false;
    };
    policy.metadata.deletion_timestamp.is_none()
        && policy
            .status
            .as_ref()
            .and_then(|status| status.get("ancestors"))
            .and_then(Value::as_array)
            .is_some_and(|ancestors| {
                ancestors.iter().any(|ancestor| {
                    let reference = &ancestor["ancestorRef"];
                    ancestor["controllerName"] == "gateway.envoyproxy.io/gatewayclass-controller"
                        && reference["name"] == GATEWAY_NAME
                        && reference["namespace"] == GATEWAY_NAMESPACE
                        && reference.get("kind").is_none_or(|kind| kind == "Gateway")
                        && reference
                            .get("group")
                            .is_none_or(|group| group == "gateway.networking.k8s.io")
                        && ancestor["conditions"].as_array().is_some_and(|conditions| {
                            conditions.iter().any(|condition| {
                                condition["type"] == "Accepted"
                                    && condition["status"] == "True"
                                    && condition["observedGeneration"].as_i64() == Some(generation)
                            })
                        })
                })
            })
}

pub(crate) fn blocked_route(mut route: HTTPRoute) -> HTTPRoute {
    let pending_name = format!("{}-oidc-pending", route.name_any());
    for rule in &mut route.spec.rules {
        // Gateway API requires an invalid backend reference to return HTTP 500.
        rule.backend_refs = vec![BackendReference {
            group: String::new(),
            kind: "Service".into(),
            name: pending_name.clone(),
            namespace: None,
            port: Some(80),
        }];
    }
    route
}

/// Block the public route before changing credentials, without changing any Pod.
pub async fn block_existing_route(client: Client, namespace: &str, name: &str) -> Result<()> {
    let routes = Api::<HTTPRoute>::namespaced(client, namespace);
    if let Some(route) = routes.get_opt(name).await? {
        let mut blocked = blocked_route(route);
        blocked.metadata.managed_fields = None;
        blocked.metadata.resource_version = None;
        blocked.status = None;
        routes
            .patch(
                name,
                &PatchParams::apply("heterocloud-flash-controller").force(),
                &Patch::Apply(&blocked),
            )
            .await?;
    }
    Ok(())
}

pub(crate) struct PreparedAuthentication {
    pub ready: bool,
    pub message: Option<&'static str>,
}

pub(crate) async fn prepare(
    client: Client,
    namespace: &str,
    flash: &FlashService,
    owner: &OwnerReference,
    hostname: Option<&str>,
    route: Option<&HTTPRoute>,
) -> Result<PreparedAuthentication> {
    let policies = Api::<SecurityPolicy>::namespaced(client.clone(), namespace);
    let name = flash.name_any();
    let Some(authentication) = &flash.spec.workload.exposure.authentication else {
        if let Some(policy) = policies.get_opt(&name).await? {
            ensure_owner(&policy.metadata.owner_references, owner)?;
            match policies.delete(&name, &DeleteParams::default()).await {
                Ok(_) => {}
                Err(kube::Error::Api(error)) if error.code == 404 => {}
                Err(error) => return Err(error.into()),
            }
        }
        return Ok(PreparedAuthentication {
            ready: true,
            message: None,
        });
    };
    let route = route.ok_or_else(|| anyhow::anyhow!("OIDC requires an HTTP route"))?;
    let hostname =
        hostname.ok_or_else(|| anyhow::anyhow!("OIDC requires provider publicDomain"))?;
    let service_id = Uuid::parse_str(&flash.spec.service_instance_id)?;
    let credential_name = secret_name(service_id, &authentication.client_secret_ref);
    let secrets = Api::<Secret>::namespaced(client.clone(), namespace);
    let mut secret = secrets.get_opt(&credential_name).await?;
    if let Some(existing) = &secret {
        if existing
            .metadata
            .owner_references
            .as_ref()
            .is_none_or(Vec::is_empty)
        {
            let labels = existing.metadata.labels.as_ref();
            anyhow::ensure!(
                labels.and_then(|l| l.get("flash.heterocloud.io/instance"))
                    == Some(&flash.spec.service_instance_id)
                    && labels.and_then(|l| l.get("flash.heterocloud.io/organization"))
                        == Some(&flash.spec.organization_id)
                    && labels.and_then(|l| l.get("flash.heterocloud.io/project"))
                        == Some(&flash.spec.project_id),
                "OIDC credential scope does not match the service"
            );
            secret = Some(secrets.patch(&credential_name, &PatchParams::default(), &Patch::Merge(json!({"metadata":{"resourceVersion":existing.metadata.resource_version,"ownerReferences":[owner]}}))).await?);
        } else {
            ensure_owner(&existing.metadata.owner_references, owner)?;
        }
    }
    let revision = format!(
        "{}:{}:{}",
        serde_json::to_string(authentication)?,
        secret
            .as_ref()
            .and_then(|s| s.metadata.uid.as_deref())
            .unwrap_or("missing"),
        secret
            .as_ref()
            .and_then(|s| s.metadata.resource_version.as_deref())
            .unwrap_or("missing")
    );
    let suffix = Uuid::new_v5(&service_id, revision.as_bytes())
        .simple()
        .to_string();
    let mut desired = SecurityPolicy::new(
        &name,
        SecurityPolicySpec {
            target_refs: vec![
                json!({"group":"gateway.networking.k8s.io", "kind":"HTTPRoute", "name":name}),
            ],
            oidc: Some(json!({
                "provider": {"issuer": authentication.issuer_url},
                "clientID": authentication.client_id,
                "clientSecret": {"name": credential_name},
                "scopes": authentication.scopes,
                "redirectURL": format!("https://{hostname}{CALLBACK_PATH}"),
                "logoutPath": LOGOUT_PATH,
                "cookieConfig": {"sameSite":"Lax"},
                "cookieNames": {"accessToken":format!("HcAccessToken-{suffix}"), "idToken":format!("HcIdToken-{suffix}")},
                "forwardAccessToken":false, "passThroughAuthHeader":false,
                "disableTokenEncryption":false, "refreshToken":true
            })),
        },
    );
    desired.metadata.owner_references = Some(vec![owner.clone()]);
    let current = policies.get_opt(&name).await?;
    if let Some(current) = &current {
        ensure_owner(&current.metadata.owner_references, owner)?;
    }
    let current_ready = current.as_ref().is_some_and(|policy| {
        policy.spec.target_refs == desired.spec.target_refs
            && policy.spec.oidc == desired.spec.oidc
            && policy_is_ready(policy)
    });
    if !current_ready {
        let blocked = blocked_route(route.clone());
        Api::<HTTPRoute>::namespaced(client.clone(), namespace)
            .patch(
                &name,
                &PatchParams::apply("heterocloud-flash-controller").force(),
                &Patch::Apply(&blocked),
            )
            .await?;
    }
    let applied = policies
        .patch(
            &name,
            &PatchParams::apply(MANAGER).force(),
            &Patch::Apply(&desired),
        )
        .await?;
    let secret_valid = secret.as_ref().is_some_and(|s| {
        s.data
            .as_ref()
            .and_then(|data| data.get("client-secret"))
            .is_some_and(|value| !value.0.is_empty())
    });
    let ready = secret_valid && policy_is_ready(&applied);
    Ok(PreparedAuthentication {
        ready,
        message: (!ready).then_some(if secret_valid {
            "waiting for load balancer OIDC policy acceptance; unauthenticated access is blocked"
        } else {
            "write the referenced load balancer client secret; unauthenticated access is blocked"
        }),
    })
}

fn ensure_owner(references: &Option<Vec<OwnerReference>>, owner: &OwnerReference) -> Result<()> {
    anyhow::ensure!(
        references.as_ref().is_some_and(|refs| refs
            .iter()
            .any(|r| r.uid == owner.uid && r.controller == Some(true))),
        "OIDC resource ownership does not match the Flash service"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn readiness_requires_current_generation_and_expected_gateway() -> Result<()> {
        let mut policy: SecurityPolicy = serde_json::from_value(
            json!({"apiVersion":"gateway.envoyproxy.io/v1alpha1", "kind":"SecurityPolicy", "metadata":{"name":"test","generation":2}, "spec":{}, "status":{"ancestors":[{"ancestorRef":{"name":GATEWAY_NAME,"namespace":GATEWAY_NAMESPACE},"controllerName":"gateway.envoyproxy.io/gatewayclass-controller","conditions":[{"type":"Accepted","status":"True","observedGeneration":2}]}]}}),
        )?;
        assert!(policy_is_ready(&policy));
        policy.metadata.generation = Some(3);
        assert!(!policy_is_ready(&policy));
        policy.metadata.generation = Some(2);
        if let Some(status) = &mut policy.status {
            status["ancestors"][0]["ancestorRef"]["namespace"] = json!("other");
        }
        assert!(!policy_is_ready(&policy));
        Ok(())
    }
    #[test]
    fn credential_names_are_scoped_to_service_and_reference() {
        let id = Uuid::from_u128(1);
        let name = secret_name(id, "credential");
        assert!(name.len() <= 63);
        assert_ne!(name, secret_name(id, "other"));
        assert_ne!(name, secret_name(Uuid::from_u128(2), "credential"));
    }
}
