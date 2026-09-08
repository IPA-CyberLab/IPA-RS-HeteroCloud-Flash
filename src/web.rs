use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use serde::{Deserialize, Serialize};

pub const GATEWAY_NAME: &str = "heterocloud-edge";
pub const GATEWAY_NAMESPACE: &str = "heterocloud-edge";
pub const GATEWAY_SECTION: &str = "http";
pub const PROXY_NAMESPACE: &str = "envoy-gateway-system";
const GATEWAY_CONTROLLER: &str = "gateway.envoyproxy.io/gatewayclass-controller";

// Client-side types for the externally installed Gateway API, not a provider-owned CRD.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1",
    kind = "HTTPRoute",
    plural = "httproutes",
    namespaced,
    schema = "disabled",
    status = "HTTPRouteStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct HTTPRouteSpec {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parent_refs: Vec<ParentReference>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hostnames: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<HTTPRouteRule>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParentReference {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HTTPRouteRule {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backend_refs: Vec<BackendReference>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filters: Vec<serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackendReference {
    #[serde(default)]
    pub group: String,
    #[serde(default = "service_kind")]
    pub kind: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

fn service_kind() -> String {
    "Service".into()
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HTTPRouteStatus {
    #[serde(default)]
    pub parents: Vec<RouteParentStatus>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteParentStatus {
    pub parent_ref: ParentReference,
    pub controller_name: String,
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

pub fn route_is_ready(route: &HTTPRoute) -> bool {
    let Some(generation) = route.metadata.generation else {
        return false;
    };
    route.metadata.deletion_timestamp.is_none()
        && route.status.as_ref().is_some_and(|status| {
            status.parents.iter().any(|parent| {
                let reference = &parent.parent_ref;
                parent.controller_name == GATEWAY_CONTROLLER
                    && reference.name == GATEWAY_NAME
                    && reference.namespace.as_deref() == Some(GATEWAY_NAMESPACE)
                    && reference.section_name.as_deref() == Some(GATEWAY_SECTION)
                    && reference
                        .group
                        .as_deref()
                        .unwrap_or("gateway.networking.k8s.io")
                        == "gateway.networking.k8s.io"
                    && reference.kind.as_deref().unwrap_or("Gateway") == "Gateway"
                    && ["Accepted", "ResolvedRefs"].iter().all(|kind| {
                        parent.conditions.iter().any(|condition| {
                            condition.type_ == *kind
                                && condition.status == "True"
                                && condition.observed_generation == Some(generation)
                        })
                    })
            })
        })
}

#[cfg(test)]
mod tests {
    #[test]
    fn watcher_accepts_unowned_routes_with_optional_fields_absent() -> anyhow::Result<()> {
        for spec in [
            serde_json::json!({}),
            serde_json::json!({"rules":[{"filters":[{"type":"RequestRedirect", "requestRedirect":{"scheme":"https"}}]}]}),
            serde_json::json!({"rules":[{"backendRefs":[{"name":"other", "kind":"CustomBackend"}]}]}),
        ] {
            let route: super::HTTPRoute = serde_json::from_value(serde_json::json!({
                "apiVersion":"gateway.networking.k8s.io/v1", "kind":"HTTPRoute",
                "metadata":{"name":"unrelated"}, "spec":spec
            }))?;
            assert!(!super::route_is_ready(&route));
        }
        Ok(())
    }
}
