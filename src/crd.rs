use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::domain::{FlashSpec, TransportProtocol};

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "flash.heterocloud.io",
    version = "v1alpha1",
    kind = "FlashService",
    plural = "flashservices",
    shortname = "flash",
    namespaced,
    status = "FlashServiceStatus"
)]
#[serde(deny_unknown_fields)]
pub struct FlashServiceSpec {
    pub desired_generation: i64,
    pub display_name: String,
    pub organization_id: String,
    pub project_id: String,
    pub service_instance_id: String,
    pub workload: FlashSpec,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlashServiceStatus {
    pub phase: FlashServicePhase,
    pub observed_generation: i64,
    pub ready_replicas: i32,
    pub desired_replicas: i32,
    pub runtime_class: String,
    #[serde(default)]
    pub endpoints: Vec<FlashEndpoint>,
    pub message: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FlashServicePhase {
    #[default]
    Provisioning,
    Ready,
    Error,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlashEndpoint {
    pub name: String,
    pub protocol: TransportProtocol,
    pub host: String,
    pub port: u16,
}
