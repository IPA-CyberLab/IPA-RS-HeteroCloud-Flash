use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::domain::{FlashSpec, TransportProtocol};

pub const MAX_WEEKLY_GPU_SECONDS: u64 = 31_536_000;

pub fn validated_crd() -> anyhow::Result<serde_json::Value> {
    use kube::CustomResourceExt;
    use serde_json::json;
    let mut crd = serde_json::to_value(FlashService::crd())?;
    let workload = &mut crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
        ["properties"]["workload"];
    workload["x-kubernetes-validations"] = json!([
        {"rule": "!has(self.autoscaling) || (self.replicas >= self.autoscaling.min_replicas && self.replicas <= self.autoscaling.max_replicas)", "message": "replicas must be within autoscaling bounds"},
        {"rule": "!has(self.autoscaling) || self.autoscaling.min_replicas != 0 || (has(self.exposure.endpoint_mode) && self.exposure.endpoint_mode == 'web')", "message": "min_replicas=0 requires web endpoint mode"},
        {"rule": "!has(self.exposure.endpoint_mode) || self.exposure.endpoint_mode != 'web' || (size(self.ports) == 1 && self.ports.all(p, p.protocol == 'tcp'))", "message": "web requires exactly one TCP port"}
    ]);
    workload["properties"]["autoscaling"]["x-kubernetes-validations"] = json!([
        {"rule": "self.max_replicas >= self.min_replicas", "message": "max_replicas must be at least min_replicas"},
        {"rule": "has(self.target_cpu_utilization_percent) || has(self.target_memory_utilization_percent)", "message": "at least one CPU or memory target is required"}
    ]);
    workload["properties"]["exposure"]["x-kubernetes-validations"] = json!([
        {"rule": "!has(self.endpoint_mode) || !(self.endpoint_mode in ['load_balancer', 'web']) || (self.type == 'public' && self.traffic_mode == 'forwarded')", "message": "load_balancer and web require public forwarded exposure"},
        {"rule": "!has(self.endpoint_mode) || self.endpoint_mode != 'web' || ((!has(self.allowed_source_cidrs) || size(self.allowed_source_cidrs) == 0) && (!has(self.denied_source_cidrs) || size(self.denied_source_cidrs) == 0))", "message": "web does not yet support source CIDR filters"}
    ]);
    Ok(crd)
}

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
    #[serde(default)]
    pub policy: FlashServicePolicy,
    pub workload: FlashSpec,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlashServicePolicy {
    #[schemars(range(min = 0, max = 31_536_000))]
    pub max_weekly_gpu_seconds: u64,
}

impl Default for FlashServicePolicy {
    fn default() -> Self {
        Self {
            max_weekly_gpu_seconds: MAX_WEEKLY_GPU_SECONDS,
        }
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writable_storage_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_weekly_usage: Option<FlashGpuWeeklyUsage>,
    #[serde(default)]
    pub gpu_quota_exhausted: bool,
    #[serde(default)]
    pub cold: bool,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlashGpuWeeklyUsage {
    pub week_started_at: i64,
    pub used_seconds: u64,
    pub last_metered_at: i64,
    pub limit_seconds: u64,
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

#[cfg(test)]
mod tests {
    #[test]
    fn generated_crd_matches_chart_and_preserves_legacy_defaults() -> anyhow::Result<()> {
        let generated = super::validated_crd()?;
        let checked_in: serde_json::Value = serde_yaml::from_str(include_str!(
            "../deploy/helm/heterocloud-flash/crds/flashservices.yaml"
        ))?;
        assert_eq!(generated, checked_in);
        let workload = &generated["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["workload"];
        assert_eq!(
            workload["properties"]["exposure"]["properties"]["endpoint_mode"]["default"],
            "ip"
        );
        assert!(
            !workload["required"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("missing required fields"))?
                .contains(&serde_json::json!("autoscaling"))
        );
        Ok(())
    }
}
