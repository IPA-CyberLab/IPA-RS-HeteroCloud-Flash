use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::domain::{FlashSpec, TransportProtocol};

pub const MAX_WEEKLY_GPU_SECONDS: u64 = 31_536_000;
pub const MAX_PRIVATE_GPU_ASSIGNMENTS: u64 = 256;

pub fn validated_crd() -> anyhow::Result<serde_json::Value> {
    use kube::CustomResourceExt;
    use serde_json::json;
    let mut crd = serde_json::to_value(FlashService::crd())?;
    let workload = &mut crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
        ["properties"]["workload"];
    workload["x-kubernetes-validations"] = json!([
        {"rule": "!has(self.autoscaling) || (self.replicas >= self.autoscaling.min_replicas && self.replicas <= self.autoscaling.max_replicas)", "message": "replicas must be within autoscaling bounds"},
        {"rule": "!has(self.autoscaling) || self.autoscaling.min_replicas != 0 || (has(self.exposure.endpoint_mode) && self.exposure.endpoint_mode == 'web')", "message": "min_replicas=0 requires web endpoint mode"},
        {"rule": "!has(self.gpu_type) || self.gpu_type.matches('^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$')", "message": "gpu_type must be a canonical lowercase DNS label"},
        {"rule": "(!has(self.gpu_type) && (!has(self.gpu_count) || self.gpu_count == 0)) || (self.replicas == 1 && (!has(self.autoscaling) || self.autoscaling.max_replicas == 1))", "message": "GPU Flash VMs require replicas=1 and autoscaling max_replicas=1"},
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

pub fn validated_gpu_device_crd() -> anyhow::Result<serde_json::Value> {
    use kube::CustomResourceExt;
    use serde_json::json;
    let mut crd = serde_json::to_value(FlashGpuDevice::crd())?;
    let spec = &mut crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"];
    let assignments = &mut spec["properties"]["private_assignments"];
    assignments["maxItems"] = json!(MAX_PRIVATE_GPU_ASSIGNMENTS);
    assignments["items"]["minLength"] = json!(36);
    assignments["items"]["maxLength"] = json!(36);
    assignments["items"]["pattern"] =
        json!("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");
    spec["x-kubernetes-validations"] = json!([
        {"rule": "self.gpu_type.matches('^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$')", "message": "gpu_type must be a canonical lowercase DNS label"},
        {"rule": "self.node_name.matches('^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?$')", "message": "node_name must be a lowercase Kubernetes node name"},
        {"rule": "self.physical_id.matches('^[A-Za-z0-9._:-]+$')", "message": "physical_id contains unsupported characters"}
    ]);
    Ok(crd)
}

pub fn validated_gpu_job_crd() -> anyhow::Result<serde_json::Value> {
    use kube::CustomResourceExt;
    use serde_json::json;
    let mut crd = serde_json::to_value(FlashGpuJob::crd())?;
    let spec = &mut crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"];
    spec["x-kubernetes-validations"] = json!([
        {"rule": "self.count == 1", "message": "a Flash VM may request exactly one GPU"},
        {"rule": "!has(self.gpu_type) || self.gpu_type.matches('^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$')", "message": "gpu_type must be a canonical lowercase DNS label"}
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
    /// Authenticated user that owns this service. Older resources do not have
    /// this field and may only consume open GPU inventory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_id: Option<String>,
    #[serde(default)]
    pub policy: FlashServicePolicy,
    pub workload: FlashSpec,
}

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "flash.heterocloud.io",
    version = "v1alpha1",
    kind = "FlashGpuDevice",
    plural = "flashgpudevices",
    shortname = "flashgpu",
    status = "FlashGpuDeviceStatus"
)]
#[serde(deny_unknown_fields)]
pub struct FlashGpuDeviceSpec {
    /// Kubernetes node that advertises this physical device.
    #[schemars(length(min = 1, max = 253))]
    pub node_name: String,
    /// Provider-only stable identifier, normally the NVIDIA GPU UUID.
    #[schemars(length(min = 1, max = 128))]
    pub physical_id: String,
    /// Stable, user-selectable type used by the node label and catalog.
    #[schemars(length(min = 1, max = 63))]
    pub gpu_type: String,
    /// Human-readable model name.
    #[schemars(length(min = 1, max = 256))]
    pub model: String,
    #[schemars(range(min = 1))]
    pub memory_mib: u64,
    #[serde(default)]
    pub visibility: FlashGpuVisibility,
    /// HeteroCloud UserId UUIDs allowed to see and consume a private GPU.
    #[serde(default)]
    pub private_assignments: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FlashGpuVisibility {
    #[default]
    Open,
    Private,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FlashGpuHealth {
    #[default]
    Unknown,
    Healthy,
    Unhealthy,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlashGpuDeviceStatus {
    #[serde(default)]
    pub health: FlashGpuHealth,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reservation: Option<FlashGpuReservation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_allocated_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_organization_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_subject_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlashGpuReservation {
    pub job_namespace: String,
    pub job_name: String,
    pub subject_id: String,
    pub service_instance_id: String,
    pub reserved_at: i64,
    pub lease_expires_at: i64,
}

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "flash.heterocloud.io",
    version = "v1alpha1",
    kind = "FlashGpuJob",
    plural = "flashgpujobs",
    shortname = "flashgpujob",
    namespaced,
    status = "FlashGpuJobStatus"
)]
#[serde(deny_unknown_fields)]
pub struct FlashGpuJobSpec {
    pub service_instance_id: String,
    pub service_generation: i64,
    pub subject_id: String,
    pub organization_id: String,
    pub project_id: String,
    /// None is accepted only for legacy `gpu_count=1` services and means any
    /// visible type. New requests always provide a type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_type: Option<String>,
    #[schemars(range(min = 1, max = 1))]
    pub count: u32,
    /// Remaining organization quota captured when the request is queued.
    pub quota_remaining_seconds: u64,
    pub queued_at: i64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FlashGpuJobPhase {
    #[default]
    Queued,
    Reserved,
    Running,
    Retry,
    Cancelled,
    Released,
    Rejected,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlashGpuJobStatus {
    #[serde(default)]
    pub phase: FlashGpuJobPhase,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignment: Option<FlashGpuAssignment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default)]
    pub updated_at: i64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlashGpuAssignment {
    /// Internal inventory identity. It is never returned by the user catalog.
    pub inventory_name: String,
    pub node_name: String,
    pub gpu_type: String,
    pub model: String,
    pub lease_expires_at: i64,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_scheduling: Option<FlashGpuSchedulingStatus>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlashGpuSchedulingStatus {
    pub phase: FlashGpuJobPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
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
        assert!(
            workload["x-kubernetes-validations"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("workload validations missing"))?
                .iter()
                .any(|rule| rule["message"]
                    == "GPU Flash VMs require replicas=1 and autoscaling max_replicas=1")
        );
        Ok(())
    }

    #[test]
    fn gpu_crds_match_chart_and_keep_access_fields_owner_managed() -> anyhow::Result<()> {
        let device = super::validated_gpu_device_crd()?;
        let checked_device: serde_json::Value = serde_yaml::from_str(include_str!(
            "../deploy/helm/heterocloud-flash/crds/flashgpudevices.yaml"
        ))?;
        assert_eq!(device, checked_device);
        let spec =
            &device["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"];
        assert_eq!(spec["properties"]["visibility"]["default"], "open");
        assert_eq!(
            spec["properties"]["private_assignments"]["default"],
            serde_json::json!([])
        );
        assert_eq!(
            spec["properties"]["private_assignments"]["maxItems"],
            super::MAX_PRIVATE_GPU_ASSIGNMENTS
        );
        assert_eq!(
            spec["properties"]["private_assignments"]["items"]["maxLength"],
            36
        );
        assert_eq!(
            spec["properties"]["private_assignments"]["items"]["pattern"],
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        );
        let rules = spec["x-kubernetes-validations"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("device validations missing"))?;
        assert_eq!(rules.len(), 3);
        uuid::Uuid::parse_str("01a05ad9-b529-7573-8d7b-0123456789ab")?;

        let job = super::validated_gpu_job_crd()?;
        let checked_job: serde_json::Value = serde_yaml::from_str(include_str!(
            "../deploy/helm/heterocloud-flash/crds/flashgpujobs.yaml"
        ))?;
        assert_eq!(job, checked_job);
        Ok(())
    }
}
