pub mod auth;
pub mod crd;
pub mod domain;
pub mod image;
pub mod reconcile;
mod web;

pub const PROVIDER_RECONCILE_ACTION: &str = "service-instance.reconcile";
pub const PROVIDER_DELETE_ACTION: &str = "service-instance.delete";
pub const PROVIDER_LIST_CONTAINERS_ACTION: &str = "flash.containers.list";
pub const PROVIDER_EXEC_ACTION: &str = "flash.exec";
pub const PROVIDER_STATUS_GET_ACTION: &str = "flash.status.get";
pub const RUNTIME_CLASS_NAME: &str = "gvisor";
pub const GPU_RUNTIME_CLASS_NAME: &str = "nvidia";
pub const GPU_RESOURCE_NAME: &str = "nvidia.com/gpu";
pub const GPU_READY_LABEL: &str = "flash.heterocloud.io/gpu-ready";
pub const LOAD_BALANCER_CLASS: &str = "heteronetwork.io/public";
pub const TRAFFIC_MODE_ANNOTATION: &str = "networking.heteronetwork.io/traffic-mode";

#[must_use]
pub const fn workload_runtime_class(gpu_count: u32) -> &'static str {
    if gpu_count == 0 {
        RUNTIME_CLASS_NAME
    } else {
        GPU_RUNTIME_CLASS_NAME
    }
}
