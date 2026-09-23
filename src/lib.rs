pub mod auth;
pub mod crd;
pub mod domain;
pub mod gpu_scheduler;
pub mod image;
pub mod reconcile;
mod web;

pub const PROVIDER_RECONCILE_ACTION: &str = "service-instance.reconcile";
pub const PROVIDER_DELETE_ACTION: &str = "service-instance.delete";
pub const PROVIDER_LIST_CONTAINERS_ACTION: &str = "flash.containers.list";
pub const PROVIDER_EXEC_ACTION: &str = "flash.exec";
pub const PROVIDER_STATUS_GET_ACTION: &str = "flash.status.get";
pub const PROVIDER_USAGE_LIST_ACTION: &str = "flash.usage.list";
pub const PROVIDER_GPU_TYPES_LIST_ACTION: &str = "flash.gpu-types.list";
pub const PROVIDER_GPU_CATALOG_LIST_ACTION: &str = "flash.gpus.catalog.list";
pub const PROVIDER_GPU_ACCESS_UPDATE_ACTION: &str = "flash.gpus.access.update";
pub const RUNTIME_CLASS_NAME: &str = "gvisor";
pub const GPU_RUNTIME_CLASS_NAME: &str = "nvidia";
pub const GPU_RESOURCE_NAME: &str = "nvidia.com/gpu";
pub const GPU_READY_LABEL: &str = "flash.heterocloud.io/gpu-ready";
pub const GPU_TYPE_LABEL: &str = "flash.heterocloud.io/gpu-type";
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
