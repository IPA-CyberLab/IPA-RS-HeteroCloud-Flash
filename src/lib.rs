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
pub const LOAD_BALANCER_CLASS: &str = "heteronetwork.io/public";
pub const TRAFFIC_MODE_ANNOTATION: &str = "networking.heteronetwork.io/traffic-mode";
