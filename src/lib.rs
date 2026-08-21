pub mod auth;
pub mod crd;
pub mod domain;
pub mod reconcile;

pub const PROVIDER_RECONCILE_ACTION: &str = "service-instance.reconcile";
pub const PROVIDER_DELETE_ACTION: &str = "service-instance.delete";
pub const RUNTIME_CLASS_NAME: &str = "gvisor";
pub const LOAD_BALANCER_CLASS: &str = "heteronetwork.io/public";
pub const TRAFFIC_MODE_ANNOTATION: &str = "networking.heteronetwork.io/traffic-mode";
