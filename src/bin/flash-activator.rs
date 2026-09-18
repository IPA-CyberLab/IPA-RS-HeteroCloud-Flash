use std::{env, process::ExitCode, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, HeaderName, StatusCode, header},
    response::{IntoResponse, Response},
};
use heterocloud_flash::{
    crd::{FlashService, FlashServicePhase},
    domain::EndpointMode,
    reconcile::LAST_ACTIVITY_ANNOTATION,
};
use kube::{
    Api, Client, ResourceExt,
    api::{Patch, PatchParams},
};
use serde_json::json;
use tokio::{net::TcpListener, signal, time};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const FIELD_MANAGER: &str = "heterocloud-flash-activator";
const DEFAULT_COLD_START_TIMEOUT_SECONDS: u64 = 120;

#[derive(Clone)]
struct AppState {
    services: Api<FlashService>,
    deployments: Api<k8s_openapi::api::apps::v1::Deployment>,
    http: reqwest::Client,
    workload_namespace: String,
    public_domain: String,
    cold_start_timeout: Duration,
}

#[tokio::main]
async fn main() -> ExitCode {
    install_crypto_provider();
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = ?error, "flash-activator stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let bind_addr = env::var("FLASH_ACTIVATOR_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8081".into());
    let workload_namespace = required("FLASH_WORKLOAD_NAMESPACE")?;
    let public_domain = required("FLASH_PUBLIC_DOMAIN")?;
    let cold_start_timeout = env::var("FLASH_COLD_START_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_COLD_START_TIMEOUT_SECONDS)
        .clamp(10, 600);
    let client = Client::try_default()
        .await
        .context("create Kubernetes client")?;
    let state = Arc::new(AppState {
        services: Api::namespaced(client.clone(), &workload_namespace),
        deployments: Api::namespaced(client, &workload_namespace),
        http: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .build()
            .context("create upstream HTTP client")?,
        workload_namespace,
        public_domain,
        cold_start_timeout: Duration::from_secs(cold_start_timeout),
    });
    let app = Router::new().fallback(proxy).with_state(state);
    let listener = TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("bind Flash activator to {bind_addr}"))?;
    tracing::info!(%bind_addr, "Flash scale-to-zero activator ready");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve Flash activator")
}

async fn proxy(State(state): State<Arc<AppState>>, request: Request) -> Response {
    match proxy_inner(&state, request).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn proxy_inner(state: &AppState, request: Request) -> Result<Response, ActivatorError> {
    let hostname = request_hostname(request.headers())?;
    let service_id = service_id_from_hostname(&hostname, &state.public_domain)?;
    let resource_name = format!("flash-{service_id}");
    let service = state
        .services
        .get_opt(&resource_name)
        .await?
        .ok_or(ActivatorError::NotFound)?;
    let workload = &service.spec.workload;
    if service.spec.service_instance_id != service_id.to_string()
        || workload.exposure.endpoint_mode != EndpointMode::Web
        || workload
            .autoscaling
            .as_ref()
            .is_none_or(|scaling| scaling.min_replicas != 0)
    {
        return Err(ActivatorError::NotFound);
    }
    if service
        .status
        .as_ref()
        .is_some_and(|status| status.gpu_quota_exhausted)
    {
        return Err(ActivatorError::QuotaExceeded);
    }
    let port = workload
        .ports
        .first()
        .map(|port| port.service_port)
        .ok_or(ActivatorError::NotFound)?;
    mark_activity(&state.services, &service).await?;
    // GPU cold starts must pass through the durable reservation queue before
    // a Pod is allowed to consume device-plugin capacity. CPU services can be
    // woken directly.
    if workload.effective_gpu_count() == 0 {
        wake(&state.deployments, &resource_name).await?;
    }
    wait_until_ready(state, &resource_name, service.spec.desired_generation).await?;

    let (parts, body) = request.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or("/", |value| value.as_str());
    let upstream = format!(
        "http://{resource_name}.{}.svc.cluster.local:{port}{path_and_query}",
        state.workload_namespace
    );
    let mut builder = state.http.request(parts.method, upstream);
    for (name, value) in &parts.headers {
        if !is_hop_by_hop(name) && *name != header::HOST {
            builder = builder.header(name, value);
        }
    }
    let upstream = builder
        .body(reqwest::Body::wrap_stream(body.into_data_stream()))
        .send()
        .await
        .map_err(ActivatorError::Upstream)?;
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let mut response = Response::builder().status(status);
    for (name, value) in &headers {
        if !is_hop_by_hop(name) {
            response = response.header(name, value);
        }
    }
    response
        .body(Body::from_stream(upstream.bytes_stream()))
        .map_err(|_| ActivatorError::Internal)
}

fn request_hostname(headers: &HeaderMap) -> Result<String, ActivatorError> {
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .ok_or(ActivatorError::NotFound)?;
    let host = host
        .split_once(':')
        .map_or(host, |(hostname, _port)| hostname)
        .trim_end_matches('.')
        .to_ascii_lowercase();
    if host.is_empty() {
        return Err(ActivatorError::NotFound);
    }
    Ok(host)
}

fn service_id_from_hostname(hostname: &str, public_domain: &str) -> Result<Uuid, ActivatorError> {
    let suffix = format!(".{public_domain}");
    let label = hostname
        .strip_suffix(&suffix)
        .and_then(|value| value.strip_prefix("f-"))
        .ok_or(ActivatorError::NotFound)?;
    Uuid::parse_str(label).map_err(|_| ActivatorError::NotFound)
}

async fn mark_activity(
    services: &Api<FlashService>,
    service: &FlashService,
) -> Result<(), kube::Error> {
    services
        .patch(
            &service.name_any(),
            &PatchParams::apply(FIELD_MANAGER),
            &Patch::Merge(json!({
                "metadata": {"annotations": {
                    (LAST_ACTIVITY_ANNOTATION): chrono::Utc::now().timestamp().to_string()
                }}
            })),
        )
        .await?;
    Ok(())
}

async fn wake(
    deployments: &Api<k8s_openapi::api::apps::v1::Deployment>,
    name: &str,
) -> Result<(), kube::Error> {
    deployments
        .patch(
            name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(json!({
                "apiVersion": "apps/v1", "kind": "Deployment",
                "metadata": {"name": name}, "spec": {"replicas": 1}
            })),
        )
        .await?;
    Ok(())
}

async fn wait_until_ready(
    state: &AppState,
    name: &str,
    generation: i64,
) -> Result<(), ActivatorError> {
    let deadline = time::Instant::now() + state.cold_start_timeout;
    loop {
        let service = state
            .services
            .get(name)
            .await
            .map_err(ActivatorError::Kubernetes)?;
        if service
            .status
            .as_ref()
            .is_some_and(|status| status.gpu_quota_exhausted)
        {
            return Err(ActivatorError::QuotaExceeded);
        }
        let deployment = state
            .deployments
            .get(name)
            .await
            .map_err(ActivatorError::Kubernetes)?;
        let deployment_ready = deployment
            .status
            .as_ref()
            .and_then(|status| status.available_replicas)
            .unwrap_or_default()
            >= 1;
        let generation_ready = service.status.as_ref().is_some_and(|status| {
            status.observed_generation == generation
                && matches!(
                    status.phase,
                    FlashServicePhase::Ready | FlashServicePhase::Provisioning
                )
        });
        if deployment_ready && generation_ready {
            // Give EndpointSlice propagation a short bounded head start.
            time::sleep(Duration::from_millis(100)).await;
            return Ok(());
        }
        if time::Instant::now() >= deadline {
            return Err(ActivatorError::ColdStartTimeout);
        }
        time::sleep(Duration::from_millis(250)).await;
    }
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str().to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

#[derive(Debug, thiserror::Error)]
enum ActivatorError {
    #[error("service not found")]
    NotFound,
    #[error("weekly GPU runtime limit reached")]
    QuotaExceeded,
    #[error("cold start timed out")]
    ColdStartTimeout,
    #[error("Kubernetes request failed")]
    Kubernetes(#[from] kube::Error),
    #[error("upstream request failed")]
    Upstream(reqwest::Error),
    #[error("internal response error")]
    Internal,
}

impl IntoResponse for ActivatorError {
    fn into_response(self) -> Response {
        let (status, code, message, retry_after) = match self {
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "service not found",
                None,
            ),
            Self::QuotaExceeded => (
                StatusCode::TOO_MANY_REQUESTS,
                "weekly_gpu_limit_reached",
                "weekly GPU runtime limit reached",
                Some("60"),
            ),
            Self::ColdStartTimeout => (
                StatusCode::GATEWAY_TIMEOUT,
                "cold_start_timeout",
                "service did not become ready before the cold-start deadline",
                Some("5"),
            ),
            Self::Kubernetes(_) | Self::Upstream(_) | Self::Internal => (
                StatusCode::BAD_GATEWAY,
                "activation_failed",
                "service activation failed",
                Some("2"),
            ),
        };
        let mut response = (
            status,
            Json(json!({"error": {"code": code, "message": message}})),
        )
            .into_response();
        if let Some(value) = retry_after {
            response.headers_mut().insert(
                header::RETRY_AFTER,
                value
                    .parse()
                    .unwrap_or_else(|_| http::HeaderValue::from_static("5")),
            );
        }
        response
    }
}

fn required(name: &str) -> Result<String> {
    env::var(name)
        .with_context(|| format!("{name} is required"))
        .and_then(|value| {
            let value = value.trim().to_owned();
            if value.is_empty() {
                anyhow::bail!("{name} must not be empty");
            }
            Ok(value)
        })
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

fn install_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
}

#[cfg(test)]
mod tests {
    use super::service_id_from_hostname;
    use uuid::Uuid;

    #[test]
    fn host_mapping_is_exact() {
        let id = Uuid::from_u128(7);
        let host = format!("f-{id}.flash.example.test");
        assert_eq!(
            service_id_from_hostname(&host, "flash.example.test").ok(),
            Some(id)
        );
        assert!(
            service_id_from_hostname(&format!("x-{id}.flash.example.test"), "flash.example.test")
                .is_err()
        );
        assert!(
            service_id_from_hostname(&format!("f-{id}.attacker.test"), "flash.example.test")
                .is_err()
        );
    }
}
