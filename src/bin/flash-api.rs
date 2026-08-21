use std::{env, process::ExitCode, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, put},
};
use heterocloud_flash::{
    PROVIDER_DELETE_ACTION, PROVIDER_RECONCILE_ACTION, RUNTIME_CLASS_NAME,
    auth::{AuthError, ProviderAuthenticator, ProviderClaims},
    crd::{FlashService, FlashServicePhase, FlashServiceSpec},
    domain::FlashSpec,
};
use kube::{
    Api, Client,
    api::{DeleteParams, ListParams, Patch, PatchParams},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const FIELD_MANAGER: &str = "heterocloud-flash-provider";

#[derive(Clone)]
struct AppState {
    services: Api<FlashService>,
    authenticator: ProviderAuthenticator,
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
            tracing::error!(error = ?error, "flash-api stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let bind_addr = env::var("FLASH_API_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let namespace = required("FLASH_WORKLOAD_NAMESPACE")?;
    let authenticator = ProviderAuthenticator::from_public_keys_json(
        required("HETEROCLOUD_PROVIDER_ISSUER")?,
        required("HETEROCLOUD_PROVIDER_AUDIENCE")?,
        &required("HETEROCLOUD_PROVIDER_PUBLIC_KEYS_JSON")?,
    )
    .context("configure provider authentication")?;
    let client = Client::try_default()
        .await
        .context("create Kubernetes client")?;
    let state = Arc::new(AppState {
        services: Api::namespaced(client, &namespace),
        authenticator,
    });
    let app = Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route(
            "/internal/v1/service-instances/{service_instance_id}",
            put(reconcile).delete(remove),
        )
        .with_state(state);
    let listener = TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("bind Flash provider API to {bind_addr}"))?;
    info!(%bind_addr, %namespace, "HeteroCloud Flash provider API ready");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve Flash provider API")
}

async fn live() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({"status": "ok"})))
}

async fn ready(State(state): State<Arc<AppState>>) -> Result<impl IntoResponse, ApiError> {
    state.services.list(&ListParams::default().limit(1)).await?;
    Ok((StatusCode::OK, Json(json!({"status": "ready"}))))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReconcileRequest {
    generation: i64,
    name: String,
    spec: Value,
}

async fn reconcile(
    State(state): State<Arc<AppState>>,
    Path(service_instance_id): Path<Uuid>,
    headers: HeaderMap,
    Json(request): Json<ReconcileRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let claims = state
        .authenticator
        .authenticate(&headers, PROVIDER_RECONCILE_ACTION)?;
    validate_command(&claims, service_instance_id, request.generation)?;
    validate_display_name(&request.name)?;
    let spec: FlashSpec = serde_json::from_value(request.spec)
        .map_err(|_| ApiError::BadRequest("Flash spec is invalid".into()))?;
    spec.validate()
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let resource_name = resource_name(service_instance_id);

    if let Some(existing) = state.services.get_opt(&resource_name).await? {
        if existing.spec.desired_generation > request.generation {
            return Err(ApiError::Conflict("generation is stale".into()));
        }
        if existing.spec.desired_generation == request.generation
            && (existing.spec.display_name != request.name || existing.spec.workload != spec)
        {
            return Err(ApiError::Conflict(
                "generation was already used for different desired state".into(),
            ));
        }
    }

    let desired = FlashService::new(
        &resource_name,
        FlashServiceSpec {
            desired_generation: request.generation,
            display_name: request.name,
            organization_id: claims.organization_id.to_string(),
            project_id: claims.project_id.to_string(),
            service_instance_id: service_instance_id.to_string(),
            workload: spec,
        },
    );
    let resource = state
        .services
        .patch(
            &resource_name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&desired),
        )
        .await?;
    let ready = resource.status.as_ref().is_some_and(|status| {
        status.phase == FlashServicePhase::Ready
            && status.observed_generation == request.generation
            && status.runtime_class == RUNTIME_CLASS_NAME
    });
    if !ready {
        return Err(ApiError::NotReady);
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(AcceptedOperation::for_resource(
            service_instance_id,
            request.generation,
            PROVIDER_RECONCILE_ACTION,
            serde_json::to_value(resource.status).map_err(|_| ApiError::Internal)?,
        )),
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteQuery {
    generation: i64,
}

async fn remove(
    State(state): State<Arc<AppState>>,
    Path(service_instance_id): Path<Uuid>,
    Query(query): Query<DeleteQuery>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let claims = state
        .authenticator
        .authenticate(&headers, PROVIDER_DELETE_ACTION)?;
    validate_command(&claims, service_instance_id, query.generation)?;
    let resource_name = resource_name(service_instance_id);
    let Some(existing) = state.services.get_opt(&resource_name).await? else {
        return Ok((
            StatusCode::ACCEPTED,
            Json(AcceptedOperation::for_resource(
                service_instance_id,
                query.generation,
                PROVIDER_DELETE_ACTION,
                json!({
                    "phase": "deleted",
                    "observed_generation": query.generation,
                    "runtime_class": RUNTIME_CLASS_NAME,
                }),
            )),
        ));
    };
    if existing.spec.desired_generation > query.generation {
        return Err(ApiError::Conflict("generation is stale".into()));
    }
    state
        .services
        .delete(&resource_name, &DeleteParams::default())
        .await?;
    Err(ApiError::NotReady)
}

fn validate_command(
    claims: &ProviderClaims,
    service_instance_id: Uuid,
    generation: i64,
) -> Result<(), ApiError> {
    if claims.service_instance_id != service_instance_id || claims.generation != generation {
        return Err(ApiError::Forbidden);
    }
    Ok(())
}

fn validate_display_name(value: &str) -> Result<(), ApiError> {
    if value.trim() != value || value.is_empty() || value.len() > 120 {
        return Err(ApiError::BadRequest(
            "name must contain between 1 and 120 trimmed characters".into(),
        ));
    }
    Ok(())
}

fn resource_name(service_instance_id: Uuid) -> String {
    format!("flash-{service_instance_id}")
}

#[derive(Serialize)]
struct AcceptedOperation {
    operation_id: Uuid,
    status: Value,
}

impl AcceptedOperation {
    fn for_resource(
        service_instance_id: Uuid,
        generation: i64,
        action: &str,
        status: Value,
    ) -> Self {
        let operation_key = format!("{service_instance_id}:{generation}:{action}");
        Self {
            operation_id: Uuid::new_v5(&Uuid::NAMESPACE_URL, operation_key.as_bytes()),
            status,
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("provider is still reconciling the resource")]
    NotReady,
    #[error("{0}")]
    Conflict(String),
    #[error("provider command is forbidden")]
    Forbidden,
    #[error("internal provider error")]
    Internal,
    #[error(transparent)]
    Auth(#[from] AuthError),
    #[error(transparent)]
    Kubernetes(#[from] kube::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            Self::BadRequest(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
            Self::NotReady => (StatusCode::SERVICE_UNAVAILABLE, "operation_in_progress"),
            Self::Conflict(_) => (StatusCode::CONFLICT, "generation_conflict"),
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            Self::Auth(AuthError::MissingCredentials) => {
                (StatusCode::UNAUTHORIZED, "missing_credentials")
            }
            Self::Auth(_) => (StatusCode::UNAUTHORIZED, "invalid_credentials"),
            Self::Internal | Self::Kubernetes(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal_error")
            }
        };
        let mut response = (
            status,
            Json(json!({"error": {"code": code, "message": self.to_string()}})),
        )
            .into_response();
        if status == StatusCode::SERVICE_UNAVAILABLE {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, http::HeaderValue::from_static("2"));
        }
        response
    }
}

fn required(name: &'static str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
}

fn install_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _result = rustls::crypto::ring::default_provider().install_default();
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _result = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
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
