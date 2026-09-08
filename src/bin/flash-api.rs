use std::{env, process::ExitCode, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, put},
};
use futures_util::{SinkExt, StreamExt};
use heterocloud_flash::{
    PROVIDER_DELETE_ACTION, PROVIDER_EXEC_ACTION, PROVIDER_LIST_CONTAINERS_ACTION,
    PROVIDER_RECONCILE_ACTION, PROVIDER_STATUS_GET_ACTION, RUNTIME_CLASS_NAME,
    auth::{AuthError, ProviderAuthenticator, ProviderClaims},
    crd::{FlashService, FlashServicePhase, FlashServiceSpec, FlashServiceStatus},
    domain::FlashSpec,
};
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api, Client,
    api::{AttachParams, DeleteParams, ListParams, Patch, PatchParams, TerminalSize},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
    time,
};
use tracing::info;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const FIELD_MANAGER: &str = "heterocloud-flash-provider";
const MAX_EXEC_SESSION_SECONDS: u64 = 1_800;
const MAX_EXEC_MESSAGE_BYTES: usize = 64 * 1024;
const EXEC_SHUTDOWN_GRACE_SECONDS: u64 = 2;
const EXEC_SHELL_SCRIPT: &str = "export TERM=xterm-256color COLORTERM=truecolor LANG=C.UTF-8 LC_ALL=C.UTF-8 HOME=/root NPM_CONFIG_PREFIX=/root/.local PATH=/root/.local/bin:$PATH; mkdir -p /root/.local/bin; cd /root || cd /; exec /bin/sh";

#[derive(Clone)]
struct AppState {
    services: Api<FlashService>,
    pods: Api<Pod>,
    authenticator: ProviderAuthenticator,
    exec_sessions: Arc<Semaphore>,
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
    let max_exec_sessions = env::var("FLASH_MAX_EXEC_SESSIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(32)
        .clamp(1, 256);
    let state = Arc::new(AppState {
        services: Api::namespaced(client.clone(), &namespace),
        pods: Api::namespaced(client, &namespace),
        authenticator,
        exec_sessions: Arc::new(Semaphore::new(max_exec_sessions)),
    });
    let app = app_router(state);
    let listener = TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("bind Flash provider API to {bind_addr}"))?;
    info!(%bind_addr, %namespace, "HeteroCloud Flash provider API ready");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve Flash provider API")
}

fn app_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route(
            "/internal/v1/service-instances/{service_instance_id}",
            put(reconcile).delete(remove).get(get_status),
        )
        .route(
            "/internal/v1/service-instances/{service_instance_id}/containers",
            get(list_containers),
        )
        .route(
            "/internal/v1/service-instances/{service_instance_id}/exec",
            get(exec),
        )
        .with_state(state)
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
    let current_status = resource
        .status
        .as_ref()
        .filter(|status| status.observed_generation == request.generation);
    let ready = current_status.is_some_and(|status| {
        status.phase == FlashServicePhase::Ready && status.runtime_class == RUNTIME_CLASS_NAME
    });
    if current_status.is_some_and(|status| status.phase == FlashServicePhase::Error) {
        return Ok((
            StatusCode::ACCEPTED,
            Json(AcceptedOperation::for_resource(
                service_instance_id,
                request.generation,
                PROVIDER_RECONCILE_ACTION,
                serde_json::to_value(resource.status).map_err(|_| ApiError::Internal)?,
            )),
        ));
    }
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GenerationQuery {
    generation: i64,
}

async fn get_status(
    State(state): State<Arc<AppState>>,
    Path(service_instance_id): Path<Uuid>,
    Query(query): Query<GenerationQuery>,
    headers: HeaderMap,
) -> Result<Json<FlashServiceStatus>, ApiError> {
    let claims = state
        .authenticator
        .authenticate(&headers, PROVIDER_STATUS_GET_ACTION)?;
    validate_command(&claims, service_instance_id, query.generation)?;
    let resource = state
        .services
        .get_opt(&resource_name(service_instance_id))
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(
        status_for_access(&resource, &claims, service_instance_id, query.generation)?.clone(),
    ))
}

fn status_for_access<'a>(
    resource: &'a FlashService,
    claims: &ProviderClaims,
    service_instance_id: Uuid,
    generation: i64,
) -> Result<&'a FlashServiceStatus, ApiError> {
    validate_resource_identity(resource, claims, service_instance_id)?;
    if resource.spec.desired_generation != generation {
        return Err(ApiError::Conflict(
            "generation does not match current desired state".into(),
        ));
    }
    resource
        .status
        .as_ref()
        .filter(|status| status.observed_generation == generation)
        .ok_or(ApiError::NotReady)
}

#[derive(Clone, Debug, Serialize)]
struct ContainerSummary {
    name: String,
    phase: String,
    ready: bool,
}

async fn list_containers(
    State(state): State<Arc<AppState>>,
    Path(service_instance_id): Path<Uuid>,
    Query(query): Query<GenerationQuery>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let claims = state
        .authenticator
        .authenticate(&headers, PROVIDER_LIST_CONTAINERS_ACTION)?;
    validate_command(&claims, service_instance_id, query.generation)?;
    validate_resource_scope(&state, &claims, service_instance_id, query.generation).await?;
    let mut containers = state
        .pods
        .list(&service_pod_selector(service_instance_id))
        .await?
        .items
        .into_iter()
        .filter_map(container_summary)
        .collect::<Vec<_>>();
    containers.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(Json(json!({ "items": containers })))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecQuery {
    generation: i64,
    pod: String,
}

async fn exec(
    State(state): State<Arc<AppState>>,
    Path(service_instance_id): Path<Uuid>,
    Query(query): Query<ExecQuery>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Result<impl IntoResponse, ApiError> {
    let claims = state
        .authenticator
        .authenticate(&headers, PROVIDER_EXEC_ACTION)?;
    validate_command(&claims, service_instance_id, query.generation)?;
    validate_resource_scope(&state, &claims, service_instance_id, query.generation).await?;
    let pod = state.pods.get(&query.pod).await?;
    if !pod_belongs_to_service(&pod, service_instance_id) || !pod_can_exec(&pod) {
        return Err(ApiError::Forbidden);
    }
    let permit = Arc::clone(&state.exec_sessions)
        .try_acquire_owned()
        .map_err(|_| ApiError::TooManyExecSessions)?;
    let pods = state.pods.clone();
    let pod_name = query.pod;
    Ok(upgrade
        .max_message_size(MAX_EXEC_MESSAGE_BYTES)
        .on_upgrade(move |socket| exec_shell(socket, pods, pod_name, permit)))
}

async fn validate_resource_scope(
    state: &AppState,
    claims: &ProviderClaims,
    service_instance_id: Uuid,
    generation: i64,
) -> Result<(), ApiError> {
    let resource = state
        .services
        .get(&resource_name(service_instance_id))
        .await?;
    validate_resource_access(&resource, claims, service_instance_id, generation)
}

fn validate_resource_access(
    resource: &FlashService,
    claims: &ProviderClaims,
    service_instance_id: Uuid,
    generation: i64,
) -> Result<(), ApiError> {
    validate_resource_identity(resource, claims, service_instance_id)?;
    if resource.spec.desired_generation != generation {
        return Err(ApiError::Forbidden);
    }
    // Readiness is operational state, not authorization for listing or diagnostics.
    let status = resource.status.as_ref().ok_or(ApiError::NotReady)?;
    if status.observed_generation != generation {
        return Err(ApiError::Forbidden);
    }
    Ok(())
}

fn validate_resource_identity(
    resource: &FlashService,
    claims: &ProviderClaims,
    service_instance_id: Uuid,
) -> Result<(), ApiError> {
    if resource.spec.service_instance_id != service_instance_id.to_string()
        || resource.spec.organization_id != claims.organization_id.to_string()
        || resource.spec.project_id != claims.project_id.to_string()
        || resource.metadata.deletion_timestamp.is_some()
    {
        return Err(ApiError::Forbidden);
    }
    Ok(())
}

fn service_pod_selector(service_instance_id: Uuid) -> ListParams {
    ListParams::default().labels(&format!(
        "flash.heterocloud.io/instance={service_instance_id}"
    ))
}

fn pod_belongs_to_service(pod: &Pod, service_instance_id: Uuid) -> bool {
    pod.metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get("flash.heterocloud.io/instance"))
        .is_some_and(|value| value == &service_instance_id.to_string())
}

fn pod_can_exec(pod: &Pod) -> bool {
    pod.metadata.deletion_timestamp.is_none()
        && pod
            .status
            .as_ref()
            .and_then(|status| status.phase.as_deref())
            == Some("Running")
        && pod
            .status
            .as_ref()
            .and_then(|status| status.container_statuses.as_ref())
            .is_some_and(|statuses| {
                statuses.iter().any(|container| {
                    container.name == "workload"
                        && container
                            .state
                            .as_ref()
                            .is_some_and(|state| state.running.is_some())
                })
            })
}

fn container_summary(pod: Pod) -> Option<ContainerSummary> {
    let ready = pod_can_exec(&pod);
    let name = pod.metadata.name?;
    let status = pod.status?;
    let phase = status.phase.unwrap_or_else(|| "Unknown".into());
    Some(ContainerSummary { name, phase, ready })
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum TerminalControl {
    Resize { cols: u16, rows: u16 },
}

async fn exec_shell(
    mut socket: WebSocket,
    pods: Api<Pod>,
    pod_name: String,
    _permit: OwnedSemaphorePermit,
) {
    let params = AttachParams::interactive_tty()
        .container("workload")
        .max_stdin_buf_size(MAX_EXEC_MESSAGE_BYTES)
        .max_stdout_buf_size(MAX_EXEC_MESSAGE_BYTES);
    let mut process = match pods
        .exec(&pod_name, ["/bin/sh", "-c", EXEC_SHELL_SCRIPT], &params)
        .await
    {
        Ok(process) => process,
        Err(error) => {
            tracing::warn!(%pod_name, error = %error, "Flash container shell could not be started");
            let _result = socket
                .send(Message::Text(
                    json!({"type": "error", "message": "container shell could not be started"})
                        .to_string()
                        .into(),
                ))
                .await;
            let _result = socket.close().await;
            return;
        }
    };
    let Some(mut stdin) = process.stdin() else {
        process.abort();
        let _result = socket.close().await;
        return;
    };
    let Some(mut stdout) = process.stdout() else {
        process.abort();
        let _result = socket.close().await;
        return;
    };
    let Some(mut terminal_size) = process.terminal_size() else {
        process.abort();
        let _result = socket.close().await;
        return;
    };
    let (mut sender, mut receiver) = socket.split();
    let session = async {
        let mut output = vec![0_u8; 8 * 1024];
        loop {
            tokio::select! {
                read = stdout.read(&mut output) => {
                    let count = read.unwrap_or(0);
                    if count == 0
                        || sender.send(Message::Binary(output[..count].to_vec().into())).await.is_err()
                    {
                        break;
                    }
                }
                message = receiver.next() => {
                    match message {
                        Some(Ok(Message::Binary(input))) => {
                            if stdin.write_all(&input).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Text(control))) => {
                            if let Ok(TerminalControl::Resize { cols, rows }) =
                                serde_json::from_str::<TerminalControl>(&control)
                                && cols > 0
                                && rows > 0
                            {
                                let _result = terminal_size
                                    .send(TerminalSize { width: cols, height: rows })
                                    .await;
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            if sender.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                        Some(Ok(Message::Pong(_))) => {}
                    }
                }
            }
        }
    };
    let _result = time::timeout(Duration::from_secs(MAX_EXEC_SESSION_SECONDS), session).await;
    let status = process.take_status();
    let _result = stdin.shutdown().await;
    drop(stdin);
    if let Some(status) = status {
        let _result = time::timeout(Duration::from_secs(EXEC_SHUTDOWN_GRACE_SECONDS), status).await;
    }
    process.abort();
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
    #[error("service instance was not found")]
    NotFound,
    #[error("{0}")]
    BadRequest(String),
    #[error("provider is still reconciling the resource")]
    NotReady,
    #[error("{0}")]
    Conflict(String),
    #[error("provider command is forbidden")]
    Forbidden,
    #[error("too many active exec sessions")]
    TooManyExecSessions,
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
            Self::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            Self::BadRequest(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
            Self::NotReady => (StatusCode::SERVICE_UNAVAILABLE, "operation_in_progress"),
            Self::Conflict(_) => (StatusCode::CONFLICT, "generation_conflict"),
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            Self::TooManyExecSessions => (StatusCode::TOO_MANY_REQUESTS, "exec_limit_reached"),
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

#[cfg(test)]
mod tests {
    use k8s_openapi::api::core::v1::Pod;
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::{
        ApiError, EXEC_SHELL_SCRIPT, FlashService, ProviderClaims, TerminalControl,
        container_summary, pod_belongs_to_service, pod_can_exec, validate_command,
        validate_resource_access,
    };

    fn access_fixture() -> Result<(ProviderClaims, FlashService), serde_json::Error> {
        let claims: ProviderClaims = serde_json::from_value(json!({
            "iss": "test", "aud": "flash", "sub": Uuid::from_u128(1),
            "organization_id": Uuid::from_u128(2), "project_id": Uuid::from_u128(3),
            "service_instance_id": Uuid::from_u128(7),
            "action": super::PROVIDER_LIST_CONTAINERS_ACTION,
            "generation": 4, "jti": Uuid::from_u128(5),
            "iat": 100, "nbf": 95, "exp": 160
        }))?;
        let resource = serde_json::from_value(json!({
            "metadata": {"name": super::resource_name(claims.service_instance_id)},
            "spec": {
                "desired_generation": claims.generation, "display_name": "test",
                "organization_id": claims.organization_id,
                "project_id": claims.project_id,
                "service_instance_id": claims.service_instance_id,
                "workload": {
                    "region": "heteronet-global", "image": "example.invalid/workload:test",
                    "replicas": 1, "cpu_millis": 500, "memory_mib": 256, "ports": [],
                    "exposure": {"type": "public", "traffic_mode": "forwarded",
                        "allowed_source_cidrs": [], "denied_source_cidrs": []}
                }
            },
            "status": {
                "phase": "provisioning", "observed_generation": claims.generation,
                "ready_replicas": 0, "desired_replicas": 1,
                "runtime_class": super::RUNTIME_CLASS_NAME
            }
        }))?;
        Ok((claims, resource))
    }

    async fn signed_status_request(
        mut claims: ProviderClaims,
        resource: Option<FlashService>,
        uri: String,
    ) -> Result<(axum::response::Response, usize), Box<dyn std::error::Error>> {
        use kube::{Api, Client, client::Body};
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tower::ServiceExt;
        // Test-only keypair, generated for this fixture and never used by a provider.
        let private_key = b"-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEICKoEEWPLg2OazcyTWzBEw/mMPPXatNOUcEUWDHo2y0Y\n-----END PRIVATE KEY-----\n";
        let public_key = "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAcBpAFx4KtN1FYwvSN0XJMWSGiAJPjzetPXEiuMX2azg=\n-----END PUBLIC KEY-----\n";
        claims.issued_at = chrono::Utc::now().timestamp();
        claims.not_before = claims.issued_at - 5;
        claims.expires_at = claims.issued_at + 60;
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::EdDSA);
        header.kid = Some("status-test".into());
        let token = jsonwebtoken::encode(
            &header,
            &claims,
            &jsonwebtoken::EncodingKey::from_ed_pem(private_key)?,
        )?;
        let authenticator = super::ProviderAuthenticator::from_public_keys_json(
            "test",
            "flash",
            &json!({"status-test": public_key}).to_string(),
        )?;
        let reads = Arc::new(AtomicUsize::new(0));
        let server_reads = reads.clone();
        let client = Client::new(
            tower::service_fn(move |request: http::Request<Body>| {
                let reads = server_reads.clone();
                let resource = resource.clone();
                async move {
                    assert_eq!(
                        request.method(),
                        http::Method::GET,
                        "status refresh must never write"
                    );
                    assert_eq!(
                        request.uri().path(),
                        format!(
                            "/apis/flash.heterocloud.io/v1alpha1/namespaces/test/flashservices/flash-{}",
                            Uuid::from_u128(7)
                        )
                    );
                    reads.fetch_add(1, Ordering::SeqCst);
                    let (status, body) = match resource {
                        Some(resource) => (200, serde_json::to_value(resource)?),
                        None => (
                            404,
                            json!({"apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": "NotFound", "message": "absent", "code": 404}),
                        ),
                    };
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                        http::Response::builder()
                            .status(status)
                            .body(Body::from(serde_json::to_vec(&body)?))?,
                    )
                }
            }),
            "test",
        );
        let app = super::app_router(Arc::new(super::AppState {
            services: Api::namespaced(client.clone(), "test"),
            pods: Api::namespaced(client, "test"),
            authenticator,
            exec_sessions: Arc::new(tokio::sync::Semaphore::new(0)),
        }));
        let response = app
            .oneshot(
                http::Request::builder()
                    .uri(uri)
                    .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(axum::body::Body::empty())?,
            )
            .await?;
        Ok((response, reads.load(Ordering::SeqCst)))
    }

    #[tokio::test]
    async fn signed_status_returns_raw_current_status_in_all_phases_without_writes()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut claims, resource) = access_fixture()?;
        claims.action = super::PROVIDER_STATUS_GET_ACTION.into();
        let uri = format!(
            "/internal/v1/service-instances/{}?generation=4",
            claims.service_instance_id
        );
        for phase in ["provisioning", "error", "ready"] {
            for count in [1, 7] {
                let mut value = serde_json::to_value(&resource)?;
                value["status"]["phase"] = json!(phase);
                value["status"]["ready_replicas"] = json!(count);
                value["status"]["desired_replicas"] = json!(count);
                let resource: FlashService = serde_json::from_value(value)?;
                let expected = serde_json::to_value(&resource.status)?;
                let (response, reads) =
                    signed_status_request(claims.clone(), Some(resource), uri.clone()).await?;
                assert_eq!(response.status(), http::StatusCode::OK);
                assert_eq!(reads, 1);
                let body = axum::body::to_bytes(response.into_body(), 65536).await?;
                assert_eq!(serde_json::from_slice::<Value>(&body)?, expected);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn signed_status_requires_its_own_action_and_exact_command_scope()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut claims, resource) = access_fixture()?;
        let uri = format!(
            "/internal/v1/service-instances/{}?generation=4",
            claims.service_instance_id
        );
        for action in [
            super::PROVIDER_EXEC_ACTION,
            super::PROVIDER_LIST_CONTAINERS_ACTION,
            super::PROVIDER_RECONCILE_ACTION,
            super::PROVIDER_DELETE_ACTION,
        ] {
            claims.action = action.into();
            let (response, reads) =
                signed_status_request(claims.clone(), Some(resource.clone()), uri.clone()).await?;
            assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
            assert_eq!(reads, 0);
        }
        claims.action = super::PROVIDER_STATUS_GET_ACTION.into();
        for bad_uri in [
            format!(
                "/internal/v1/service-instances/{}?generation=4",
                Uuid::from_u128(8)
            ),
            format!(
                "/internal/v1/service-instances/{}?generation=5",
                claims.service_instance_id
            ),
        ] {
            let (response, reads) =
                signed_status_request(claims.clone(), Some(resource.clone()), bad_uri).await?;
            assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
            assert_eq!(reads, 0);
        }
        for bad_uri in [
            format!(
                "/internal/v1/service-instances/{}",
                claims.service_instance_id
            ),
            format!("{uri}&unexpected=true"),
        ] {
            let (response, reads) =
                signed_status_request(claims.clone(), Some(resource.clone()), bad_uri).await?;
            assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
            assert_eq!(reads, 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn signed_status_rejects_tenant_deletion_and_stale_status()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut claims, resource) = access_fixture()?;
        claims.action = super::PROVIDER_STATUS_GET_ACTION.into();
        let uri = format!(
            "/internal/v1/service-instances/{}?generation=4",
            claims.service_instance_id
        );
        for (section, field, replacement, expected) in [
            (
                "spec",
                "service_instance_id",
                json!(Uuid::from_u128(8)),
                403,
            ),
            ("spec", "organization_id", json!(Uuid::from_u128(8)), 403),
            ("spec", "project_id", json!(Uuid::from_u128(8)), 403),
            (
                "metadata",
                "deletionTimestamp",
                json!("2026-08-21T00:00:00Z"),
                403,
            ),
            ("spec", "desired_generation", json!(3), 409),
            ("spec", "desired_generation", json!(5), 409),
            ("status", "observed_generation", json!(3), 503),
            ("status", "observed_generation", json!(5), 503),
        ] {
            let mut value = serde_json::to_value(&resource)?;
            value[section][field] = replacement;
            let (response, reads) = signed_status_request(
                claims.clone(),
                Some(serde_json::from_value(value)?),
                uri.clone(),
            )
            .await?;
            assert_eq!(response.status().as_u16(), expected, "{section}.{field}");
            assert_eq!(reads, 1);
            if expected == 503 {
                assert_eq!(response.headers()[http::header::RETRY_AFTER], "2");
            }
        }
        let mut missing_status = resource;
        missing_status.status = None;
        let (response, reads) =
            signed_status_request(claims.clone(), Some(missing_status), uri.clone()).await?;
        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(reads, 1);
        let (response, reads) = signed_status_request(claims, None, uri).await?;
        assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
        assert_eq!(reads, 1);
        Ok(())
    }

    #[test]
    fn diagnostics_scope_accepts_current_non_ready_resources()
    -> Result<(), Box<dyn std::error::Error>> {
        let (claims, resource) = access_fixture()?;
        for phase in ["provisioning", "error", "ready"] {
            let mut value = serde_json::to_value(&resource)?;
            value["status"]["phase"] = json!(phase);
            let resource = serde_json::from_value(value)?;
            validate_resource_access(
                &resource,
                &claims,
                claims.service_instance_id,
                claims.generation,
            )?;
        }
        Ok(())
    }

    #[test]
    fn diagnostics_scope_rejects_tenant_generation_and_deletion_mismatches()
    -> Result<(), Box<dyn std::error::Error>> {
        let (claims, resource) = access_fixture()?;
        for (section, field, replacement) in [
            ("spec", "service_instance_id", json!(Uuid::from_u128(8))),
            ("spec", "organization_id", json!(Uuid::from_u128(8))),
            ("spec", "project_id", json!(Uuid::from_u128(8))),
            ("spec", "desired_generation", json!(3)),
            ("spec", "desired_generation", json!(5)),
            ("status", "observed_generation", json!(3)),
            ("status", "observed_generation", json!(5)),
            (
                "metadata",
                "deletionTimestamp",
                json!("2026-08-21T00:00:00Z"),
            ),
        ] {
            let mut value = serde_json::to_value(&resource)?;
            value[section][field] = replacement;
            let resource = serde_json::from_value(value)?;
            assert!(
                matches!(
                    validate_resource_access(
                        &resource,
                        &claims,
                        claims.service_instance_id,
                        claims.generation,
                    ),
                    Err(ApiError::Forbidden)
                ),
                "{section}.{field}"
            );
        }
        let mut resource = resource;
        resource.status = None;
        assert!(matches!(
            validate_resource_access(
                &resource,
                &claims,
                claims.service_instance_id,
                claims.generation,
            ),
            Err(ApiError::NotReady)
        ));
        assert!(matches!(
            validate_command(&claims, Uuid::from_u128(8), claims.generation),
            Err(ApiError::Forbidden)
        ));
        for generation in [claims.generation - 1, claims.generation + 1] {
            assert!(matches!(
                validate_command(&claims, claims.service_instance_id, generation),
                Err(ApiError::Forbidden)
            ));
        }
        Ok(())
    }

    fn pod(instance_id: Uuid, phase: &str, ready: bool) -> Result<Pod, serde_json::Error> {
        serde_json::from_value(json!({
            "metadata": {
                "name": "flash-workload-abc123",
                "labels": {"flash.heterocloud.io/instance": instance_id.to_string()}
            },
            "status": {
                "phase": phase,
                "conditions": [
                    {"type": "Ready", "status": "False"},
                    {"type": "ContainersReady", "status": if ready { "True" } else { "False" }}
                ],
                "containerStatuses": [{
                    "name": "workload",
                    "image": "example.invalid/workload:test",
                    "imageID": "",
                    "ready": ready,
                    "restartCount": 0,
                    "started": true,
                    "state": {"running": {"startedAt": "2026-08-21T00:00:00Z"}}
                }]
            }
        }))
    }

    #[test]
    fn retained_pod_generation_remains_listable_and_executable()
    -> Result<(), Box<dyn std::error::Error>> {
        let instance_id = Uuid::from_u128(7);
        let mut retained = pod(instance_id, "Running", true)?;
        retained
            .metadata
            .labels
            .as_mut()
            .ok_or("labels")?
            .insert("flash.heterocloud.io/generation".into(), "1".into());
        assert_eq!(
            super::service_pod_selector(instance_id).label_selector,
            Some(format!("flash.heterocloud.io/instance={instance_id}"))
        );
        assert!(pod_belongs_to_service(&retained, instance_id));
        assert!(pod_can_exec(&retained));
        assert!(container_summary(retained).ok_or("summary")?.ready);
        Ok(())
    }

    #[test]
    fn exec_allows_owned_running_workload_without_readiness()
    -> Result<(), Box<dyn std::error::Error>> {
        let instance_id = Uuid::from_u128(7);
        let ready = pod(instance_id, "Running", true)?;
        assert!(pod_belongs_to_service(&ready, instance_id));
        assert!(pod_can_exec(&ready));
        assert!(!pod_belongs_to_service(&ready, Uuid::from_u128(8)));
        assert!(pod_can_exec(&pod(instance_id, "Running", false)?));
        let mut unowned = ready;
        unowned.metadata.labels = None;
        assert!(!pod_belongs_to_service(&unowned, instance_id));
        Ok(())
    }

    #[test]
    fn exec_rejects_non_running_missing_and_terminating_workloads()
    -> Result<(), Box<dyn std::error::Error>> {
        let instance_id = Uuid::from_u128(7);
        for phase in ["Pending", "Succeeded", "Failed", "Unknown"] {
            assert!(!pod_can_exec(&pod(instance_id, phase, true)?));
        }
        let base = serde_json::to_value(pod(instance_id, "Running", true)?)?;
        for state in [
            json!({"waiting": {"reason": "CrashLoopBackOff"}}),
            json!({"terminated": {"exitCode": 0}}),
            json!({}),
            Value::Null,
        ] {
            let mut value = base.clone();
            value["status"]["containerStatuses"][0]["state"] = state;
            assert!(!pod_can_exec(&serde_json::from_value(value)?));
        }
        for (section, field, replacement) in [
            (
                "metadata",
                "deletionTimestamp",
                json!("2026-08-21T00:00:00Z"),
            ),
            ("status", "containerStatuses", json!([])),
            ("status", "containerStatuses", Value::Null),
            ("status", "phase", Value::Null),
        ] {
            let mut value = base.clone();
            value[section][field] = replacement;
            assert!(!pod_can_exec(&serde_json::from_value(value)?));
        }
        let mut value = base;
        value["status"]["containerStatuses"][0]["name"] = json!("sidecar");
        assert!(!pod_can_exec(&serde_json::from_value(value.clone())?));
        value["status"] = Value::Null;
        assert!(!pod_can_exec(&serde_json::from_value(value)?));
        Ok(())
    }

    #[test]
    fn container_list_reports_exec_capability() -> Result<(), Box<dyn std::error::Error>> {
        let instance_id = Uuid::from_u128(9);
        for ready in [true, false] {
            let summary = container_summary(pod(instance_id, "Running", ready)?)
                .ok_or("named pod must have a summary")?;
            assert_eq!(summary.name, "flash-workload-abc123");
            assert_eq!(summary.phase, "Running");
            assert!(summary.ready);
        }
        let base = serde_json::to_value(pod(instance_id, "Running", true)?)?;
        for pointer in [
            "/metadata/deletionTimestamp",
            "/status/phase",
            "/status/containerStatuses/0/state",
        ] {
            let mut value = base.clone();
            match pointer {
                "/metadata/deletionTimestamp" => {
                    value["metadata"]["deletionTimestamp"] = json!("2026-08-21T00:00:00Z");
                }
                "/status/phase" => value["status"]["phase"] = json!("Pending"),
                _ => {
                    value["status"]["containerStatuses"][0]["state"] =
                        json!({"terminated": {"exitCode": 0}})
                }
            }
            let summary = container_summary(serde_json::from_value(value)?)
                .ok_or("named pod must have a summary")?;
            assert!(!summary.ready, "{pointer}");
        }
        Ok(())
    }

    #[test]
    fn terminal_resize_control_is_strict() {
        assert!(matches!(
            serde_json::from_value::<TerminalControl>(
                json!({"type": "resize", "cols": 120, "rows": 40})
            ),
            Ok(TerminalControl::Resize {
                cols: 120,
                rows: 40
            })
        ));
        assert!(
            serde_json::from_value::<TerminalControl>(
                json!({"type": "resize", "cols": 120, "rows": 40, "command": "id"})
            )
            .is_err()
        );
    }

    #[test]
    fn exec_shell_uses_utf8_truecolor_terminal_environment() {
        assert!(EXEC_SHELL_SCRIPT.contains("TERM=xterm-256color"));
        assert!(EXEC_SHELL_SCRIPT.contains("COLORTERM=truecolor"));
        assert!(EXEC_SHELL_SCRIPT.contains("LANG=C.UTF-8"));
        assert!(EXEC_SHELL_SCRIPT.contains("LC_ALL=C.UTF-8"));
        assert!(EXEC_SHELL_SCRIPT.contains("NPM_CONFIG_PREFIX=/root/.local"));
        assert!(EXEC_SHELL_SCRIPT.contains("cd /root"));
    }
}
