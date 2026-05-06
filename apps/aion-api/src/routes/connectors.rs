use crate::{
    auth::{require_scope, AuthContext},
    ensure_connector_secret_exists,
    error::ApiError,
    get_connector, reconcile_connector_workers_after_mutation, record_connector_event, AppState,
    ConnectorWorkerRuntimeState,
};
use aion_storage::{ConnectorProfile, IngestionConnector, IngestionConnectorType};
use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{get, post, put},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/ingestion/connectors",
            post(create_ingestion_connector).get(list_ingestion_connectors),
        )
        .route(
            "/ingestion/connectors/:connector_id",
            get(get_ingestion_connector).patch(update_ingestion_connector),
        )
        .route(
            "/ingestion/connectors/:connector_id/enable",
            put(enable_ingestion_connector),
        )
        .route(
            "/ingestion/connectors/:connector_id/disable",
            put(disable_ingestion_connector),
        )
        .route(
            "/ingestion/connectors/:connector_id/status",
            get(get_ingestion_connector_status),
        )
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateIngestionConnectorRequest {
    pub connector_key: String,
    pub connector_type: IngestionConnectorType,
    pub connector_profile: ConnectorProfile,
    #[serde(default)]
    pub enabled: bool,
    pub display_name: Option<String>,
    pub protocol: Option<String>,
    pub endpoint: Option<String>,
    pub broker_url: Option<String>,
    pub client_id: Option<String>,
    pub topic_filter: Option<String>,
    pub http_path: Option<String>,
    pub payload_format: Option<String>,
    pub content_type: Option<String>,
    pub secret_ref_id: Option<Uuid>,
    pub default_producer_entity_id: Option<Uuid>,
    pub default_feature_of_interest_id: Option<Uuid>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateIngestionConnectorRequest {
    pub display_name: Option<String>,
    pub enabled: Option<bool>,
    pub protocol: Option<String>,
    pub endpoint: Option<String>,
    pub broker_url: Option<String>,
    pub client_id: Option<String>,
    pub topic_filter: Option<String>,
    pub http_path: Option<String>,
    pub payload_format: Option<String>,
    pub content_type: Option<String>,
    pub secret_ref_id: Option<Uuid>,
    pub default_producer_entity_id: Option<Uuid>,
    pub default_feature_of_interest_id: Option<Uuid>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Serialize)]
pub(crate) struct IngestionConnectorStatusResponse {
    pub connector_id: Uuid,
    pub connector_key: String,
    pub connector_type: IngestionConnectorType,
    pub connector_profile: ConnectorProfile,
    pub enabled: bool,
    pub status: &'static str,
    pub last_error: Option<String>,
    pub last_message_at: Option<DateTime<Utc>>,
    pub last_successful_ingest_at: Option<DateTime<Utc>>,
    pub last_failed_ingest_at: Option<DateTime<Utc>>,
}

async fn create_ingestion_connector(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateIngestionConnectorRequest>,
) -> Result<(StatusCode, Json<IngestionConnector>), ApiError> {
    require_scope(&state, &auth, "/ingestion/connectors", "connectors:admin")?;
    ensure_connector_secret_exists(&state, request.secret_ref_id)?;
    let connector = IngestionConnector::new(
        state.tenant_id,
        request.connector_key,
        request.connector_type,
        request.connector_profile,
        request.enabled,
        request.display_name,
        request.protocol,
        request.endpoint,
        request.broker_url,
        request.client_id,
        request.topic_filter,
        request.http_path,
        request.payload_format,
        request.content_type,
        request.default_producer_entity_id,
        request.default_feature_of_interest_id,
        request.metadata,
        Utc::now(),
    )?;
    let mut connector = connector;
    connector.secret_ref_id = request.secret_ref_id;
    let connector = state.storage.create_ingestion_connector(connector)?;
    record_connector_event(
        &state,
        "aion:IngestionConnectorCreated",
        &connector,
        Some("Ingestion connector created".to_string()),
    )?;
    reconcile_connector_workers_after_mutation(&state).await;
    Ok((StatusCode::CREATED, Json(connector)))
}

async fn list_ingestion_connectors(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Vec<IngestionConnector>>, ApiError> {
    require_scope(&state, &auth, "/ingestion/connectors", "connectors:read")?;
    Ok(Json(
        state.storage.list_ingestion_connectors(state.tenant_id)?,
    ))
}

async fn get_ingestion_connector(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
) -> Result<Json<IngestionConnector>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id",
        "connectors:read",
    )?;
    Ok(Json(get_connector(&state, connector_id)?))
}

async fn update_ingestion_connector(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
    Json(request): Json<UpdateIngestionConnectorRequest>,
) -> Result<Json<IngestionConnector>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id",
        "connectors:admin",
    )?;
    let mut connector = get_connector(&state, connector_id)?;
    apply_connector_update(&state, &mut connector, request)?;
    connector.updated_at = Utc::now();

    let connector = state.storage.update_ingestion_connector(connector)?;
    record_connector_event(
        &state,
        "aion:IngestionConnectorUpdated",
        &connector,
        Some("Ingestion connector updated".to_string()),
    )?;
    reconcile_connector_workers_after_mutation(&state).await;
    Ok(Json(connector))
}

async fn enable_ingestion_connector(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
) -> Result<Json<IngestionConnector>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/enable",
        "connectors:admin",
    )?;
    let mut connector = get_connector(&state, connector_id)?;
    connector.set_enabled(true, Utc::now());
    let connector = state.storage.update_ingestion_connector(connector)?;
    record_connector_event(
        &state,
        "aion:IngestionConnectorEnabled",
        &connector,
        Some("Ingestion connector enabled".to_string()),
    )?;
    reconcile_connector_workers_after_mutation(&state).await;
    Ok(Json(connector))
}

async fn disable_ingestion_connector(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
) -> Result<Json<IngestionConnector>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/disable",
        "connectors:admin",
    )?;
    let mut connector = get_connector(&state, connector_id)?;
    connector.set_enabled(false, Utc::now());
    let connector = state.storage.update_ingestion_connector(connector)?;
    record_connector_event(
        &state,
        "aion:IngestionConnectorDisabled",
        &connector,
        Some("Ingestion connector disabled".to_string()),
    )?;
    reconcile_connector_workers_after_mutation(&state).await;
    Ok(Json(connector))
}

async fn get_ingestion_connector_status(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
) -> Result<Json<IngestionConnectorStatusResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/status",
        "connectors:read",
    )?;
    let connector = get_connector(&state, connector_id)?;
    Ok(Json(connector_status(&state, &connector)))
}

fn apply_connector_update(
    state: &AppState,
    connector: &mut IngestionConnector,
    request: UpdateIngestionConnectorRequest,
) -> Result<(), ApiError> {
    if let Some(display_name) = request.display_name {
        connector.display_name = Some(display_name);
    }
    if let Some(enabled) = request.enabled {
        connector.enabled = enabled;
    }
    if let Some(protocol) = request.protocol {
        connector.protocol = Some(protocol);
    }
    if let Some(endpoint) = request.endpoint {
        connector.endpoint = Some(endpoint);
    }
    if let Some(broker_url) = request.broker_url {
        connector.broker_url = Some(broker_url);
    }
    if let Some(client_id) = request.client_id {
        connector.client_id = Some(client_id);
    }
    if let Some(topic_filter) = request.topic_filter {
        connector.topic_filter = Some(topic_filter);
    }
    if let Some(http_path) = request.http_path {
        connector.http_path = Some(http_path);
    }
    if let Some(payload_format) = request.payload_format {
        connector.payload_format = Some(payload_format);
    }
    if let Some(content_type) = request.content_type {
        connector.content_type = Some(content_type);
    }
    if let Some(secret_ref_id) = request.secret_ref_id {
        ensure_connector_secret_exists(state, Some(secret_ref_id))?;
        connector.secret_ref_id = Some(secret_ref_id);
    }
    if let Some(default_producer_entity_id) = request.default_producer_entity_id {
        connector.default_producer_entity_id = Some(default_producer_entity_id);
    }
    if let Some(default_feature_of_interest_id) = request.default_feature_of_interest_id {
        connector.default_feature_of_interest_id = Some(default_feature_of_interest_id);
    }
    if let Some(metadata) = request.metadata {
        connector.metadata = Some(metadata);
    }
    Ok(())
}

fn connector_status(
    state: &AppState,
    connector: &IngestionConnector,
) -> IngestionConnectorStatusResponse {
    if let Some(worker) = state
        .connector_worker_statuses
        .read()
        .ok()
        .and_then(|statuses| statuses.get(&connector.id).cloned())
    {
        return IngestionConnectorStatusResponse {
            connector_id: connector.id,
            connector_key: connector.connector_key.clone(),
            connector_type: connector.connector_type.clone(),
            connector_profile: connector.connector_profile.clone(),
            enabled: connector.enabled,
            status: if !connector.enabled {
                "disabled"
            } else {
                connector_runtime_state_label(&worker.status)
            },
            last_error: worker.last_error,
            last_message_at: worker.last_message_at,
            last_successful_ingest_at: worker.last_successful_ingest_at,
            last_failed_ingest_at: worker.last_failed_ingest_at,
        };
    }

    let (status, last_error) = if !connector.enabled {
        ("disabled", None)
    } else {
        match connector.connector_type {
            IngestionConnectorType::Http => ("ready", None),
            IngestionConnectorType::Mqtt => (
                "planned",
                Some("dynamic connector workers are disabled unless AIONCORE_CONNECTOR_WORKERS_ENABLED=true".to_string()),
            ),
            IngestionConnectorType::Future => (
                "unsupported",
                Some("future connector runtime is not implemented yet".to_string()),
            ),
        }
    };

    IngestionConnectorStatusResponse {
        connector_id: connector.id,
        connector_key: connector.connector_key.clone(),
        connector_type: connector.connector_type.clone(),
        connector_profile: connector.connector_profile.clone(),
        enabled: connector.enabled,
        status,
        last_error,
        last_message_at: None,
        last_successful_ingest_at: None,
        last_failed_ingest_at: None,
    }
}

fn connector_runtime_state_label(status: &ConnectorWorkerRuntimeState) -> &'static str {
    match status {
        ConnectorWorkerRuntimeState::Planned => "planned",
        ConnectorWorkerRuntimeState::Starting => "starting",
        ConnectorWorkerRuntimeState::Running => "ready",
        ConnectorWorkerRuntimeState::Reconnecting => "reconnecting",
        ConnectorWorkerRuntimeState::Degraded => "degraded",
        ConnectorWorkerRuntimeState::Stopped => "stopped",
        ConnectorWorkerRuntimeState::Skipped => "skipped",
        ConnectorWorkerRuntimeState::Invalid => "error",
        ConnectorWorkerRuntimeState::Error => "error",
        ConnectorWorkerRuntimeState::Unsupported => "unsupported",
    }
}
