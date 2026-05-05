use crate::{
    auth::{require_scope, AuthContext},
    error::ApiError,
    record_event, AppState, EventDraft,
};
use aion_action::{EdgeAdapter, EdgeAdapterStatus, EdgeAdapterStatusReport, EdgeAdapterType};
use aion_entity::Entity;
use aion_event::{Event, EventSeverity};
use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{get, post, put},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/adapters",
            post(register_edge_adapter).get(list_edge_adapters),
        )
        .route("/adapters/:adapter_id", get(get_edge_adapter))
        .route(
            "/adapters/:adapter_id/heartbeat",
            put(heartbeat_edge_adapter),
        )
        .route("/adapters/:adapter_id/status", get(get_edge_adapter_status))
}

#[derive(Debug, Deserialize)]
struct RegisterEdgeAdapterRequest {
    adapter_key: String,
    display_name: Option<String>,
    adapter_type: EdgeAdapterType,
    status: Option<EdgeAdapterStatus>,
    version: Option<String>,
    host_id: Option<String>,
    site_id: Option<String>,
    environment: Option<String>,
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct EdgeAdapterHeartbeatRequest {
    status: EdgeAdapterStatus,
    version: Option<String>,
    host_id: Option<String>,
    site_id: Option<String>,
    environment: Option<String>,
    observed_at: Option<DateTime<Utc>>,
    uptime_seconds: Option<u64>,
    active_connectors: Option<u32>,
    active_plugins: Option<u32>,
    dlq_depth: Option<u64>,
    dlq_oldest_record_at: Option<DateTime<Utc>>,
    last_publish_success_at: Option<DateTime<Utc>>,
    last_publish_failure_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    metadata: Option<Value>,
}

#[derive(Debug, Serialize)]
struct EdgeAdapterRegistrationResponse {
    adapter: EdgeAdapter,
    entity: Option<Entity>,
    status: EdgeAdapterStatusReport,
    reused: bool,
}

#[derive(Debug, Serialize)]
struct EdgeAdapterStatusResponse {
    adapter: EdgeAdapter,
    entity: Option<Entity>,
    status: EdgeAdapterStatusReport,
}

async fn register_edge_adapter(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<RegisterEdgeAdapterRequest>,
) -> Result<(StatusCode, Json<EdgeAdapterRegistrationResponse>), ApiError> {
    require_scope(&state, &auth, "/adapters", "adapters:register")?;
    let now = Utc::now();
    let existing = state
        .storage
        .get_edge_adapter_by_key(state.tenant_id, &request.adapter_key)?;
    let previous_status = existing.as_ref().map(|adapter| adapter.status.clone());
    let reused = previous_status.is_some();

    let adapter = if let Some(mut adapter) = existing {
        adapter.display_name = request.display_name;
        adapter.adapter_type = request.adapter_type;
        adapter.status = request
            .status
            .clone()
            .unwrap_or_else(|| adapter.status.clone());
        adapter.version = request.version;
        adapter.host_id = request.host_id;
        adapter.site_id = request.site_id;
        adapter.environment = request.environment;
        adapter.metadata = request.metadata;
        adapter.updated_at = now;
        state.storage.update_edge_adapter(adapter)?
    } else {
        let adapter = EdgeAdapter::new(
            state.tenant_id,
            request.adapter_key,
            request.adapter_type,
            request.display_name,
            request.status.unwrap_or(EdgeAdapterStatus::Unknown),
            request.version,
            request.host_id,
            request.site_id,
            request.environment,
            request.metadata,
            now,
        )
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
        state.storage.create_edge_adapter(adapter)?
    };

    let entity = upsert_edge_adapter_entity(&state, &adapter)?;
    let status = edge_adapter_status_from_registration(&adapter, now);
    let status = state
        .storage
        .put_edge_adapter_status(state.tenant_id, status)?;
    record_edge_adapter_event(
        &state,
        "aion:EdgeAdapterRegistered",
        &adapter,
        Some(&entity),
        Some(&status),
        Some(json!({
            "reused": previous_status.is_some(),
            "source": "edge_adapter_api"
        })),
    )?;
    if previous_status.as_ref() != Some(&adapter.status) {
        record_edge_adapter_status_changed_event(
            &state,
            &adapter,
            Some(&entity),
            previous_status.clone(),
        )?;
    }

    Ok((
        if reused {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(EdgeAdapterRegistrationResponse {
            adapter,
            entity: Some(entity),
            status,
            reused,
        }),
    ))
}

async fn list_edge_adapters(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Vec<EdgeAdapter>>, ApiError> {
    require_scope(&state, &auth, "/adapters", "adapters:read")?;
    Ok(Json(state.storage.list_edge_adapters(state.tenant_id)?))
}

async fn get_edge_adapter(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(adapter_id): Path<Uuid>,
) -> Result<Json<EdgeAdapter>, ApiError> {
    require_scope(&state, &auth, "/adapters/:adapter_id", "adapters:read")?;
    Ok(Json(get_edge_adapter_record(&state, adapter_id)?))
}

async fn heartbeat_edge_adapter(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(adapter_id): Path<Uuid>,
    Json(request): Json<EdgeAdapterHeartbeatRequest>,
) -> Result<Json<EdgeAdapterStatusResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/adapters/:adapter_id/heartbeat",
        "adapters:heartbeat",
    )?;
    let mut adapter = get_edge_adapter_record(&state, adapter_id)?;
    let previous_status = adapter.status.clone();
    let observed_at = request.observed_at.unwrap_or_else(Utc::now);
    let request_metadata = request.metadata.clone();
    adapter.heartbeat(request.status.clone(), observed_at);
    if request.version.is_some() {
        adapter.version = request.version;
    }
    if request.host_id.is_some() {
        adapter.host_id = request.host_id;
    }
    if request.site_id.is_some() {
        adapter.site_id = request.site_id;
    }
    if request.environment.is_some() {
        adapter.environment = request.environment;
    }
    if request_metadata.is_some() {
        adapter.metadata = request_metadata.clone();
    }
    let adapter = state.storage.update_edge_adapter(adapter)?;
    let entity = upsert_edge_adapter_entity(&state, &adapter)?;
    let status = EdgeAdapterStatusReport {
        adapter_id: adapter.id,
        status: request.status,
        observed_at,
        uptime_seconds: request.uptime_seconds,
        active_connectors: request.active_connectors,
        active_plugins: request.active_plugins,
        dlq_depth: request.dlq_depth,
        dlq_oldest_record_at: request.dlq_oldest_record_at,
        last_publish_success_at: request.last_publish_success_at,
        last_publish_failure_at: request.last_publish_failure_at,
        last_error: request.last_error,
        metadata: request_metadata,
    };
    let status = state
        .storage
        .put_edge_adapter_status(state.tenant_id, status)?;
    record_edge_adapter_event(
        &state,
        "aion:EdgeAdapterHeartbeat",
        &adapter,
        Some(&entity),
        Some(&status),
        Some(json!({"source": "edge_adapter_api"})),
    )?;
    if previous_status != adapter.status {
        record_edge_adapter_status_changed_event(
            &state,
            &adapter,
            Some(&entity),
            Some(previous_status),
        )?;
    }

    Ok(Json(EdgeAdapterStatusResponse {
        adapter,
        entity: Some(entity),
        status,
    }))
}

async fn get_edge_adapter_status(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(adapter_id): Path<Uuid>,
) -> Result<Json<EdgeAdapterStatusResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/adapters/:adapter_id/status",
        "adapters:read",
    )?;
    let adapter = get_edge_adapter_record(&state, adapter_id)?;
    let entity = get_edge_adapter_entity(&state, &adapter)?;
    let status = state
        .storage
        .get_edge_adapter_status(state.tenant_id, adapter_id)?
        .unwrap_or_else(|| edge_adapter_status_from_adapter(&adapter, adapter.last_seen_at));

    Ok(Json(EdgeAdapterStatusResponse {
        adapter,
        entity,
        status,
    }))
}

fn get_edge_adapter_record(state: &AppState, adapter_id: Uuid) -> Result<EdgeAdapter, ApiError> {
    state
        .storage
        .get_edge_adapter(state.tenant_id, adapter_id)?
        .ok_or_else(ApiError::not_found)
}

fn get_edge_adapter_entity(
    state: &AppState,
    adapter: &EdgeAdapter,
) -> Result<Option<Entity>, ApiError> {
    state
        .storage
        .get_entity_by_key(
            state.tenant_id,
            &edge_adapter_entity_key(&adapter.adapter_key),
        )
        .map_err(ApiError::from)
}

fn upsert_edge_adapter_entity(state: &AppState, adapter: &EdgeAdapter) -> Result<Entity, ApiError> {
    let entity_key = edge_adapter_entity_key(&adapter.adapter_key);
    let now = Utc::now();
    let jsonld = edge_adapter_jsonld(adapter);
    if let Some(mut entity) = state
        .storage
        .get_entity_by_key(state.tenant_id, &entity_key)?
    {
        let unchanged = entity.entity_type == "aion:EdgeAdapter" && entity.jsonld == jsonld;
        if unchanged {
            return Ok(entity);
        }
        entity.entity_type = "aion:EdgeAdapter".to_string();
        entity.jsonld = jsonld;
        entity.updated_at = now;
        return Ok(state.storage.update_entity(entity)?);
    }

    let entity = Entity::new(state.tenant_id, entity_key, "aion:EdgeAdapter", jsonld, now)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    Ok(state.storage.create_entity(entity)?)
}

fn edge_adapter_entity_key(adapter_key: &str) -> String {
    format!("edge-adapter:{adapter_key}")
}

fn edge_adapter_jsonld(adapter: &EdgeAdapter) -> Value {
    json!({
        "@context": {"aion": "https://aioncore.org/ns#"},
        "@id": format!("urn:aion:edge-adapter:{}", adapter.adapter_key),
        "@type": "aion:EdgeAdapter",
        "entity_key": edge_adapter_entity_key(&adapter.adapter_key),
        "adapter_key": adapter.adapter_key,
        "adapter_type": adapter.adapter_type,
        "status": adapter.status,
        "display_name": adapter.display_name,
        "version": adapter.version,
        "host_id": adapter.host_id,
        "site_id": adapter.site_id,
        "environment": adapter.environment,
        "last_seen_at": adapter.last_seen_at,
        "metadata": adapter.metadata
    })
}

fn edge_adapter_status_from_registration(
    adapter: &EdgeAdapter,
    observed_at: DateTime<Utc>,
) -> EdgeAdapterStatusReport {
    EdgeAdapterStatusReport {
        adapter_id: adapter.id,
        status: adapter.status.clone(),
        observed_at,
        uptime_seconds: None,
        active_connectors: None,
        active_plugins: None,
        dlq_depth: None,
        dlq_oldest_record_at: None,
        last_publish_success_at: None,
        last_publish_failure_at: None,
        last_error: None,
        metadata: adapter.metadata.clone(),
    }
}

fn edge_adapter_status_from_adapter(
    adapter: &EdgeAdapter,
    observed_at: Option<DateTime<Utc>>,
) -> EdgeAdapterStatusReport {
    EdgeAdapterStatusReport {
        adapter_id: adapter.id,
        status: adapter.status.clone(),
        observed_at: observed_at.unwrap_or(adapter.created_at),
        uptime_seconds: None,
        active_connectors: None,
        active_plugins: None,
        dlq_depth: None,
        dlq_oldest_record_at: None,
        last_publish_success_at: None,
        last_publish_failure_at: None,
        last_error: None,
        metadata: adapter.metadata.clone(),
    }
}

fn record_edge_adapter_event(
    state: &AppState,
    event_type: impl Into<String>,
    adapter: &EdgeAdapter,
    entity: Option<&Entity>,
    status: Option<&EdgeAdapterStatusReport>,
    metadata: Option<Value>,
) -> Result<Event, ApiError> {
    let mut event_metadata = json!({
        "adapter_id": adapter.id,
        "adapter_key": adapter.adapter_key,
        "adapter_type": adapter.adapter_type,
        "status": adapter.status,
        "version": adapter.version,
        "host_id": adapter.host_id,
        "site_id": adapter.site_id,
        "environment": adapter.environment,
        "last_seen_at": adapter.last_seen_at
    });
    if let Some(object) = event_metadata.as_object_mut() {
        if let Some(status) = status {
            object.insert(
                "status_report".to_string(),
                json!({
                    "status": status.status,
                    "observed_at": status.observed_at,
                    "uptime_seconds": status.uptime_seconds,
                    "active_connectors": status.active_connectors,
                    "active_plugins": status.active_plugins,
                    "dlq_depth": status.dlq_depth,
                    "dlq_oldest_record_at": status.dlq_oldest_record_at,
                    "last_publish_success_at": status.last_publish_success_at,
                    "last_publish_failure_at": status.last_publish_failure_at,
                    "last_error": status.last_error,
                    "metadata": status.metadata
                }),
            );
        }
        if let Some(entity) = entity {
            object.insert("entity_id".to_string(), json!(entity.id));
            object.insert("entity_key".to_string(), json!(entity.entity_key));
        }
        if let Some(metadata) = metadata {
            object.insert("metadata".to_string(), metadata);
        }
    }

    record_event(
        state,
        EventDraft {
            event_type: event_type.into(),
            severity: EventSeverity::Info,
            source_entity_id: None,
            target_entity_id: entity.map(|entity| entity.id),
            message: Some(format!("Edge adapter {} event", adapter.adapter_key)),
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: None,
            raw_message_id: None,
            observation_id: None,
            command_id: None,
            action_id: None,
            action_result_id: None,
            metadata: Some(event_metadata),
        },
    )
}

fn record_edge_adapter_status_changed_event(
    state: &AppState,
    adapter: &EdgeAdapter,
    entity: Option<&Entity>,
    previous_status: Option<EdgeAdapterStatus>,
) -> Result<Event, ApiError> {
    record_edge_adapter_event(
        state,
        "aion:EdgeAdapterStatusChanged",
        adapter,
        entity,
        None,
        Some(json!({
            "previous_status": previous_status,
            "current_status": adapter.status
        })),
    )
}
