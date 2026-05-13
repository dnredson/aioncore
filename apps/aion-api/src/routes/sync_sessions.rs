use crate::{
    auth::{
        is_admin_all, principal_tenant_id, require_scope, require_scope_for_write,
        tenant_for_created_resource, AuthContext,
    },
    error::ApiError,
    record_event, state_for_tenant, AppState, AuthMode, EventDraft,
};
use aion_event::EventSeverity;
use aion_storage::SyncSessionFilter;
use aion_sync::{SyncSession, SyncSessionStatus};
use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

const DEFAULT_SYNC_SESSION_LIST_LIMIT: u32 = 50;
const MAX_SYNC_SESSION_LIST_LIMIT: u32 = 200;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/sync-sessions",
            post(create_sync_session).get(list_sync_sessions),
        )
        .route(
            "/sync-sessions/:session_id",
            get(get_sync_session).patch(patch_sync_session),
        )
        .route(
            "/sync-sessions/:session_id/status",
            axum::routing::patch(update_sync_session_status),
        )
        .route(
            "/sync-sessions/:session_id/complete",
            post(complete_sync_session),
        )
        .route("/sync-sessions/:session_id/fail", post(fail_sync_session))
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateSyncSessionRequest {
    pub tenant_id: Option<Uuid>,
    pub sync_session_id: String,
    pub source_system: Option<String>,
    pub source_id: Option<String>,
    pub connector_id: Option<Uuid>,
    pub edge_adapter_id: Option<Uuid>,
    pub status: Option<SyncSessionStatus>,
    pub connectivity_state: Option<String>,
    pub expected_items: Option<u64>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct PatchSyncSessionRequest {
    pub source_system: Option<String>,
    pub source_id: Option<String>,
    pub connector_id: Option<Uuid>,
    pub edge_adapter_id: Option<Uuid>,
    pub connectivity_state: Option<String>,
    pub expected_items: Option<u64>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateSyncSessionStatusRequest {
    pub status: SyncSessionStatus,
    pub message: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct ListSyncSessionsQuery {
    pub status: Option<SyncSessionStatus>,
    pub source_system: Option<String>,
    pub source_id: Option<String>,
    pub connector_id: Option<Uuid>,
    pub sync_session_id: Option<String>,
    pub limit: Option<u32>,
}

async fn create_sync_session(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateSyncSessionRequest>,
) -> Result<(StatusCode, Json<SyncSession>), ApiError> {
    require_scope_for_write(&state, &auth, "/sync-sessions", "sync-sessions:write")?;
    let tenant_id = sync_session_tenant_for_create(&state, &auth, request.tenant_id)?;
    let scoped_state = state_for_tenant(&state, tenant_id);
    let session = SyncSession::new(
        scoped_state.tenant_id,
        request.sync_session_id,
        request.source_system,
        request.source_id,
        request.connector_id,
        request.edge_adapter_id,
        request.status,
        request.connectivity_state,
        request.expected_items,
        request.metadata,
        Utc::now(),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let session = scoped_state.storage.create_sync_session(session)?;
    record_sync_session_event(
        &scoped_state,
        "aion:SyncSessionCreated",
        EventSeverity::Info,
        &session,
        Some("sync session created".to_string()),
    )?;
    Ok((StatusCode::CREATED, Json(session)))
}

async fn list_sync_sessions(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<ListSyncSessionsQuery>,
) -> Result<Json<Vec<SyncSession>>, ApiError> {
    require_scope(&state, &auth, "/sync-sessions", "sync-sessions:read")?;
    let filter = query_to_filter(query);
    let sessions = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state.storage.list_sync_sessions(state.tenant_id, filter)?
    } else if is_admin_all(&auth) {
        state.storage.list_all_sync_sessions(filter)?
    } else {
        state
            .storage
            .list_sync_sessions(principal_tenant_id(&auth)?, filter)?
    };
    Ok(Json(sessions))
}

async fn get_sync_session(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(session_id): Path<Uuid>,
) -> Result<Json<SyncSession>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/sync-sessions/:session_id",
        "sync-sessions:read",
    )?;
    let session = require_same_tenant_for_target_sync_session(
        &state,
        &auth,
        "/sync-sessions/:session_id",
        session_id,
    )?;
    Ok(Json(session))
}

async fn patch_sync_session(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(session_id): Path<Uuid>,
    Json(request): Json<PatchSyncSessionRequest>,
) -> Result<Json<SyncSession>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/sync-sessions/:session_id",
        "sync-sessions:write",
    )?;
    let mut session = require_same_tenant_for_target_sync_session(
        &state,
        &auth,
        "/sync-sessions/:session_id",
        session_id,
    )?;
    if request.source_system.is_some() {
        session.source_system = normalize_optional(request.source_system);
    }
    if request.source_id.is_some() {
        session.source_id = normalize_optional(request.source_id);
    }
    if request.connector_id.is_some() {
        session.connector_id = request.connector_id;
    }
    if request.edge_adapter_id.is_some() {
        session.edge_adapter_id = request.edge_adapter_id;
    }
    if request.connectivity_state.is_some() {
        session.connectivity_state = normalize_optional(request.connectivity_state);
    }
    if request.expected_items.is_some() {
        session.expected_items = request.expected_items;
    }
    if let Some(metadata) = request.metadata {
        session.metadata = metadata;
    }
    session.updated_at = Utc::now();
    let scoped_state = state_for_tenant(&state, session.tenant_id);
    let session = scoped_state.storage.update_sync_session(session)?;
    record_sync_session_event(
        &scoped_state,
        "aion:SyncSessionUpdated",
        EventSeverity::Info,
        &session,
        Some("sync session updated".to_string()),
    )?;
    Ok(Json(session))
}

async fn update_sync_session_status(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(session_id): Path<Uuid>,
    Json(request): Json<UpdateSyncSessionStatusRequest>,
) -> Result<Json<SyncSession>, ApiError> {
    update_sync_session_status_inner(state, auth, session_id, request).await
}

async fn complete_sync_session(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(session_id): Path<Uuid>,
) -> Result<Json<SyncSession>, ApiError> {
    update_sync_session_status_inner(
        state,
        auth,
        session_id,
        UpdateSyncSessionStatusRequest {
            status: SyncSessionStatus::Completed,
            message: Some("sync session completed".to_string()),
            metadata: None,
        },
    )
    .await
}

async fn fail_sync_session(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(session_id): Path<Uuid>,
    Json(request): Json<Option<UpdateSyncSessionStatusRequest>>,
) -> Result<Json<SyncSession>, ApiError> {
    update_sync_session_status_inner(
        state,
        auth,
        session_id,
        request.unwrap_or(UpdateSyncSessionStatusRequest {
            status: SyncSessionStatus::Failed,
            message: Some("sync session failed".to_string()),
            metadata: None,
        }),
    )
    .await
}

async fn update_sync_session_status_inner(
    state: AppState,
    auth: AuthContext,
    session_id: Uuid,
    request: UpdateSyncSessionStatusRequest,
) -> Result<Json<SyncSession>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/sync-sessions/:session_id/status",
        "sync-sessions:write",
    )?;
    let mut session = require_same_tenant_for_target_sync_session(
        &state,
        &auth,
        "/sync-sessions/:session_id/status",
        session_id,
    )?;
    if let Some(metadata) = request.metadata {
        session.metadata = metadata;
    }
    session.set_status(request.status, Utc::now());
    let scoped_state = state_for_tenant(&state, session.tenant_id);
    let session = scoped_state.storage.update_sync_session(session)?;
    record_sync_session_event(
        &scoped_state,
        sync_session_status_event_type(&session.status),
        EventSeverity::Info,
        &session,
        request
            .message
            .or_else(|| Some("sync session status updated".to_string())),
    )?;
    Ok(Json(session))
}

fn sync_session_tenant_for_create(
    state: &AppState,
    auth: &AuthContext,
    explicit_tenant_id: Option<Uuid>,
) -> Result<Uuid, ApiError> {
    if matches!(auth.mode, AuthMode::Token) && is_admin_all(auth) {
        Ok(explicit_tenant_id.unwrap_or(tenant_for_created_resource(state, auth)?))
    } else {
        tenant_for_created_resource(state, auth)
    }
}

fn require_same_tenant_for_target_sync_session(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    session_id: Uuid,
) -> Result<SyncSession, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        return state
            .storage
            .get_sync_session(state.tenant_id, session_id)?
            .ok_or_else(ApiError::not_found);
    }

    if is_admin_all(auth) {
        return state
            .storage
            .get_sync_session_any_tenant(session_id)?
            .ok_or_else(ApiError::not_found);
    }

    let tenant_id = principal_tenant_id(auth)?;
    match state.storage.get_sync_session(tenant_id, session_id)? {
        Some(session) => Ok(session),
        None => {
            if state
                .storage
                .get_sync_session_any_tenant(session_id)?
                .is_some()
            {
                Err(ApiError::forbidden(format!(
                    "principal tenant does not own the target sync session for {endpoint}"
                )))
            } else {
                Err(ApiError::not_found())
            }
        }
    }
}

fn query_to_filter(query: ListSyncSessionsQuery) -> SyncSessionFilter {
    SyncSessionFilter {
        status: query.status,
        source_system: query.source_system,
        source_id: query.source_id,
        connector_id: query.connector_id,
        sync_session_id: query.sync_session_id,
        limit: query
            .limit
            .unwrap_or(DEFAULT_SYNC_SESSION_LIST_LIMIT)
            .min(MAX_SYNC_SESSION_LIST_LIMIT),
    }
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn sync_session_status_event_type(status: &SyncSessionStatus) -> &'static str {
    match status {
        SyncSessionStatus::Open => "aion:SyncSessionOpened",
        SyncSessionStatus::Receiving => "aion:SyncSessionReceiving",
        SyncSessionStatus::Completed => "aion:SyncSessionCompleted",
        SyncSessionStatus::Failed => "aion:SyncSessionFailed",
        SyncSessionStatus::Abandoned => "aion:SyncSessionAbandoned",
    }
}

fn sync_session_status_label(status: &SyncSessionStatus) -> &'static str {
    match status {
        SyncSessionStatus::Open => "open",
        SyncSessionStatus::Receiving => "receiving",
        SyncSessionStatus::Completed => "completed",
        SyncSessionStatus::Failed => "failed",
        SyncSessionStatus::Abandoned => "abandoned",
    }
}

fn record_sync_session_event(
    state: &AppState,
    event_type: &str,
    severity: EventSeverity,
    session: &SyncSession,
    message: Option<String>,
) -> Result<(), ApiError> {
    let _ = record_event(
        state,
        EventDraft {
            event_type: event_type.to_string(),
            severity,
            source_entity_id: None,
            target_entity_id: None,
            message,
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: Some(session.sync_session_id.clone()),
            raw_message_id: None,
            observation_id: None,
            command_id: None,
            action_id: None,
            action_result_id: None,
            metadata: Some(json!({
                "sync_session_record_id": session.id,
                "sync_session_id": session.sync_session_id,
                "source_system": session.source_system,
                "source_id": session.source_id,
                "connector_id": session.connector_id,
                "status": sync_session_status_label(&session.status),
                "received_items": session.received_items,
                "accepted_count": session.accepted_count,
                "duplicate_count": session.duplicate_count,
                "failed_count": session.failed_count,
                "observations_created": session.observations_created,
                "last_batch_id": session.last_batch_id,
            })),
        },
    )?;
    Ok(())
}
