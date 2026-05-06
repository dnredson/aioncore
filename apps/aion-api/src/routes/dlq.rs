use crate::{
    auth::{
        is_admin_all, principal_tenant_id, require_scope, require_scope_for_write,
        tenant_for_created_resource, AuthContext,
    },
    error::ApiError,
    record_event, state_for_tenant, AppState, AuthMode, EventDraft,
};
use aion_dlq::{DlqFailureStage, DlqRecord, DlqStatus};
use aion_event::EventSeverity;
use aion_storage::DlqRecordFilter;
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

const DEFAULT_DLQ_LIST_LIMIT: u32 = 50;
const MAX_DLQ_LIST_LIMIT: u32 = 200;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/dlq/records",
            post(create_dlq_record).get(list_dlq_records),
        )
        .route("/dlq/records/:record_id", get(get_dlq_record))
        .route(
            "/dlq/records/:record_id/status",
            axum::routing::patch(update_dlq_record_status),
        )
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateDlqRecordRequest {
    pub tenant_id: Option<Uuid>,
    pub dlq_key: Option<String>,
    pub source_system: Option<String>,
    pub source_id: Option<String>,
    pub connector_id: Option<Uuid>,
    pub flow_id: Option<Uuid>,
    pub raw_message_id: Option<Uuid>,
    pub event_id: Option<Uuid>,
    pub command_id: Option<Uuid>,
    pub idempotency_key: Option<String>,
    pub external_flow_id: Option<String>,
    pub external_flow_name: Option<String>,
    pub external_flowfile_uuid: Option<String>,
    pub external_process_group_id: Option<String>,
    pub external_processor_id: Option<String>,
    pub external_provenance_uri: Option<String>,
    pub sync_session_id: Option<String>,
    pub payload_format: Option<String>,
    pub payload: Option<Value>,
    pub payload_hash: Option<String>,
    pub failure_stage: DlqFailureStage,
    pub failure_reason: String,
    pub failure_detail: Option<String>,
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default)]
    pub replay_count: u32,
    pub status: Option<DlqStatus>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct ListDlqRecordsQuery {
    pub status: Option<DlqStatus>,
    pub failure_stage: Option<DlqFailureStage>,
    pub source_system: Option<String>,
    pub connector_id: Option<Uuid>,
    pub flow_id: Option<Uuid>,
    pub raw_message_id: Option<Uuid>,
    pub idempotency_key: Option<String>,
    pub external_flowfile_uuid: Option<String>,
    pub sync_session_id: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateDlqRecordStatusRequest {
    pub status: DlqStatus,
}

async fn create_dlq_record(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateDlqRecordRequest>,
) -> Result<(StatusCode, Json<DlqRecord>), ApiError> {
    require_scope_for_write(&state, &auth, "/dlq/records", "dlq:write")?;
    let CreateDlqRecordRequest {
        tenant_id: explicit_tenant_id,
        dlq_key,
        source_system,
        source_id,
        connector_id,
        flow_id,
        raw_message_id,
        event_id,
        command_id,
        idempotency_key,
        external_flow_id,
        external_flow_name,
        external_flowfile_uuid,
        external_process_group_id,
        external_processor_id,
        external_provenance_uri,
        sync_session_id,
        payload_format,
        payload,
        payload_hash,
        failure_stage,
        failure_reason,
        failure_detail,
        retry_count,
        replay_count,
        status,
        metadata,
    } = request;
    let tenant_id = dlq_tenant_for_create(&state, &auth, explicit_tenant_id)?;
    let scoped_state = state_for_tenant(&state, tenant_id);
    let record = DlqRecord::new(
        scoped_state.tenant_id,
        dlq_key,
        source_system,
        source_id,
        connector_id,
        flow_id,
        raw_message_id,
        event_id,
        command_id,
        idempotency_key,
        external_flow_id,
        external_flow_name,
        external_flowfile_uuid,
        external_process_group_id,
        external_processor_id,
        external_provenance_uri,
        sync_session_id,
        payload_format,
        payload,
        payload_hash,
        failure_stage,
        failure_reason,
        failure_detail,
        retry_count,
        replay_count,
        status.unwrap_or(DlqStatus::Pending),
        metadata,
        Utc::now(),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let record = scoped_state.storage.create_dlq_record(record)?;
    record_dlq_event(
        &scoped_state,
        "aion:DlqRecordCreated",
        EventSeverity::Warning,
        &record,
        Some("dlq record created".to_string()),
    )?;
    Ok((StatusCode::CREATED, Json(record)))
}

async fn list_dlq_records(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<ListDlqRecordsQuery>,
) -> Result<Json<Vec<DlqRecord>>, ApiError> {
    require_scope(&state, &auth, "/dlq/records", "dlq:read")?;
    let filter = query_to_filter(query);

    let records = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state.storage.list_dlq_records(state.tenant_id, filter)?
    } else if is_admin_all(&auth) {
        state.storage.list_all_dlq_records(filter)?
    } else {
        state
            .storage
            .list_dlq_records(principal_tenant_id(&auth)?, filter)?
    };

    Ok(Json(records))
}

async fn get_dlq_record(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(record_id): Path<Uuid>,
) -> Result<Json<DlqRecord>, ApiError> {
    require_scope(&state, &auth, "/dlq/records/:record_id", "dlq:read")?;

    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        return Ok(Json(
            state
                .storage
                .get_dlq_record(state.tenant_id, record_id)?
                .ok_or_else(ApiError::not_found)?,
        ));
    }

    if is_admin_all(&auth) {
        return Ok(Json(
            state
                .storage
                .get_dlq_record_any_tenant(record_id)?
                .ok_or_else(ApiError::not_found)?,
        ));
    }

    let tenant_id = principal_tenant_id(&auth)?;
    match state.storage.get_dlq_record(tenant_id, record_id)? {
        Some(record) => Ok(Json(record)),
        None => {
            if state
                .storage
                .get_dlq_record_any_tenant(record_id)?
                .is_some()
            {
                Err(ApiError::forbidden(
                    "principal tenant does not own the resource for /dlq/records/:record_id",
                ))
            } else {
                Err(ApiError::not_found())
            }
        }
    }
}

async fn update_dlq_record_status(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(record_id): Path<Uuid>,
    Json(request): Json<UpdateDlqRecordStatusRequest>,
) -> Result<Json<DlqRecord>, ApiError> {
    require_scope_for_write(&state, &auth, "/dlq/records/:record_id/status", "dlq:write")?;
    let record = require_same_tenant_for_target_dlq(
        &state,
        &auth,
        "/dlq/records/:record_id/status",
        record_id,
    )?;
    let scoped_state = state_for_tenant(&state, record.tenant_id);
    let record = scoped_state.storage.update_dlq_record_status(
        scoped_state.tenant_id,
        record.id,
        request.status,
        Utc::now(),
    )?;

    record_dlq_event(
        &scoped_state,
        dlq_status_event_type(&record.status),
        EventSeverity::Info,
        &record,
        Some(format!(
            "dlq record status updated to {}",
            dlq_status_label(&record.status)
        )),
    )?;
    Ok(Json(record))
}

fn dlq_tenant_for_create(
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

fn query_to_filter(query: ListDlqRecordsQuery) -> DlqRecordFilter {
    DlqRecordFilter {
        status: query.status,
        failure_stage: query.failure_stage,
        source_system: query.source_system,
        connector_id: query.connector_id,
        flow_id: query.flow_id,
        raw_message_id: query.raw_message_id,
        idempotency_key: query.idempotency_key,
        external_flowfile_uuid: query.external_flowfile_uuid,
        sync_session_id: query.sync_session_id,
        limit: query
            .limit
            .unwrap_or(DEFAULT_DLQ_LIST_LIMIT)
            .min(MAX_DLQ_LIST_LIMIT),
    }
}

fn require_same_tenant_for_target_dlq(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    record_id: Uuid,
) -> Result<DlqRecord, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        return state
            .storage
            .get_dlq_record(state.tenant_id, record_id)?
            .ok_or_else(ApiError::not_found);
    }

    if is_admin_all(auth) {
        return state
            .storage
            .get_dlq_record_any_tenant(record_id)?
            .ok_or_else(ApiError::not_found);
    }

    let tenant_id = principal_tenant_id(auth)?;
    match state.storage.get_dlq_record(tenant_id, record_id)? {
        Some(record) => Ok(record),
        None => {
            if state
                .storage
                .get_dlq_record_any_tenant(record_id)?
                .is_some()
            {
                Err(ApiError::forbidden(format!(
                    "principal tenant does not own the target dlq record for {endpoint}"
                )))
            } else {
                Err(ApiError::not_found())
            }
        }
    }
}

fn record_dlq_event(
    state: &AppState,
    event_type: &str,
    severity: EventSeverity,
    record: &DlqRecord,
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
            correlation_id: record.idempotency_key.clone(),
            raw_message_id: record.raw_message_id,
            observation_id: None,
            command_id: record.command_id,
            action_id: None,
            action_result_id: None,
            metadata: Some(json!({
                "dlq_record_id": record.id,
                "dlq_key": record.dlq_key,
                "status": record.status,
                "failure_stage": record.failure_stage,
                "failure_reason": record.failure_reason,
                "source_system": record.source_system,
                "source_id": record.source_id,
                "connector_id": record.connector_id,
                "flow_id": record.flow_id,
                "event_id": record.event_id,
                "external_flow_id": record.external_flow_id,
                "external_flow_name": record.external_flow_name,
                "external_flowfile_uuid": record.external_flowfile_uuid,
                "external_process_group_id": record.external_process_group_id,
                "external_processor_id": record.external_processor_id,
                "external_provenance_uri": record.external_provenance_uri,
                "sync_session_id": record.sync_session_id,
                "idempotency_key": record.idempotency_key,
                "retry_count": record.retry_count,
                "replay_count": record.replay_count,
                "payload_hash": record.payload_hash,
            })),
        },
    )?;
    Ok(())
}

fn dlq_status_event_type(status: &DlqStatus) -> &'static str {
    match status {
        DlqStatus::Ignored => "aion:DlqRecordIgnored",
        DlqStatus::ReplayRequested => "aion:DlqReplayRequested",
        _ => "aion:DlqRecordStatusUpdated",
    }
}

fn dlq_status_label(status: &DlqStatus) -> &'static str {
    match status {
        DlqStatus::Pending => "pending",
        DlqStatus::Inspecting => "inspecting",
        DlqStatus::Resolved => "resolved",
        DlqStatus::Ignored => "ignored",
        DlqStatus::ReplayRequested => "replay_requested",
        DlqStatus::FailedReplay => "failed_replay",
    }
}
