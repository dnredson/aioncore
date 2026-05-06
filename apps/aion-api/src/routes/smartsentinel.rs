use crate::{
    auth::{require_scope, AuthContext},
    command_support::{
        claim_command_for_executor, ensure_executor_can_run_command, executor_can_run_command,
        get_command_for_executor_mutation, mark_active_lease_completed, mark_active_lease_failed,
        mutate_command_raw, record_command_event, smartsentinel_command_envelope,
        smartsentinel_report_metadata,
    },
    ensure_entity_exists,
    error::ApiError,
    evaluate_rules_for_event, evaluate_rules_for_observation, get_executor_agent,
    merge_json_object, payload_to_bytes, record_event, record_executor_event,
    record_ingest_event_optional,
    routes::executors::{ExecutorClaimCommandRequest, PutExecutorScopeRequest},
    AppState, EventDraft, SMARTSENTINEL_PAYLOAD_FORMAT,
};
use aion_action::{
    Action, ActionResult, Command, CommandLease, CommandStatus, ExecutorAgent, ExecutorAgentStatus,
    ExecutorCapability, ExecutorScope,
};
use aion_entity::Entity;
use aion_event::{Event, EventSeverity};
use aion_observation::{Observation, ObservationValue};
use aion_raw_message::{RawMessage, RawMessageSource};
use aion_relationship::Relationship;
use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/integrations/smartsentinel/snapshots",
            post(ingest_smartsentinel_snapshot),
        )
        .route(
            "/integrations/smartsentinel/executors/register",
            post(register_smartsentinel_executor),
        )
        .route(
            "/integrations/smartsentinel/executors/:executor_id/commands",
            get(poll_smartsentinel_executor_commands),
        )
        .route(
            "/integrations/smartsentinel/executors/:executor_id/commands/:command_id/claim",
            post(claim_smartsentinel_executor_command),
        )
        .route(
            "/integrations/smartsentinel/executors/:executor_id/commands/:command_id/report",
            post(report_smartsentinel_executor_command),
        )
}

#[derive(Debug, Deserialize)]
struct SmartSentinelSnapshot {
    snapshot_id: String,
    node_id: String,
    observed_at: Option<DateTime<Utc>>,
    source: Option<Value>,
    provenance: Option<Value>,
    #[serde(default)]
    evidence: Vec<Value>,
    #[serde(default)]
    entities: Vec<SmartSentinelSnapshotEntity>,
    #[serde(default)]
    relationships: Vec<SmartSentinelSnapshotRelationship>,
    #[serde(default)]
    observations: Vec<SmartSentinelSnapshotObservation>,
    #[serde(default)]
    events: Vec<SmartSentinelSnapshotEvent>,
}

#[derive(Debug, Deserialize)]
struct SmartSentinelSnapshotEntity {
    id: String,
    #[serde(rename = "type")]
    entity_type: String,
    name: Option<String>,
    status: Option<String>,
    #[serde(default = "crate::empty_object")]
    properties: Value,
}

#[derive(Debug, Deserialize)]
struct SmartSentinelSnapshotRelationship {
    source: String,
    #[serde(rename = "type")]
    relationship_type: String,
    target: String,
}

#[derive(Debug, Deserialize)]
struct SmartSentinelSnapshotObservation {
    entity_id: String,
    observed_property: String,
    value: Value,
    unit: Option<String>,
    observed_at: Option<DateTime<Utc>>,
    evidence_refs: Option<Value>,
    source: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct SmartSentinelSnapshotEvent {
    event_type: String,
    target_entity_id: Option<String>,
    source_entity_id: Option<String>,
    severity: Option<EventSeverity>,
    message: Option<String>,
    occurred_at: Option<DateTime<Utc>>,
    incident_id: Option<String>,
    alert_id: Option<String>,
    workflow_id: Option<String>,
    run_id: Option<String>,
    trace_id: Option<String>,
    evidence_refs: Option<Value>,
}

#[derive(Debug, Serialize)]
struct SmartSentinelSnapshotResponse {
    raw_message_id: Uuid,
    snapshot_id: String,
    node_id: String,
    entities_created: usize,
    entities_updated: usize,
    entities_reused: usize,
    entities_skipped: usize,
    relationships_created: usize,
    relationships_reused: usize,
    relationships_skipped: usize,
    observations_created: usize,
    events_created: usize,
    validation_warnings: Vec<SmartSentinelValidationIssue>,
    validation_errors: Vec<SmartSentinelValidationIssue>,
    skipped_items: Vec<SmartSentinelSkippedItem>,
    provenance_present: bool,
    evidence_count: usize,
    external_ref_count: usize,
    correlation_id: Option<String>,
    trace_id: Option<String>,
    run_id: Option<String>,
    cycle_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SmartSentinelValidationIssue {
    pub(crate) path: String,
    pub(crate) code: String,
    pub(crate) message: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SmartSentinelSkippedItem {
    pub(crate) path: String,
    pub(crate) reason: String,
}

#[derive(Debug, Clone)]
pub(crate) struct SmartSentinelValidationReport {
    pub(crate) warnings: Vec<SmartSentinelValidationIssue>,
    pub(crate) errors: Vec<SmartSentinelValidationIssue>,
    pub(crate) skipped_items: Vec<SmartSentinelSkippedItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SmartSentinelEntityMappingStatus {
    Created,
    Updated,
    Reused,
}

#[derive(Debug, Clone)]
struct SmartSentinelProvenanceSummary {
    provenance_present: bool,
    evidence_count: usize,
    external_ref_count: usize,
    correlation_id: Option<String>,
    trace_id: Option<String>,
    run_id: Option<String>,
    cycle_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RegisterSmartSentinelExecutorRequest {
    agent_key: String,
    display_name: Option<String>,
    metadata: Option<Value>,
    #[serde(default)]
    capabilities: Vec<SmartSentinelExecutorCapabilityRequest>,
    #[serde(default)]
    scopes: Vec<PutExecutorScopeRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum SmartSentinelExecutorCapabilityRequest {
    CommandType(String),
    Detailed {
        command_type: String,
        protocol: Option<String>,
        metadata: Option<Value>,
    },
}

#[derive(Debug, Serialize)]
struct RegisterSmartSentinelExecutorResponse {
    executor: ExecutorAgent,
    reused: bool,
    capabilities: Vec<ExecutorCapability>,
    scopes: Vec<ExecutorScope>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SmartSentinelCommandEnvelope {
    pub(crate) command: Command,
    pub(crate) latest_lease: Option<CommandLease>,
    pub(crate) target_entity: Option<Entity>,
    pub(crate) recent_provenance: Vec<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SmartSentinelCommandReportRequest {
    pub(crate) action_type: String,
    pub(crate) status: String,
    pub(crate) verified: bool,
    pub(crate) result_payload: Value,
    pub(crate) evidence_refs: Option<Value>,
    pub(crate) incident_id: Option<String>,
    pub(crate) alert_id: Option<String>,
    pub(crate) workflow_id: Option<String>,
    pub(crate) run_id: Option<String>,
    pub(crate) trace_id: Option<String>,
    pub(crate) correlation_id: Option<String>,
    pub(crate) message: Option<String>,
    pub(crate) metadata: Option<Value>,
}

#[derive(Debug, Serialize)]
struct SmartSentinelCommandReportResponse {
    command: Command,
    action: Action,
    action_result: ActionResult,
    event: Event,
}

async fn register_smartsentinel_executor(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<RegisterSmartSentinelExecutorRequest>,
) -> Result<(StatusCode, Json<RegisterSmartSentinelExecutorResponse>), ApiError> {
    require_scope(
        &state,
        &auth,
        "/integrations/smartsentinel/executors/register",
        "smartsentinel:executor_register",
    )?;
    let now = Utc::now();
    let existing = state
        .storage
        .list_executors(state.tenant_id)?
        .into_iter()
        .find(|executor| executor.agent_key == request.agent_key);

    let (executor, reused) = if let Some(mut executor) = existing {
        if executor.agent_type != "smartsentinel" {
            return Err(ApiError::bad_request(
                "agent_key is already registered for a non-SmartSentinel executor",
            ));
        }
        executor.display_name = request.display_name;
        executor.metadata = request.metadata;
        executor.heartbeat(ExecutorAgentStatus::Online, now);
        (state.storage.update_executor(executor)?, true)
    } else {
        let executor = ExecutorAgent::new(
            state.tenant_id,
            request.agent_key,
            "smartsentinel",
            request.display_name,
            ExecutorAgentStatus::Online,
            request.metadata,
            now,
        )
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
        (state.storage.create_executor(executor)?, false)
    };

    let capabilities = smart_sentinel_executor_capabilities(executor.id, request.capabilities)?;
    let capabilities =
        state
            .storage
            .put_executor_capabilities(state.tenant_id, executor.id, capabilities)?;
    let scopes = smart_sentinel_executor_scopes(&state, executor.id, request.scopes)?;
    let scopes = state
        .storage
        .put_executor_scopes(state.tenant_id, executor.id, scopes)?;

    record_executor_event(
        &state,
        if reused {
            "aion:SmartSentinelExecutorUpdated"
        } else {
            "aion:SmartSentinelExecutorRegistered"
        },
        &executor,
        None,
        Some(json!({
            "capability_count": capabilities.len(),
            "scope_count": scopes.len(),
            "source": "smartsentinel_bridge"
        })),
    )?;

    Ok((
        if reused {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(RegisterSmartSentinelExecutorResponse {
            executor,
            reused,
            capabilities,
            scopes,
        }),
    ))
}

async fn poll_smartsentinel_executor_commands(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(executor_id): Path<Uuid>,
) -> Result<Json<Vec<SmartSentinelCommandEnvelope>>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/integrations/smartsentinel/executors/:executor_id/commands",
        "smartsentinel:executor_poll",
    )?;
    let executor = get_executor_agent(&state, executor_id)?;
    ensure_smartsentinel_executor(&executor)?;
    let commands = state
        .storage
        .query_commands(state.tenant_id, None, Some(CommandStatus::Pending))?
        .into_iter()
        .filter(|command| executor_can_run_command(&state, executor_id, command).unwrap_or(false))
        .map(|command| smartsentinel_command_envelope(&state, command))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Json(commands))
}

async fn claim_smartsentinel_executor_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((executor_id, command_id)): Path<(Uuid, Uuid)>,
    request: Option<Json<ExecutorClaimCommandRequest>>,
) -> Result<Json<SmartSentinelCommandEnvelope>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/integrations/smartsentinel/executors/:executor_id/commands/:command_id/claim",
        "smartsentinel:executor_claim",
    )?;
    let executor = get_executor_agent(&state, executor_id)?;
    ensure_smartsentinel_executor(&executor)?;
    ensure_executor_can_run_command(&state, executor_id, command_id)?;
    let request = request.map(|Json(request)| request);
    let command = claim_command_for_executor(
        &state,
        command_id,
        &executor,
        request
            .as_ref()
            .and_then(|request| request.lease_duration_seconds),
        request.as_ref().and_then(|request| request.max_retries),
        request
            .and_then(|request| request.metadata)
            .map(|metadata| json!({"source": "smartsentinel_bridge", "metadata": metadata})),
    )?;
    record_executor_event(
        &state,
        "aion:SmartSentinelCommandClaimed",
        &executor,
        Some(&command),
        Some(json!({"source": "smartsentinel_bridge"})),
    )?;

    Ok(Json(smartsentinel_command_envelope(&state, command)?))
}

async fn report_smartsentinel_executor_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((executor_id, command_id)): Path<(Uuid, Uuid)>,
    Json(request): Json<SmartSentinelCommandReportRequest>,
) -> Result<Json<SmartSentinelCommandReportResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/integrations/smartsentinel/executors/:executor_id/commands/:command_id/report",
        "smartsentinel:executor_report",
    )?;
    let executor = get_executor_agent(&state, executor_id)?;
    ensure_smartsentinel_executor(&executor)?;
    let command = get_command_for_executor_mutation(&state, command_id, &executor.agent_key)?;
    let report_status = request.status.trim();
    if !matches!(report_status, "executed" | "failed") {
        return Err(ApiError::bad_request(
            "status must be either executed or failed",
        ));
    }
    if request.action_type.trim().is_empty() {
        return Err(ApiError::bad_request("action_type must not be empty"));
    }

    let now = Utc::now();
    let metadata = smartsentinel_report_metadata(&executor, &request);
    let action = Action::new(
        state.tenant_id,
        command.id,
        None,
        request.action_type.clone(),
        report_status.to_string(),
        command.claimed_at,
        Some(now),
        Some(metadata.clone()),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let action = state.storage.store_action(action)?;
    let action_result = ActionResult::new(
        state.tenant_id,
        command.id,
        action.id,
        report_status.to_string(),
        request.verified,
        request.result_payload.clone(),
        now,
        Some(metadata.clone()),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let action_result = state.storage.store_action_result(action_result)?;

    let command = if report_status == "executed" {
        let command = mutate_command_raw(&state, command_id, |command, now| {
            command.mark_executed(now)
        })?;
        mark_active_lease_completed(&state, command_id, executor_id)?;
        record_command_event(
            &state,
            "aion:CommandExecuted",
            EventSeverity::Info,
            &command,
            request.message.clone(),
        )?;
        command
    } else {
        let failure_reason = request
            .message
            .clone()
            .unwrap_or_else(|| "SmartSentinel executor reported failure".to_string());
        let command = mutate_command_raw(&state, command_id, |command, now| {
            command.mark_failed(failure_reason, now)
        })?;
        mark_active_lease_failed(&state, command_id, executor_id)?;
        record_command_event(
            &state,
            "aion:CommandFailed",
            EventSeverity::Error,
            &command,
            request.message.clone(),
        )?;
        command
    };

    let event = record_event(
        &state,
        EventDraft {
            event_type: "aion:SmartSentinelCommandReported".to_string(),
            severity: if report_status == "executed" {
                EventSeverity::Info
            } else {
                EventSeverity::Error
            },
            source_entity_id: None,
            target_entity_id: Some(command.target_entity_id),
            message: request.message,
            occurred_at: now,
            observed_at: None,
            correlation_id: request.correlation_id,
            raw_message_id: None,
            observation_id: None,
            command_id: Some(command.id),
            action_id: Some(action.id),
            action_result_id: Some(action_result.id),
            metadata: Some(metadata),
        },
    )?;

    Ok(Json(SmartSentinelCommandReportResponse {
        command,
        action,
        action_result,
        event,
    }))
}

async fn ingest_smartsentinel_snapshot(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(payload): Json<Value>,
) -> Result<(StatusCode, Json<SmartSentinelSnapshotResponse>), ApiError> {
    require_scope(
        &state,
        &auth,
        "/integrations/smartsentinel/snapshots",
        "smartsentinel:ingest",
    )?;
    let received_at = Utc::now();
    let snapshot_id = payload
        .get("snapshot_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let node_id = payload
        .get("node_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let raw_snapshot_id = snapshot_id.clone();
    let raw_node_id = node_id.clone();
    let provenance_metadata = smartsentinel_provenance_metadata_from_payload(&payload);
    let provenance_summary = smartsentinel_provenance_summary(&provenance_metadata);
    let mut raw_message = RawMessage::new(
        state.tenant_id,
        RawMessageSource::Http,
        Some("/integrations/smartsentinel/snapshots".to_string()),
        node_id.clone(),
        Some(SMARTSENTINEL_PAYLOAD_FORMAT.to_string()),
        Some("application/json".to_string()),
        None,
        None,
        Some(SMARTSENTINEL_PAYLOAD_FORMAT.to_string()),
        json!({
            "protocol": "http",
            "payload_format": SMARTSENTINEL_PAYLOAD_FORMAT,
            "connector_profile": "smartsentinel",
            "source_endpoint": "/integrations/smartsentinel/snapshots",
            "topic_or_path": "/integrations/smartsentinel/snapshots",
            "snapshot_id": snapshot_id,
            "node_id": node_id,
            "smartsentinel": provenance_metadata,
            "decoder_metadata": {
                "adapter": "SmartSentinelSnapshotDecoder",
                "domain_agnostic": true,
                "actions_executed": false
            }
        }),
        payload_to_bytes(&payload),
        received_at,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    raw_message = state.storage.store_raw_message(raw_message)?;
    let validation = validate_smartsentinel_snapshot(&state, &payload)?;
    record_ingest_event_optional(
        &state,
        "aion:SmartSentinelSnapshotReceived",
        EventSeverity::Info,
        None,
        None,
        Some(raw_message.id),
        Some("SmartSentinel snapshot received".to_string()),
        json!({
            "payload_format": SMARTSENTINEL_PAYLOAD_FORMAT,
            "snapshot_id": raw_snapshot_id,
            "node_id": raw_node_id,
            "source": provenance_metadata.get("source").cloned(),
            "provenance": provenance_metadata.get("provenance").cloned(),
            "evidence_count": provenance_summary.evidence_count,
            "external_ref_count": provenance_summary.external_ref_count,
            "correlation_id": provenance_summary.correlation_id,
            "trace_id": provenance_summary.trace_id,
            "run_id": provenance_summary.run_id,
            "cycle_id": provenance_summary.cycle_id,
            "validation_warning_count": validation.warnings.len(),
            "validation_error_count": validation.errors.len(),
            "skipped_item_count": validation.skipped_items.len()
        }),
    )?;

    if !validation.errors.is_empty() {
        let message = "SmartSentinel snapshot validation failed";
        state
            .storage
            .mark_raw_message_failed(state.tenant_id, raw_message.id, message)?;
        record_ingest_event_optional(
            &state,
            "aion:SmartSentinelSnapshotMappingFailed",
            EventSeverity::Error,
            None,
            None,
            Some(raw_message.id),
            Some(message.to_string()),
            json!({
                "payload_format": SMARTSENTINEL_PAYLOAD_FORMAT,
                "snapshot_id": raw_snapshot_id,
                "node_id": raw_node_id,
                "reason": "validation_failed",
                "source": provenance_metadata.get("source").cloned(),
                "provenance": provenance_metadata.get("provenance").cloned(),
                "evidence_count": provenance_summary.evidence_count,
                "external_ref_count": provenance_summary.external_ref_count,
                "correlation_id": provenance_summary.correlation_id,
                "trace_id": provenance_summary.trace_id,
                "run_id": provenance_summary.run_id,
                "cycle_id": provenance_summary.cycle_id,
                "validation_warning_count": validation.warnings.len(),
                "validation_error_count": validation.errors.len(),
                "skipped_item_count": validation.skipped_items.len()
            }),
        )?;
        return Err(ApiError::smartsentinel_validation(message, validation));
    }

    let snapshot = serde_json::from_value::<SmartSentinelSnapshot>(payload.clone())
        .map_err(|err| ApiError::bad_request(format!("invalid SmartSentinel snapshot: {err}")))?;

    let summary =
        match map_smartsentinel_snapshot(&state, snapshot, raw_message.id, received_at, validation)
        {
            Ok(summary) => summary,
            Err(err) => {
                state.storage.mark_raw_message_failed(
                    state.tenant_id,
                    raw_message.id,
                    &err.message,
                )?;
                record_ingest_event_optional(
                    &state,
                    "aion:SmartSentinelSnapshotMappingFailed",
                    EventSeverity::Error,
                    None,
                    None,
                    Some(raw_message.id),
                    Some(err.message.clone()),
                    json!({
                        "payload_format": SMARTSENTINEL_PAYLOAD_FORMAT,
                        "snapshot_id": raw_snapshot_id,
                        "node_id": raw_node_id,
                        "reason": "mapping_error",
                        "source": provenance_metadata.get("source").cloned(),
                        "provenance": provenance_metadata.get("provenance").cloned(),
                        "evidence_count": provenance_summary.evidence_count,
                        "external_ref_count": provenance_summary.external_ref_count,
                        "correlation_id": provenance_summary.correlation_id,
                        "trace_id": provenance_summary.trace_id,
                        "run_id": provenance_summary.run_id,
                        "cycle_id": provenance_summary.cycle_id
                    }),
                )?;
                return Err(err);
            }
        };
    state
        .storage
        .mark_raw_message_normalized(state.tenant_id, raw_message.id)?;
    record_ingest_event_optional(
        &state,
        "aion:SmartSentinelSnapshotMapped",
        EventSeverity::Info,
        None,
        None,
        Some(raw_message.id),
        Some("SmartSentinel snapshot mapped".to_string()),
        json!({
            "payload_format": SMARTSENTINEL_PAYLOAD_FORMAT,
            "snapshot_id": summary.snapshot_id,
            "node_id": summary.node_id,
            "source": provenance_metadata.get("source").cloned(),
            "provenance": provenance_metadata.get("provenance").cloned(),
            "entities_created": summary.entities_created,
            "entities_updated": summary.entities_updated,
            "entities_reused": summary.entities_reused,
            "entities_skipped": summary.entities_skipped,
            "relationships_created": summary.relationships_created,
            "relationships_reused": summary.relationships_reused,
            "relationships_skipped": summary.relationships_skipped,
            "observations_created": summary.observations_created,
            "events_created": summary.events_created,
            "provenance_present": summary.provenance_present,
            "evidence_count": summary.evidence_count,
            "external_ref_count": summary.external_ref_count,
            "correlation_id": summary.correlation_id,
            "trace_id": summary.trace_id,
            "run_id": summary.run_id,
            "cycle_id": summary.cycle_id,
            "validation_warning_count": summary.validation_warnings.len(),
            "validation_error_count": summary.validation_errors.len(),
            "skipped_item_count": summary.skipped_items.len()
        }),
    )?;

    Ok((StatusCode::CREATED, Json(summary)))
}

fn smartsentinel_provenance_metadata_from_payload(payload: &Value) -> Value {
    json!({
        "source": payload.get("source").cloned(),
        "provenance": payload.get("provenance").cloned(),
        "evidence": payload.get("evidence").cloned().unwrap_or_else(|| json!([]))
    })
}

fn smartsentinel_provenance_metadata(snapshot: &SmartSentinelSnapshot) -> Value {
    json!({
        "source": snapshot.source,
        "provenance": snapshot.provenance,
        "evidence": snapshot.evidence
    })
}

fn smartsentinel_provenance_summary(metadata: &Value) -> SmartSentinelProvenanceSummary {
    let provenance = metadata.get("provenance").filter(|value| value.is_object());
    SmartSentinelProvenanceSummary {
        provenance_present: provenance.is_some(),
        evidence_count: metadata
            .get("evidence")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter(|value| smartsentinel_evidence_reference_is_usable(value))
                    .count()
            })
            .unwrap_or(0),
        external_ref_count: provenance
            .and_then(|value| value.get("external_refs"))
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0),
        correlation_id: provenance
            .and_then(|value| optional_trimmed_string(value, "correlation_id")),
        trace_id: provenance.and_then(|value| optional_trimmed_string(value, "trace_id")),
        run_id: provenance.and_then(|value| optional_trimmed_string(value, "run_id")),
        cycle_id: provenance.and_then(|value| optional_trimmed_string(value, "cycle_id")),
    }
}

fn optional_trimmed_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn smartsentinel_evidence_reference_is_usable(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    object
        .get("uri")
        .map(|value| value.is_string())
        .unwrap_or(true)
}

fn smartsentinel_base_metadata(
    snapshot_id: &str,
    node_id: &str,
    provenance_metadata: &Value,
) -> Value {
    let summary = smartsentinel_provenance_summary(provenance_metadata);
    json!({
        "adapter": "SmartSentinelSnapshotDecoder",
        "snapshot_id": snapshot_id,
        "node_id": node_id,
        "source": provenance_metadata.get("source").cloned(),
        "provenance": provenance_metadata.get("provenance").cloned(),
        "evidence": provenance_metadata.get("evidence").cloned().unwrap_or_else(|| json!([])),
        "evidence_count": summary.evidence_count,
        "external_ref_count": summary.external_ref_count,
        "correlation_id": summary.correlation_id,
        "trace_id": summary.trace_id,
        "run_id": summary.run_id,
        "cycle_id": summary.cycle_id,
        "uri_fetch_attempted": false
    })
}

fn validate_smartsentinel_snapshot(
    state: &AppState,
    payload: &Value,
) -> Result<SmartSentinelValidationReport, ApiError> {
    let mut report = SmartSentinelValidationReport {
        warnings: Vec::new(),
        errors: Vec::new(),
        skipped_items: Vec::new(),
    };

    let Some(object) = payload.as_object() else {
        report.errors.push(smartsentinel_issue(
            "$",
            "snapshot_not_object",
            "SmartSentinel snapshot must be a JSON object",
        ));
        return Ok(report);
    };

    let node_id = required_string(object.get("node_id"), "$.node_id", "node_id", &mut report);
    required_string(
        object.get("snapshot_id"),
        "$.snapshot_id",
        "snapshot_id",
        &mut report,
    );
    if let Some(value) = object.get("observed_at") {
        validate_optional_rfc3339(value, "$.observed_at", "observed_at", &mut report);
    }

    let mut snapshot_entity_ids = HashSet::new();
    if let Some(entities) = object.get("entities") {
        match entities.as_array() {
            Some(entities) => {
                for (index, entity) in entities.iter().enumerate() {
                    let path = format!("$.entities[{index}]");
                    let Some(entity_object) = entity.as_object() else {
                        report.errors.push(smartsentinel_issue(
                            path,
                            "entity_not_object",
                            "SmartSentinel entity must be a JSON object",
                        ));
                        continue;
                    };
                    if let Some(entity_id) = required_string(
                        entity_object.get("id"),
                        format!("{path}.id"),
                        "entity id",
                        &mut report,
                    ) {
                        snapshot_entity_ids.insert(entity_id);
                    }
                    required_string(
                        entity_object.get("type"),
                        format!("{path}.type"),
                        "entity type",
                        &mut report,
                    );
                    if let Some(properties) = entity_object.get("properties") {
                        if !properties.is_object() {
                            report.errors.push(smartsentinel_issue(
                                format!("{path}.properties"),
                                "entity_properties_not_object",
                                "SmartSentinel entity properties must be a JSON object when present",
                            ));
                        }
                    }
                }
            }
            None => report.errors.push(smartsentinel_issue(
                "$.entities",
                "entities_not_array",
                "entities must be an array when present",
            )),
        }
    }

    let node_id = node_id.unwrap_or_default();
    let relationships = object.get("relationships").and_then(Value::as_array);
    if object.get("relationships").is_some() && relationships.is_none() {
        report.errors.push(smartsentinel_issue(
            "$.relationships",
            "relationships_not_array",
            "relationships must be an array when present",
        ));
    }
    if let Some(relationships) = relationships {
        for (index, relationship) in relationships.iter().enumerate() {
            let path = format!("$.relationships[{index}]");
            let Some(relationship_object) = relationship.as_object() else {
                report.errors.push(smartsentinel_issue(
                    path,
                    "relationship_not_object",
                    "SmartSentinel relationship must be a JSON object",
                ));
                continue;
            };
            let source = required_string(
                relationship_object.get("source"),
                format!("{path}.source"),
                "relationship source",
                &mut report,
            );
            let relationship_type = required_string(
                relationship_object.get("type"),
                format!("{path}.type"),
                "relationship type",
                &mut report,
            );
            let target = required_string(
                relationship_object.get("target"),
                format!("{path}.target"),
                "relationship target",
                &mut report,
            );
            if let (Some(source), Some(target)) = (source.as_deref(), target.as_deref()) {
                if source == target {
                    report.warnings.push(smartsentinel_issue(
                        path.clone(),
                        "relationship_self_reference",
                        "relationship source and target are the same; item will be skipped",
                    ));
                    report.skipped_items.push(SmartSentinelSkippedItem {
                        path: path.clone(),
                        reason: "relationship_self_reference".to_string(),
                    });
                }
                if !smartsentinel_entity_ref_resolves(
                    state,
                    &node_id,
                    &snapshot_entity_ids,
                    source,
                )? {
                    report.errors.push(smartsentinel_issue(
                        format!("{path}.source"),
                        "relationship_source_unknown",
                        "relationship source does not reference a snapshot entity or existing mapped entity",
                    ));
                }
                if !smartsentinel_entity_ref_resolves(
                    state,
                    &node_id,
                    &snapshot_entity_ids,
                    target,
                )? {
                    report.errors.push(smartsentinel_issue(
                        format!("{path}.target"),
                        "relationship_target_unknown",
                        "relationship target does not reference a snapshot entity or existing mapped entity",
                    ));
                }
            }
            if relationship_type.is_none() {
                report.skipped_items.push(SmartSentinelSkippedItem {
                    path,
                    reason: "relationship_missing_type".to_string(),
                });
            }
        }
    }

    validate_smartsentinel_observation_items(
        state,
        object.get("observations"),
        &node_id,
        &snapshot_entity_ids,
        &mut report,
    )?;
    validate_smartsentinel_event_items(
        state,
        object.get("events"),
        &node_id,
        &snapshot_entity_ids,
        &mut report,
    )?;
    validate_smartsentinel_evidence_items(object.get("evidence"), &mut report);

    Ok(report)
}

fn validate_smartsentinel_evidence_items(
    value: Option<&Value>,
    report: &mut SmartSentinelValidationReport,
) {
    let Some(value) = value else {
        return;
    };
    let Some(evidence) = value.as_array() else {
        report.warnings.push(smartsentinel_issue(
            "$.evidence",
            "evidence_not_array",
            "evidence must be an array when present; evidence references will be ignored",
        ));
        report.skipped_items.push(SmartSentinelSkippedItem {
            path: "$.evidence".to_string(),
            reason: "evidence_not_array".to_string(),
        });
        return;
    };

    for (index, evidence) in evidence.iter().enumerate() {
        let path = format!("$.evidence[{index}]");
        let Some(object) = evidence.as_object() else {
            report.warnings.push(smartsentinel_issue(
                path.clone(),
                "evidence_not_object",
                "evidence entry must be a JSON object; item will be skipped",
            ));
            report.skipped_items.push(SmartSentinelSkippedItem {
                path,
                reason: "evidence_not_object".to_string(),
            });
            continue;
        };

        if object
            .get("evidence_type")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
        {
            report.warnings.push(smartsentinel_issue(
                format!("{path}.evidence_type"),
                "evidence_type_defaulted",
                "evidence_type is missing; it will be interpreted as custom",
            ));
        }

        if let Some(uri) = object.get("uri") {
            if !uri.is_string() {
                report.warnings.push(smartsentinel_issue(
                    format!("{path}.uri"),
                    "evidence_uri_invalid",
                    "evidence uri must be a string when present; item will be skipped",
                ));
                report.skipped_items.push(SmartSentinelSkippedItem {
                    path: path.clone(),
                    reason: "evidence_uri_invalid".to_string(),
                });
            }
        }

        if let Some(collected_at) = object.get("collected_at") {
            let before = report.errors.len();
            validate_optional_rfc3339(
                collected_at,
                format!("{path}.collected_at"),
                "collected_at",
                report,
            );
            if report.errors.len() > before {
                if let Some(issue) = report.errors.pop() {
                    report.warnings.push(issue);
                }
            }
        }
    }
}

fn validate_smartsentinel_observation_items(
    state: &AppState,
    value: Option<&Value>,
    node_id: &str,
    snapshot_entity_ids: &HashSet<String>,
    report: &mut SmartSentinelValidationReport,
) -> Result<(), ApiError> {
    let Some(value) = value else {
        return Ok(());
    };
    let Some(observations) = value.as_array() else {
        report.errors.push(smartsentinel_issue(
            "$.observations",
            "observations_not_array",
            "observations must be an array when present",
        ));
        return Ok(());
    };
    for (index, observation) in observations.iter().enumerate() {
        let path = format!("$.observations[{index}]");
        let Some(observation_object) = observation.as_object() else {
            report.errors.push(smartsentinel_issue(
                path,
                "observation_not_object",
                "SmartSentinel observation must be a JSON object",
            ));
            continue;
        };
        let entity_id = required_string(
            observation_object.get("entity_id"),
            format!("{path}.entity_id"),
            "observation entity_id",
            report,
        );
        required_string(
            observation_object.get("observed_property"),
            format!("{path}.observed_property"),
            "observation observed_property",
            report,
        );
        if !observation_object.contains_key("value") {
            report.errors.push(smartsentinel_issue(
                format!("{path}.value"),
                "observation_value_missing",
                "observation value is required",
            ));
        }
        if let Some(value) = observation_object.get("observed_at") {
            validate_optional_rfc3339(value, format!("{path}.observed_at"), "observed_at", report);
        }
        if let Some(entity_id) = entity_id {
            if !smartsentinel_entity_ref_resolves(state, node_id, snapshot_entity_ids, &entity_id)?
            {
                report.errors.push(smartsentinel_issue(
                    format!("{path}.entity_id"),
                    "observation_entity_unknown",
                    "observation entity_id does not reference a snapshot entity or existing mapped entity",
                ));
            }
        }
    }
    Ok(())
}

fn validate_smartsentinel_event_items(
    state: &AppState,
    value: Option<&Value>,
    node_id: &str,
    snapshot_entity_ids: &HashSet<String>,
    report: &mut SmartSentinelValidationReport,
) -> Result<(), ApiError> {
    let Some(value) = value else {
        return Ok(());
    };
    let Some(events) = value.as_array() else {
        report.errors.push(smartsentinel_issue(
            "$.events",
            "events_not_array",
            "events must be an array when present",
        ));
        return Ok(());
    };
    for (index, event) in events.iter().enumerate() {
        let path = format!("$.events[{index}]");
        let Some(event_object) = event.as_object() else {
            report.errors.push(smartsentinel_issue(
                path,
                "event_not_object",
                "SmartSentinel event must be a JSON object",
            ));
            continue;
        };
        required_string(
            event_object.get("event_type"),
            format!("{path}.event_type"),
            "event_type",
            report,
        );
        if let Some(severity) = event_object.get("severity") {
            match severity.as_str() {
                Some("debug" | "info" | "warning" | "error" | "critical") => {}
                _ => report.errors.push(smartsentinel_issue(
                    format!("{path}.severity"),
                    "event_severity_invalid",
                    "event severity must be debug, info, warning, error, or critical",
                )),
            }
        }
        for field_name in ["source_entity_id", "target_entity_id"] {
            if let Some(value) = event_object.get(field_name) {
                match value.as_str().map(str::trim).filter(|value| !value.is_empty()) {
                    Some(entity_id)
                        if smartsentinel_entity_ref_resolves(
                            state,
                            node_id,
                            snapshot_entity_ids,
                            entity_id,
                        )? => {}
                    Some(_) => report.errors.push(smartsentinel_issue(
                        format!("{path}.{field_name}"),
                        "event_entity_unknown",
                        "event entity reference does not reference a snapshot entity or existing mapped entity",
                    )),
                    None => report.errors.push(smartsentinel_issue(
                        format!("{path}.{field_name}"),
                        "event_entity_invalid",
                        "event entity reference must be a non-empty string when present",
                    )),
                }
            }
        }
        if let Some(value) = event_object.get("occurred_at") {
            validate_optional_rfc3339(value, format!("{path}.occurred_at"), "occurred_at", report);
        }
    }
    Ok(())
}

fn required_string(
    value: Option<&Value>,
    path: impl Into<String>,
    label: &str,
    report: &mut SmartSentinelValidationReport,
) -> Option<String> {
    let path = path.into();
    match value.and_then(Value::as_str).map(str::trim) {
        Some(value) if !value.is_empty() => Some(value.to_string()),
        _ => {
            report.errors.push(smartsentinel_issue(
                path,
                format!("{}_missing", label.replace(' ', "_")),
                format!("{label} is required"),
            ));
            None
        }
    }
}

fn validate_optional_rfc3339(
    value: &Value,
    path: impl Into<String>,
    label: &str,
    report: &mut SmartSentinelValidationReport,
) {
    if value
        .as_str()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .is_none()
    {
        report.errors.push(smartsentinel_issue(
            path,
            format!("{label}_invalid"),
            format!("{label} must be an RFC3339 timestamp when present"),
        ));
    }
}

fn smartsentinel_entity_ref_resolves(
    state: &AppState,
    node_id: &str,
    snapshot_entity_ids: &HashSet<String>,
    snapshot_entity_id: &str,
) -> Result<bool, ApiError> {
    if snapshot_entity_ids.contains(snapshot_entity_id) {
        return Ok(true);
    }
    let entity_key = smartsentinel_entity_key(node_id, snapshot_entity_id);
    Ok(state
        .storage
        .get_entity_by_key(state.tenant_id, &entity_key)?
        .is_some())
}

fn smartsentinel_issue(
    path: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
) -> SmartSentinelValidationIssue {
    SmartSentinelValidationIssue {
        path: path.into(),
        code: code.into(),
        message: message.into(),
    }
}

fn map_smartsentinel_snapshot(
    state: &AppState,
    snapshot: SmartSentinelSnapshot,
    raw_message_id: Uuid,
    received_at: DateTime<Utc>,
    validation: SmartSentinelValidationReport,
) -> Result<SmartSentinelSnapshotResponse, ApiError> {
    let snapshot_id = snapshot.snapshot_id.clone();
    let node_id = snapshot.node_id.clone();
    let observed_at = snapshot.observed_at.unwrap_or(received_at);
    let provenance_metadata = smartsentinel_provenance_metadata(&snapshot);
    let provenance_summary = smartsentinel_provenance_summary(&provenance_metadata);
    let mut entity_ids = HashMap::new();
    let mut entities_created = 0;
    let mut entities_updated = 0;
    let mut entities_reused = 0;
    let entities_skipped = 0;
    let mut relationships_created = 0;
    let mut relationships_reused = 0;
    let mut relationships_skipped = 0;
    let mut observations_created = 0;
    let mut events_created = 0;
    let observer_raw_id = format!("host:{node_id}");

    for snapshot_entity in &snapshot.entities {
        let entity_key = smartsentinel_entity_key(&node_id, &snapshot_entity.id);
        let (entity, status) = upsert_smartsentinel_entity(
            state,
            &entity_key,
            &node_id,
            &snapshot_id,
            &snapshot,
            snapshot_entity,
            received_at,
        )?;
        entity_ids.insert(snapshot_entity.id.clone(), entity.id);
        match status {
            SmartSentinelEntityMappingStatus::Created => entities_created += 1,
            SmartSentinelEntityMappingStatus::Updated => entities_updated += 1,
            SmartSentinelEntityMappingStatus::Reused => entities_reused += 1,
        }
    }

    let observer_entity_id = entity_ids.get(&observer_raw_id).copied().or_else(|| {
        snapshot
            .entities
            .first()
            .and_then(|entity| entity_ids.get(&entity.id).copied())
    });

    for relationship in &snapshot.relationships {
        let source_entity_id = resolve_smartsentinel_mapped_entity_id(
            state,
            &node_id,
            &entity_ids,
            &relationship.source,
        )?;
        let target_entity_id = resolve_smartsentinel_mapped_entity_id(
            state,
            &node_id,
            &entity_ids,
            &relationship.target,
        )?;
        if relationship.relationship_type.trim().is_empty() || source_entity_id == target_entity_id
        {
            relationships_skipped += 1;
            continue;
        }
        if smartsentinel_relationship_exists(
            state,
            source_entity_id,
            &relationship.relationship_type,
            target_entity_id,
        )? {
            relationships_reused += 1;
        } else {
            let relationship = Relationship::new(
                state.tenant_id,
                source_entity_id,
                relationship.relationship_type.clone(),
                target_entity_id,
                json!({
                    "@context": smartsentinel_jsonld_context(),
                    "@type": "aion:Relationship",
                    "sentinel:snapshotId": snapshot_id,
                    "sentinel:nodeId": node_id,
                    "sentinel:source": relationship.source,
                    "sentinel:target": relationship.target
                }),
                received_at,
            )
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
            state.storage.create_relationship(relationship)?;
            relationships_created += 1;
        }
    }

    for snapshot_entity in &snapshot.entities {
        if let Some(status) = snapshot_entity
            .status
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let feature_of_interest_id = entity_ids[&snapshot_entity.id];
            let producer_entity_id = observer_entity_id.unwrap_or(feature_of_interest_id);
            let observation = Observation::new(
                state.tenant_id,
                producer_entity_id,
                feature_of_interest_id,
                format!("{}Status", snapshot_entity.entity_type),
                ObservationValue::Text {
                    value: status.to_string(),
                },
                None,
                observed_at,
                received_at,
                "http",
                SMARTSENTINEL_PAYLOAD_FORMAT,
                Some(raw_message_id),
                json!({"source": "smartsentinel"}),
                {
                    let mut metadata =
                        smartsentinel_base_metadata(&snapshot_id, &node_id, &provenance_metadata);
                    merge_json_object(
                        &mut metadata,
                        json!({
                            "source": "entity_status",
                            "snapshot_entity_id": snapshot_entity.id
                        }),
                    );
                    metadata
                },
            )
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
            let observation = state.storage.store_observation(observation)?;
            evaluate_rules_for_observation(state, &observation, true)?;
            observations_created += 1;
        }
    }

    for snapshot_observation in &snapshot.observations {
        let feature_of_interest_id = resolve_smartsentinel_mapped_entity_id(
            state,
            &node_id,
            &entity_ids,
            &snapshot_observation.entity_id,
        )?;
        let producer_entity_id = observer_entity_id.unwrap_or(feature_of_interest_id);
        let observation = Observation::new(
            state.tenant_id,
            producer_entity_id,
            feature_of_interest_id,
            snapshot_observation.observed_property.clone(),
            observation_value_from_json(&snapshot_observation.value),
            snapshot_observation.unit.clone(),
            snapshot_observation.observed_at.unwrap_or(observed_at),
            received_at,
            "http",
            SMARTSENTINEL_PAYLOAD_FORMAT,
            Some(raw_message_id),
            json!({"source": "smartsentinel"}),
            {
                let mut metadata =
                    smartsentinel_base_metadata(&snapshot_id, &node_id, &provenance_metadata);
                merge_json_object(
                    &mut metadata,
                    json!({
                        "source": "snapshot_observation",
                        "snapshot_entity_id": snapshot_observation.entity_id,
                        "observation_source": snapshot_observation.source,
                        "evidence_refs": snapshot_observation.evidence_refs
                    }),
                );
                metadata
            },
        )
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
        let observation = state.storage.store_observation(observation)?;
        evaluate_rules_for_observation(state, &observation, true)?;
        observations_created += 1;
    }

    for snapshot_event in &snapshot.events {
        let source_entity_id = snapshot_event
            .source_entity_id
            .as_deref()
            .map(|entity_id| {
                resolve_smartsentinel_mapped_entity_id(state, &node_id, &entity_ids, entity_id)
            })
            .transpose()?;
        let target_entity_id = snapshot_event
            .target_entity_id
            .as_deref()
            .map(|entity_id| {
                resolve_smartsentinel_mapped_entity_id(state, &node_id, &entity_ids, entity_id)
            })
            .transpose()?;
        let event = Event::new(
            state.tenant_id,
            snapshot_event.event_type.clone(),
            snapshot_event
                .severity
                .clone()
                .unwrap_or(EventSeverity::Info),
            source_entity_id,
            target_entity_id,
            snapshot_event.message.clone(),
            snapshot_event.occurred_at.unwrap_or(observed_at),
            Some(observed_at),
            Some(snapshot_id.clone()),
            Some(raw_message_id),
            None,
            None,
            None,
            None,
            Some(smartsentinel_event_metadata(
                &snapshot_id,
                &node_id,
                &provenance_metadata,
                snapshot_event,
            )),
            received_at,
        )
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
        let event = state.storage.store_event(event)?;
        evaluate_rules_for_event(state, &event, true)?;
        events_created += 1;
    }

    Ok(SmartSentinelSnapshotResponse {
        raw_message_id,
        snapshot_id,
        node_id,
        entities_created,
        entities_updated,
        entities_reused,
        entities_skipped,
        relationships_created,
        relationships_reused,
        relationships_skipped,
        observations_created,
        events_created,
        validation_warnings: validation.warnings,
        validation_errors: validation.errors,
        skipped_items: validation.skipped_items,
        provenance_present: provenance_summary.provenance_present,
        evidence_count: provenance_summary.evidence_count,
        external_ref_count: provenance_summary.external_ref_count,
        correlation_id: provenance_summary.correlation_id,
        trace_id: provenance_summary.trace_id,
        run_id: provenance_summary.run_id,
        cycle_id: provenance_summary.cycle_id,
    })
}

fn upsert_smartsentinel_entity(
    state: &AppState,
    entity_key: &str,
    node_id: &str,
    snapshot_id: &str,
    snapshot: &SmartSentinelSnapshot,
    snapshot_entity: &SmartSentinelSnapshotEntity,
    now: DateTime<Utc>,
) -> Result<(Entity, SmartSentinelEntityMappingStatus), ApiError> {
    let jsonld =
        smartsentinel_entity_jsonld(entity_key, node_id, snapshot_id, snapshot, snapshot_entity);
    if let Some(mut entity) = state
        .storage
        .get_entity_by_key(state.tenant_id, entity_key)?
    {
        let new_entity_type = snapshot_entity.entity_type.clone();
        let unchanged = entity.entity_type == new_entity_type && entity.jsonld == jsonld;
        if unchanged {
            return Ok((entity, SmartSentinelEntityMappingStatus::Reused));
        }
        entity.entity_type = new_entity_type;
        entity.jsonld = jsonld;
        entity.updated_at = now;
        let entity = state.storage.update_entity(entity)?;
        return Ok((entity, SmartSentinelEntityMappingStatus::Updated));
    }

    let entity = Entity::new(
        state.tenant_id,
        entity_key,
        snapshot_entity.entity_type.clone(),
        jsonld,
        now,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    Ok((
        state.storage.create_entity(entity)?,
        SmartSentinelEntityMappingStatus::Created,
    ))
}

fn smartsentinel_entity_jsonld(
    entity_key: &str,
    node_id: &str,
    snapshot_id: &str,
    snapshot: &SmartSentinelSnapshot,
    snapshot_entity: &SmartSentinelSnapshotEntity,
) -> Value {
    let related_evidence = snapshot
        .evidence
        .iter()
        .filter(|evidence| {
            evidence
                .get("related_entity_id")
                .and_then(Value::as_str)
                .map(|entity_id| entity_id == snapshot_entity.id)
                .unwrap_or(false)
                && smartsentinel_evidence_reference_is_usable(evidence)
        })
        .cloned()
        .collect::<Vec<_>>();
    json!({
        "@context": smartsentinel_jsonld_context(),
        "@id": format!("urn:aion:smartsentinel:{node_id}:{}", snapshot_entity.id),
        "@type": snapshot_entity.entity_type,
        "entity_key": entity_key,
        "name": snapshot_entity.name,
        "sentinel:externalId": snapshot_entity.id,
        "sentinel:nodeId": node_id,
        "sentinel:snapshotId": snapshot_id,
        "sentinel:status": snapshot_entity.status,
        "sentinel:properties": snapshot_entity.properties,
        "sentinel:evidence": related_evidence
    })
}

fn smartsentinel_event_metadata(
    snapshot_id: &str,
    node_id: &str,
    provenance_metadata: &Value,
    snapshot_event: &SmartSentinelSnapshotEvent,
) -> Value {
    let mut metadata = smartsentinel_base_metadata(snapshot_id, node_id, provenance_metadata);
    merge_json_object(
        &mut metadata,
        json!({
            "source": "snapshot_event",
            "incident_id": snapshot_event.incident_id,
            "alert_id": snapshot_event.alert_id,
            "workflow_id": snapshot_event.workflow_id,
            "run_id": snapshot_event.run_id,
            "trace_id": snapshot_event.trace_id,
            "evidence_refs": snapshot_event.evidence_refs
        }),
    );
    metadata
}

fn smartsentinel_relationship_exists(
    state: &AppState,
    source_entity_id: Uuid,
    relationship_type: &str,
    target_entity_id: Uuid,
) -> Result<bool, ApiError> {
    Ok(state
        .storage
        .list_relationships(
            state.tenant_id,
            Some(source_entity_id),
            Some(target_entity_id),
        )?
        .into_iter()
        .any(|relationship| relationship.relationship_type == relationship_type))
}

fn resolve_smartsentinel_mapped_entity_id(
    state: &AppState,
    node_id: &str,
    entity_ids: &HashMap<String, Uuid>,
    snapshot_entity_id: &str,
) -> Result<Uuid, ApiError> {
    if let Some(entity_id) = entity_ids.get(snapshot_entity_id).copied() {
        return Ok(entity_id);
    }
    let entity_key = smartsentinel_entity_key(node_id, snapshot_entity_id);
    state
        .storage
        .get_entity_by_key(state.tenant_id, &entity_key)?
        .map(|entity| entity.id)
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "SmartSentinel entity reference '{}' does not reference a mapped entity",
                snapshot_entity_id
            ))
        })
}

fn smartsentinel_entity_key(node_id: &str, snapshot_entity_id: &str) -> String {
    format!("smartsentinel:{node_id}:{snapshot_entity_id}")
}

fn smartsentinel_jsonld_context() -> Value {
    json!({
        "aion": "https://aioncore.org/ns#",
        "sentinel": "https://aioncore.org/ns/smartsentinel#"
    })
}

fn observation_value_from_json(value: &Value) -> ObservationValue {
    if let Some(value) = value.as_f64() {
        ObservationValue::Number { value }
    } else if let Some(value) = value.as_str() {
        ObservationValue::Text {
            value: value.to_string(),
        }
    } else if let Some(value) = value.as_bool() {
        ObservationValue::Bool { value }
    } else {
        ObservationValue::Json {
            value: value.clone(),
        }
    }
}

fn ensure_smartsentinel_executor(executor: &ExecutorAgent) -> Result<(), ApiError> {
    if executor.agent_type != "smartsentinel" {
        return Err(ApiError::bad_request(
            "executor is not registered as a SmartSentinel executor",
        ));
    }
    Ok(())
}

fn smart_sentinel_executor_capabilities(
    executor_id: Uuid,
    requests: Vec<SmartSentinelExecutorCapabilityRequest>,
) -> Result<Vec<ExecutorCapability>, ApiError> {
    requests
        .into_iter()
        .map(|request| match request {
            SmartSentinelExecutorCapabilityRequest::CommandType(command_type) => {
                ExecutorCapability::new(
                    executor_id,
                    command_type,
                    Some("smartsentinel".to_string()),
                    Some(json!({"source": "smartsentinel_bridge"})),
                )
            }
            SmartSentinelExecutorCapabilityRequest::Detailed {
                command_type,
                protocol,
                metadata,
            } => ExecutorCapability::new(
                executor_id,
                command_type,
                protocol.or_else(|| Some("smartsentinel".to_string())),
                metadata,
            ),
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| ApiError::bad_request(err.to_string()))
}

fn smart_sentinel_executor_scopes(
    state: &AppState,
    executor_id: Uuid,
    requests: Vec<PutExecutorScopeRequest>,
) -> Result<Vec<ExecutorScope>, ApiError> {
    let mut scopes = Vec::with_capacity(requests.len());
    for request in requests {
        if let Some(target_entity_id) = request.target_entity_id {
            ensure_entity_exists(state, target_entity_id)?;
        }
        scopes.push(ExecutorScope::new(
            executor_id,
            request.target_entity_id,
            request.entity_type,
            request.relationship_type,
            request.metadata,
        ));
    }
    Ok(scopes)
}
