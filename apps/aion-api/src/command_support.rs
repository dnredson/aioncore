use crate::error::ApiError;
use crate::{
    insert_optional_string, record_event,
    routes::smartsentinel::{SmartSentinelCommandEnvelope, SmartSentinelCommandReportRequest},
    AppState, EventDraft, EventFilter, DEFAULT_COMMAND_LEASE_SECONDS,
};
use aion_action::{Command, CommandLease, ExecutorAgent, ExecutorScope};
use aion_event::{Event, EventSeverity};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use uuid::Uuid;

pub(crate) fn smartsentinel_command_envelope(
    state: &AppState,
    command: Command,
) -> Result<SmartSentinelCommandEnvelope, ApiError> {
    let latest_lease = state
        .storage
        .get_latest_command_lease(state.tenant_id, command.id)?;
    let target_entity = state
        .storage
        .get_entity(state.tenant_id, command.target_entity_id)?;
    let recent_provenance = state
        .storage
        .query_events(
            state.tenant_id,
            EventFilter {
                target_entity_id: Some(command.target_entity_id),
                ..Default::default()
            },
        )?
        .into_iter()
        .filter_map(|event| event.metadata)
        .filter(smartsentinel_metadata_has_provenance)
        .take(5)
        .collect();

    Ok(SmartSentinelCommandEnvelope {
        command,
        latest_lease,
        target_entity,
        recent_provenance,
    })
}

fn smartsentinel_metadata_has_provenance(metadata: &Value) -> bool {
    metadata.get("smartsentinel").is_some()
        || metadata.get("evidence_refs").is_some()
        || metadata.get("incident_id").is_some()
        || metadata.get("alert_id").is_some()
        || metadata.get("workflow_id").is_some()
        || metadata.get("run_id").is_some()
        || metadata.get("trace_id").is_some()
        || metadata
            .get("provenance")
            .map(|provenance| {
                provenance.get("run_id").is_some()
                    || provenance.get("trace_id").is_some()
                    || provenance.get("workflow_id").is_some()
            })
            .unwrap_or(false)
}

pub(crate) fn smartsentinel_report_metadata(
    executor: &ExecutorAgent,
    request: &SmartSentinelCommandReportRequest,
) -> Value {
    let mut metadata = json!({
        "source": "smartsentinel_bridge",
        "executor_id": executor.id,
        "agent_key": executor.agent_key,
        "agent_type": executor.agent_type,
        "status": request.status.as_str(),
        "verified": request.verified
    });

    if let Some(object) = metadata.as_object_mut() {
        insert_optional_string(object, "incident_id", request.incident_id.as_deref());
        insert_optional_string(object, "alert_id", request.alert_id.as_deref());
        insert_optional_string(object, "workflow_id", request.workflow_id.as_deref());
        insert_optional_string(object, "run_id", request.run_id.as_deref());
        insert_optional_string(object, "trace_id", request.trace_id.as_deref());
        insert_optional_string(object, "correlation_id", request.correlation_id.as_deref());
        if let Some(evidence_refs) = &request.evidence_refs {
            object.insert("evidence_refs".to_string(), evidence_refs.clone());
        }
        if let Some(extra) = &request.metadata {
            object.insert("metadata".to_string(), extra.clone());
        }
    }

    metadata
}

pub(crate) fn ensure_executor_can_run_command(
    state: &AppState,
    executor_id: Uuid,
    command_id: Uuid,
) -> Result<Command, ApiError> {
    let command = state
        .storage
        .get_command(state.tenant_id, command_id)?
        .ok_or_else(ApiError::not_found)?;

    if !executor_can_run_command(state, executor_id, &command)? {
        return Err(ApiError::bad_request(
            "command is not compatible with executor capabilities or scopes",
        ));
    }

    Ok(command)
}

pub(crate) fn executor_can_run_command(
    state: &AppState,
    executor_id: Uuid,
    command: &Command,
) -> Result<bool, ApiError> {
    let capabilities = state
        .storage
        .list_executor_capabilities(state.tenant_id, executor_id)?;
    let has_capability = capabilities
        .iter()
        .any(|capability| capability.command_type == command.command_type);
    if !has_capability {
        return Ok(false);
    }

    let scopes = state
        .storage
        .list_executor_scopes(state.tenant_id, executor_id)?;
    if scopes.is_empty() {
        return Ok(false);
    }

    for scope in scopes {
        if executor_scope_matches_command(state, &scope, command)? {
            return Ok(true);
        }
    }

    Ok(false)
}

fn executor_scope_matches_command(
    state: &AppState,
    scope: &ExecutorScope,
    command: &Command,
) -> Result<bool, ApiError> {
    if let Some(target_entity_id) = scope.target_entity_id {
        if target_entity_id != command.target_entity_id {
            return Ok(false);
        }
    }

    if let Some(entity_type) = scope.entity_type.as_deref() {
        let entity = state
            .storage
            .get_entity(state.tenant_id, command.target_entity_id)?
            .ok_or_else(ApiError::not_found)?;
        if entity.entity_type != entity_type {
            return Ok(false);
        }
    }

    if let Some(relationship_type) = scope.relationship_type.as_deref() {
        let outgoing = state.storage.list_relationships(
            state.tenant_id,
            Some(command.target_entity_id),
            None,
        )?;
        let incoming = state.storage.list_relationships(
            state.tenant_id,
            None,
            Some(command.target_entity_id),
        )?;
        if !outgoing
            .iter()
            .chain(incoming.iter())
            .any(|relationship| relationship.relationship_type == relationship_type)
        {
            return Ok(false);
        }
    }

    Ok(true)
}

pub(crate) fn get_command_for_executor_mutation(
    state: &AppState,
    command_id: Uuid,
    agent_key: &str,
) -> Result<Command, ApiError> {
    let command = state
        .storage
        .get_command(state.tenant_id, command_id)?
        .ok_or_else(ApiError::not_found)?;
    if command.claimed_by.as_deref() != Some(agent_key) {
        return Err(ApiError::bad_request(
            "command must be claimed by this executor before completion",
        ));
    }
    let lease = state
        .storage
        .get_active_command_lease(state.tenant_id, command_id)?
        .ok_or_else(|| ApiError::bad_request("command has no active lease"))?;
    if !lease.is_active_at(Utc::now()) {
        return Err(ApiError::bad_request("command lease has expired"));
    }

    Ok(command)
}

pub(crate) fn claim_command_for_executor(
    state: &AppState,
    command_id: Uuid,
    executor: &ExecutorAgent,
    lease_duration_seconds: Option<i64>,
    max_retries: Option<u32>,
    metadata: Option<Value>,
) -> Result<Command, ApiError> {
    let now = Utc::now();
    if let Some(lease) = state
        .storage
        .get_active_command_lease(state.tenant_id, command_id)?
    {
        if lease.is_active_at(now) {
            return Err(ApiError::bad_request("command already has an active lease"));
        }
    }
    let expires_at = lease_expiry(now, lease_duration_seconds)?;
    let command = mutate_command_raw(state, command_id, |command, now| {
        if let Some(max_retries) = max_retries {
            command.max_retries = Some(max_retries);
        }
        command.claim(executor.agent_key.clone(), now)?;
        command.set_lease_expires_at(Some(expires_at), now);
        Ok(())
    })?;
    let lease = CommandLease::new(
        state.tenant_id,
        command.id,
        executor.id,
        now,
        expires_at,
        metadata,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let lease = state.storage.store_command_lease(lease)?;
    record_lease_event(
        state,
        "aion:CommandLeaseCreated",
        &lease,
        Some(&command),
        None,
    )?;
    record_command_event(
        state,
        "aion:CommandClaimed",
        EventSeverity::Info,
        &command,
        None,
    )?;
    Ok(command)
}

pub(crate) fn lease_expiry(
    now: DateTime<Utc>,
    lease_duration_seconds: Option<i64>,
) -> Result<DateTime<Utc>, ApiError> {
    let seconds = lease_duration_seconds.unwrap_or(DEFAULT_COMMAND_LEASE_SECONDS);
    if seconds <= 0 {
        return Err(ApiError::bad_request(
            "lease_duration_seconds must be greater than zero",
        ));
    }
    Ok(now + chrono::Duration::seconds(seconds))
}

pub(crate) fn active_lease_for_executor(
    state: &AppState,
    command_id: Uuid,
    executor_id: Uuid,
) -> Result<CommandLease, ApiError> {
    let lease = state
        .storage
        .get_active_command_lease(state.tenant_id, command_id)?
        .ok_or_else(ApiError::not_found)?;
    if lease.executor_id != executor_id {
        return Err(ApiError::bad_request(
            "active lease is owned by another executor",
        ));
    }
    if !lease.is_active_at(Utc::now()) {
        return Err(ApiError::bad_request("active lease has expired"));
    }
    Ok(lease)
}

pub(crate) fn release_active_lease(
    state: &AppState,
    command_id: Uuid,
    executor_id: Uuid,
) -> Result<CommandLease, ApiError> {
    let mut lease = active_lease_for_executor(state, command_id, executor_id)?;
    let now = Utc::now();
    lease.mark_released(now);
    let lease = state.storage.update_command_lease(lease)?;
    let command = mutate_command_raw(state, command_id, |command, now| command.release(now))?;
    record_lease_event(
        state,
        "aion:CommandLeaseReleased",
        &lease,
        Some(&command),
        None,
    )?;
    record_command_event(
        state,
        "aion:CommandReleased",
        EventSeverity::Info,
        &command,
        Some("command lease released".to_string()),
    )?;
    Ok(lease)
}

pub(crate) fn mark_active_lease_completed(
    state: &AppState,
    command_id: Uuid,
    executor_id: Uuid,
) -> Result<CommandLease, ApiError> {
    let mut lease = active_lease_for_executor(state, command_id, executor_id)?;
    lease.mark_completed(Utc::now());
    Ok(state.storage.update_command_lease(lease)?)
}

pub(crate) fn mark_active_lease_failed(
    state: &AppState,
    command_id: Uuid,
    executor_id: Uuid,
) -> Result<CommandLease, ApiError> {
    let mut lease = active_lease_for_executor(state, command_id, executor_id)?;
    lease.mark_failed(Utc::now());
    Ok(state.storage.update_command_lease(lease)?)
}

pub(crate) fn record_lease_event(
    state: &AppState,
    event_type: impl Into<String>,
    lease: &CommandLease,
    command: Option<&Command>,
    metadata: Option<Value>,
) -> Result<Event, ApiError> {
    let mut event_metadata = json!({
        "lease_id": lease.id,
        "executor_id": lease.executor_id,
        "lease_status": lease.lease_status,
        "expires_at": lease.expires_at
    });
    if let Some(object) = event_metadata.as_object_mut() {
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
            target_entity_id: command.map(|command| command.target_entity_id),
            message: Some("Command lease lifecycle event".to_string()),
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: None,
            raw_message_id: None,
            observation_id: None,
            command_id: Some(lease.command_id),
            action_id: None,
            action_result_id: None,
            metadata: Some(event_metadata),
        },
    )
}

pub(crate) fn enrich_executor_result_metadata(
    executor: &ExecutorAgent,
    metadata: Option<Value>,
) -> Value {
    let mut enriched = json!({
        "executor_id": executor.id,
        "agent_key": executor.agent_key,
        "source": "executor_api"
    });
    if let (Some(object), Some(metadata)) = (enriched.as_object_mut(), metadata) {
        object.insert("executor_metadata".to_string(), metadata);
    }

    enriched
}

pub(crate) fn record_command_event(
    state: &AppState,
    event_type: impl Into<String>,
    severity: EventSeverity,
    command: &Command,
    message: Option<String>,
) -> Result<Event, ApiError> {
    record_event(
        state,
        EventDraft {
            event_type: event_type.into(),
            severity,
            source_entity_id: None,
            target_entity_id: Some(command.target_entity_id),
            message,
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: None,
            raw_message_id: None,
            observation_id: None,
            command_id: Some(command.id),
            action_id: None,
            action_result_id: None,
            metadata: Some(json!({
                "command_type": command.command_type,
                "status": command.status,
                "approval_status": command.approval_status,
                "claimed_by": command.claimed_by
            })),
        },
    )
}

pub(crate) fn mutate_command_raw(
    state: &AppState,
    command_id: Uuid,
    mutate: impl FnOnce(&mut Command, DateTime<Utc>) -> Result<(), aion_action::ActionModelError>,
) -> Result<Command, ApiError> {
    let mut command = state
        .storage
        .get_command(state.tenant_id, command_id)?
        .ok_or_else(ApiError::not_found)?;
    mutate(&mut command, Utc::now()).map_err(|err| ApiError::bad_request(err.to_string()))?;
    let command = state.storage.update_command(command)?;
    Ok(command)
}
