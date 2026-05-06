use crate::{
    auth::{
        is_admin_all, principal_tenant_id, principal_tenant_or_default, require_scope,
        require_scope_for_write, tenant_for_created_resource, AuthContext,
    },
    command_policy_decision,
    command_support::{
        active_lease_for_executor, lease_expiry, record_command_event, record_lease_event,
        release_active_lease,
    },
    error::ApiError,
    mutate_command, require_same_tenant_for_target_action, require_same_tenant_for_target_command,
    require_same_tenant_for_target_entity, state_for_tenant, AppState, AuthMode,
};
use aion_action::{Action, ActionResult, Command, CommandLease, CommandStatus};
use aion_event::EventSeverity;
use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/commands", post(create_command).get(query_commands))
        .route(
            "/commands/recover-expired-leases",
            post(recover_expired_leases),
        )
        .route("/commands/:command_id/lease", get(get_command_lease))
        .route(
            "/commands/:command_id/lease/refresh",
            post(refresh_command_lease),
        )
        .route(
            "/commands/:command_id/lease/release",
            post(release_command_lease),
        )
        .route("/commands/:command_id/claim", post(claim_command))
        .route("/commands/:command_id/release", post(release_command))
        .route(
            "/commands/:command_id/mark-executed",
            post(mark_command_executed),
        )
        .route(
            "/commands/:command_id/mark-failed",
            post(mark_command_failed),
        )
        .route("/commands/:command_id/cancel", post(cancel_command))
        .route("/commands/:command_id/approve", post(approve_command))
        .route("/commands/:command_id/reject", post(reject_command))
        .route("/commands/:command_id", get(get_command))
        .route("/actions", post(create_action).get(query_actions))
        .route("/actions/:action_id", get(get_action))
        .route(
            "/action-results",
            post(create_action_result).get(query_action_results),
        )
}

#[derive(Debug, Deserialize)]
struct RefreshCommandLeaseRequest {
    executor_id: Uuid,
    lease_duration_seconds: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ReleaseCommandLeaseRequest {
    executor_id: Uuid,
}

#[derive(Debug, Serialize)]
struct RecoverExpiredLeasesResponse {
    expired_lease_ids: Vec<Uuid>,
    retried_command_ids: Vec<Uuid>,
    failed_command_ids: Vec<Uuid>,
}

#[derive(Debug, Deserialize)]
struct CreateCommandRequest {
    target_entity_id: Uuid,
    command_type: String,
    payload: Value,
    requested_by: Option<String>,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClaimCommandRequest {
    claimed_by: String,
}

#[derive(Debug, Deserialize)]
struct MarkFailedCommandRequest {
    failure_reason: String,
}

#[derive(Debug, Deserialize)]
struct CommandQuery {
    target_entity_id: Option<Uuid>,
    status: Option<CommandStatus>,
}

#[derive(Debug, Deserialize)]
struct CreateActionRequest {
    command_id: Uuid,
    executor_entity_id: Option<Uuid>,
    action_type: String,
    status: String,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ActionQuery {
    command_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
struct CreateActionResultRequest {
    command_id: Uuid,
    action_id: Uuid,
    status: String,
    verified: bool,
    result_payload: Value,
    observed_at: DateTime<Utc>,
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ActionResultQuery {
    action_id: Option<Uuid>,
    command_id: Option<Uuid>,
}

async fn get_command_lease(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<CommandLease>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/commands/:command_id/lease",
        "commands:read",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/lease",
        command_id,
    )?;
    let scoped_state = state_for_tenant(&state, command.tenant_id);
    Ok(Json(
        scoped_state
            .storage
            .get_latest_command_lease(scoped_state.tenant_id, command_id)?
            .ok_or_else(ApiError::not_found)?,
    ))
}

async fn refresh_command_lease(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
    Json(request): Json<RefreshCommandLeaseRequest>,
) -> Result<Json<CommandLease>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/lease/refresh",
        "commands:lease",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/lease/refresh",
        command_id,
    )?;
    let scoped_state = state_for_tenant(&state, command.tenant_id);
    let mut lease = active_lease_for_executor(&scoped_state, command_id, request.executor_id)?;
    let now = Utc::now();
    let expires_at = lease_expiry(now, request.lease_duration_seconds)?;
    lease
        .refresh(expires_at, now)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let lease = scoped_state.storage.update_command_lease(lease)?;
    let mut command = scoped_state
        .storage
        .get_command(scoped_state.tenant_id, command_id)?
        .ok_or_else(ApiError::not_found)?;
    command.set_lease_expires_at(Some(expires_at), now);
    let command = scoped_state.storage.update_command(command)?;
    record_lease_event(
        &scoped_state,
        "aion:CommandLeaseRefreshed",
        &lease,
        Some(&command),
        None,
    )?;
    Ok(Json(lease))
}

async fn release_command_lease(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
    Json(request): Json<ReleaseCommandLeaseRequest>,
) -> Result<Json<CommandLease>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/lease/release",
        "commands:lease",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/lease/release",
        command_id,
    )?;
    release_active_lease(
        &state_for_tenant(&state, command.tenant_id),
        command_id,
        request.executor_id,
    )
    .map(Json)
}

async fn recover_expired_leases(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<RecoverExpiredLeasesResponse>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/recover-expired-leases",
        "commands:lease",
    )?;
    let scoped_state = state_for_tenant(&state, principal_tenant_or_default(&state, &auth)?);
    let now = Utc::now();
    let mut response = RecoverExpiredLeasesResponse {
        expired_lease_ids: Vec::new(),
        retried_command_ids: Vec::new(),
        failed_command_ids: Vec::new(),
    };

    for mut lease in scoped_state
        .storage
        .list_active_command_leases(scoped_state.tenant_id)?
    {
        if lease.expires_at > now {
            continue;
        }
        lease.mark_expired(now);
        let lease = scoped_state.storage.update_command_lease(lease)?;
        response.expired_lease_ids.push(lease.id);

        let mut command = scoped_state
            .storage
            .get_command(scoped_state.tenant_id, lease.command_id)?
            .ok_or_else(ApiError::not_found)?;
        record_lease_event(
            &scoped_state,
            "aion:CommandLeaseExpired",
            &lease,
            Some(&command),
            None,
        )?;

        if command.retry_limit_exceeded() {
            command.mark_failed_due_to_retry_limit("command retry limit exceeded", now);
            let command = scoped_state.storage.update_command(command)?;
            response.failed_command_ids.push(command.id);
            record_lease_event(
                &scoped_state,
                "aion:CommandRetryLimitExceeded",
                &lease,
                Some(&command),
                Some(
                    json!({"retry_count": command.retry_count, "max_retries": command.max_retries}),
                ),
            )?;
            record_command_event(
                &scoped_state,
                "aion:CommandFailed",
                EventSeverity::Error,
                &command,
                Some("command retry limit exceeded".to_string()),
            )?;
        } else {
            command.schedule_retry(now);
            let command = scoped_state.storage.update_command(command)?;
            response.retried_command_ids.push(command.id);
            record_lease_event(
                &scoped_state,
                "aion:CommandRetryScheduled",
                &lease,
                Some(&command),
                Some(
                    json!({"retry_count": command.retry_count, "max_retries": command.max_retries}),
                ),
            )?;
        }
    }

    Ok(Json(response))
}

async fn create_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateCommandRequest>,
) -> Result<(StatusCode, Json<Command>), ApiError> {
    require_scope_for_write(&state, &auth, "/commands", "commands:create")?;
    require_same_tenant_for_target_entity(&state, &auth, "/commands", request.target_entity_id)?;
    let scoped_state = state_for_tenant(&state, tenant_for_created_resource(&state, &auth)?);
    let (approval_status, policy_decision) = command_policy_decision(
        &scoped_state,
        request.target_entity_id,
        &request.command_type,
    )?;
    let command = Command::new(
        scoped_state.tenant_id,
        request.target_entity_id,
        request.command_type,
        request.payload,
        request.requested_by,
        request.reason,
        Some(approval_status),
        Some(policy_decision),
        Utc::now(),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let command = scoped_state.storage.store_command(command)?;
    record_command_event(
        &scoped_state,
        "aion:CommandCreated",
        EventSeverity::Info,
        &command,
        None,
    )?;
    Ok((StatusCode::CREATED, Json(command)))
}

async fn claim_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
    Json(request): Json<ClaimCommandRequest>,
) -> Result<Json<Command>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/claim",
        "commands:claim",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/claim",
        command_id,
    )?;
    mutate_command(
        &state_for_tenant(&state, command.tenant_id),
        command_id,
        "aion:CommandClaimed",
        EventSeverity::Info,
        |command, now| command.claim(request.claimed_by, now),
    )
}

async fn release_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/release",
        "commands:write",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/release",
        command_id,
    )?;
    mutate_command(
        &state_for_tenant(&state, command.tenant_id),
        command_id,
        "aion:CommandReleased",
        EventSeverity::Info,
        |command, now| command.release(now),
    )
}

async fn mark_command_executed(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/mark-executed",
        "commands:write",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/mark-executed",
        command_id,
    )?;
    mutate_command(
        &state_for_tenant(&state, command.tenant_id),
        command_id,
        "aion:CommandExecuted",
        EventSeverity::Info,
        |command, now| command.mark_executed(now),
    )
}

async fn mark_command_failed(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
    Json(request): Json<MarkFailedCommandRequest>,
) -> Result<Json<Command>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/mark-failed",
        "commands:write",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/mark-failed",
        command_id,
    )?;
    mutate_command(
        &state_for_tenant(&state, command.tenant_id),
        command_id,
        "aion:CommandFailed",
        EventSeverity::Error,
        |command, now| command.mark_failed(request.failure_reason, now),
    )
}

async fn cancel_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/cancel",
        "commands:write",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/cancel",
        command_id,
    )?;
    mutate_command(
        &state_for_tenant(&state, command.tenant_id),
        command_id,
        "aion:CommandCancelled",
        EventSeverity::Warning,
        |command, now| command.cancel(now),
    )
}

async fn approve_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/approve",
        "commands:approve",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/approve",
        command_id,
    )?;
    mutate_command(
        &state_for_tenant(&state, command.tenant_id),
        command_id,
        "aion:CommandApproved",
        EventSeverity::Info,
        |command, now| command.approve(now),
    )
}

async fn reject_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/reject",
        "commands:approve",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/reject",
        command_id,
    )?;
    mutate_command(
        &state_for_tenant(&state, command.tenant_id),
        command_id,
        "aion:CommandRejected",
        EventSeverity::Warning,
        |command, now| command.reject(now),
    )
}

async fn get_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    require_scope(&state, &auth, "/commands/:command_id", "commands:read")?;
    let command = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_command(state.tenant_id, command_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_command_any_tenant(command_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_command(tenant_id, command_id)? {
            Some(command) => command,
            None => {
                if state.storage.get_command_any_tenant(command_id)?.is_some() {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /commands/:command_id",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };

    Ok(Json(command))
}

async fn query_commands(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<CommandQuery>,
) -> Result<Json<Vec<Command>>, ApiError> {
    require_scope(&state, &auth, "/commands", "commands:read")?;
    let commands = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .query_commands(state.tenant_id, query.target_entity_id, query.status)?
    } else if is_admin_all(&auth) {
        let status = query.status.clone();
        state
            .storage
            .list_all_commands()?
            .into_iter()
            .filter(|command| {
                query
                    .target_entity_id
                    .map(|id| command.target_entity_id == id)
                    .unwrap_or(true)
            })
            .filter(|command| {
                status
                    .as_ref()
                    .map(|value| command.status == *value)
                    .unwrap_or(true)
            })
            .collect()
    } else {
        state.storage.query_commands(
            principal_tenant_id(&auth)?,
            query.target_entity_id,
            query.status,
        )?
    };
    Ok(Json(commands))
}

async fn create_action(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateActionRequest>,
) -> Result<(StatusCode, Json<Action>), ApiError> {
    require_scope_for_write(&state, &auth, "/actions", "actions:write")?;
    let command =
        require_same_tenant_for_target_command(&state, &auth, "/actions", request.command_id)?;
    if let Some(executor_entity_id) = request.executor_entity_id {
        require_same_tenant_for_target_entity(&state, &auth, "/actions", executor_entity_id)?;
    }
    let scoped_state = state_for_tenant(&state, command.tenant_id);

    let action = Action::new(
        scoped_state.tenant_id,
        request.command_id,
        request.executor_entity_id,
        request.action_type,
        request.status,
        request.started_at,
        request.finished_at,
        request.metadata,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let action = scoped_state.storage.store_action(action)?;
    Ok((StatusCode::CREATED, Json(action)))
}

async fn get_action(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(action_id): Path<Uuid>,
) -> Result<Json<Action>, ApiError> {
    require_scope(&state, &auth, "/actions/:action_id", "actions:read")?;
    let action = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_action(state.tenant_id, action_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_action_any_tenant(action_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_action(tenant_id, action_id)? {
            Some(action) => action,
            None => {
                if state.storage.get_action_any_tenant(action_id)?.is_some() {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /actions/:action_id",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };

    Ok(Json(action))
}

async fn query_actions(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<ActionQuery>,
) -> Result<Json<Vec<Action>>, ApiError> {
    require_scope(&state, &auth, "/actions", "actions:read")?;
    let actions = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .query_actions(state.tenant_id, query.command_id)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .list_all_actions()?
            .into_iter()
            .filter(|action| {
                query
                    .command_id
                    .map(|id| action.command_id == id)
                    .unwrap_or(true)
            })
            .collect()
    } else {
        state
            .storage
            .query_actions(principal_tenant_id(&auth)?, query.command_id)?
    };
    Ok(Json(actions))
}

async fn create_action_result(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateActionResultRequest>,
) -> Result<(StatusCode, Json<ActionResult>), ApiError> {
    require_scope_for_write(&state, &auth, "/action-results", "actions:write")?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/action-results",
        request.command_id,
    )?;
    let scoped_state = state_for_tenant(&state, command.tenant_id);
    let action = require_same_tenant_for_target_action(
        &scoped_state,
        &auth,
        "/action-results",
        request.action_id,
    )?;
    if action.command_id != request.command_id {
        return Err(ApiError::bad_request(
            "action_id does not belong to command_id",
        ));
    }

    let result = ActionResult::new(
        scoped_state.tenant_id,
        request.command_id,
        request.action_id,
        request.status,
        request.verified,
        request.result_payload,
        request.observed_at,
        request.metadata,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let result = scoped_state.storage.store_action_result(result)?;
    Ok((StatusCode::CREATED, Json(result)))
}

async fn query_action_results(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<ActionResultQuery>,
) -> Result<Json<Vec<ActionResult>>, ApiError> {
    require_scope(&state, &auth, "/action-results", "actions:read")?;
    let results = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .query_action_results(state.tenant_id, query.action_id, query.command_id)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .list_all_action_results()?
            .into_iter()
            .filter(|result| {
                query
                    .action_id
                    .map(|id| result.action_id == id)
                    .unwrap_or(true)
            })
            .filter(|result| {
                query
                    .command_id
                    .map(|id| result.command_id == id)
                    .unwrap_or(true)
            })
            .collect()
    } else {
        state.storage.query_action_results(
            principal_tenant_id(&auth)?,
            query.action_id,
            query.command_id,
        )?
    };
    Ok(Json(results))
}
