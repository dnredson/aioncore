use crate::{
    auth::{is_admin_all, principal_tenant_id, require_any_scope, require_scope, AuthContext},
    claim_command_for_executor, enrich_executor_result_metadata, ensure_executor_can_run_command,
    ensure_executor_exists,
    error::ApiError,
    get_command_for_executor_mutation, get_executor_agent, mark_active_lease_completed,
    mark_active_lease_failed, record_command_event, record_executor_event,
    require_same_tenant_for_target_entity, require_same_tenant_for_target_executor,
    state_for_tenant, AppState, AuthMode,
};
use aion_action::{
    Action, ActionResult, Command, CommandStatus, ExecutorAgent, ExecutorAgentStatus,
    ExecutorCapability, ExecutorScope,
};
use aion_event::EventSeverity;
use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{get, post, put},
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/executors", post(create_executor).get(list_executors))
        .route("/executors/:executor_id", get(get_executor))
        .route("/executors/:executor_id/heartbeat", put(heartbeat_executor))
        .route(
            "/executors/:executor_id/capabilities",
            put(put_executor_capabilities).get(get_executor_capabilities),
        )
        .route(
            "/executors/:executor_id/scopes",
            put(put_executor_scopes).get(get_executor_scopes),
        )
        .route(
            "/executors/:executor_id/commands/pending",
            get(poll_executor_pending_commands),
        )
        .route(
            "/executors/:executor_id/commands/:command_id/claim",
            post(claim_executor_command),
        )
        .route(
            "/executors/:executor_id/commands/:command_id/complete",
            post(complete_executor_command),
        )
        .route(
            "/executors/:executor_id/commands/:command_id/fail",
            post(fail_executor_command),
        )
}

#[derive(Debug, Deserialize)]
struct CreateExecutorRequest {
    agent_key: String,
    agent_type: String,
    display_name: Option<String>,
    status: Option<ExecutorAgentStatus>,
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ExecutorHeartbeatRequest {
    status: ExecutorAgentStatus,
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ExecutorClaimCommandRequest {
    pub(crate) lease_duration_seconds: Option<i64>,
    pub(crate) max_retries: Option<u32>,
    pub(crate) metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct PutExecutorCapabilityRequest {
    command_type: String,
    protocol: Option<String>,
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PutExecutorScopeRequest {
    pub(crate) target_entity_id: Option<Uuid>,
    pub(crate) entity_type: Option<String>,
    pub(crate) relationship_type: Option<String>,
    pub(crate) metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ExecutorCompleteCommandRequest {
    result_payload: Value,
    verified: Option<bool>,
    status: Option<String>,
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ExecutorFailCommandRequest {
    failure_reason: String,
    result_payload: Option<Value>,
    metadata: Option<Value>,
}

#[derive(Debug, Serialize)]
struct ExecutorCommandCompletionResponse {
    command: Command,
    action: Action,
    action_result: ActionResult,
}

async fn create_executor(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateExecutorRequest>,
) -> Result<(StatusCode, Json<ExecutorAgent>), ApiError> {
    require_scope(&state, &auth, "/executors", "executors:register")?;
    let now = Utc::now();
    let executor = ExecutorAgent::new(
        state.tenant_id,
        request.agent_key,
        request.agent_type,
        request.display_name,
        request.status.unwrap_or(ExecutorAgentStatus::Online),
        request.metadata,
        now,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let executor = state.storage.create_executor(executor)?;
    record_executor_event(
        &state,
        "aion:ExecutorRegistered",
        &executor,
        None,
        Some(json!({"agent_type": executor.agent_type})),
    )?;

    Ok((StatusCode::CREATED, Json(executor)))
}

async fn list_executors(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Vec<ExecutorAgent>>, ApiError> {
    require_scope(&state, &auth, "/executors", "executors:read")?;
    let executors = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state.storage.list_executors(state.tenant_id)?
    } else if is_admin_all(&auth) {
        state.storage.list_all_executors()?
    } else {
        state.storage.list_executors(principal_tenant_id(&auth)?)?
    };
    Ok(Json(executors))
}

async fn get_executor(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(executor_id): Path<Uuid>,
) -> Result<Json<ExecutorAgent>, ApiError> {
    require_scope(&state, &auth, "/executors/:executor_id", "executors:read")?;
    Ok(Json(load_executor_for_read(
        &state,
        &auth,
        executor_id,
        "/executors/:executor_id",
    )?))
}

async fn heartbeat_executor(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(executor_id): Path<Uuid>,
    Json(request): Json<ExecutorHeartbeatRequest>,
) -> Result<Json<ExecutorAgent>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/executors/:executor_id/heartbeat",
        "executors:heartbeat",
    )?;
    let mut executor = get_executor_agent(&state, executor_id)?;
    executor.heartbeat(request.status, Utc::now());
    if request.metadata.is_some() {
        executor.metadata = request.metadata;
    }
    let executor = state.storage.update_executor(executor)?;
    record_executor_event(
        &state,
        "aion:ExecutorHeartbeat",
        &executor,
        None,
        Some(json!({"status": executor.status})),
    )?;

    Ok(Json(executor))
}

async fn put_executor_capabilities(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(executor_id): Path<Uuid>,
    Json(requests): Json<Vec<PutExecutorCapabilityRequest>>,
) -> Result<(StatusCode, Json<Vec<ExecutorCapability>>), ApiError> {
    require_any_scope(
        &state,
        &auth,
        "/executors/:executor_id/capabilities",
        &["executors:admin", "executors:write"],
    )?;
    let executor = require_same_tenant_for_target_executor(
        &state,
        &auth,
        "/executors/:executor_id/capabilities",
        executor_id,
    )?;
    let scoped_state = state_for_tenant(&state, executor.tenant_id);
    let capabilities = requests
        .into_iter()
        .map(|request| {
            ExecutorCapability::new(
                executor_id,
                request.command_type,
                request.protocol,
                request.metadata,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    Ok((
        StatusCode::OK,
        Json(scoped_state.storage.put_executor_capabilities(
            scoped_state.tenant_id,
            executor_id,
            capabilities,
        )?),
    ))
}

async fn get_executor_capabilities(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(executor_id): Path<Uuid>,
) -> Result<Json<Vec<ExecutorCapability>>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/executors/:executor_id/capabilities",
        "executors:read",
    )?;
    let executor = load_executor_for_read(
        &state,
        &auth,
        executor_id,
        "/executors/:executor_id/capabilities",
    )?;
    Ok(Json(state.storage.list_executor_capabilities(
        executor.tenant_id,
        executor_id,
    )?))
}

async fn put_executor_scopes(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(executor_id): Path<Uuid>,
    Json(requests): Json<Vec<PutExecutorScopeRequest>>,
) -> Result<(StatusCode, Json<Vec<ExecutorScope>>), ApiError> {
    require_any_scope(
        &state,
        &auth,
        "/executors/:executor_id/scopes",
        &["executors:admin", "executors:write"],
    )?;
    let executor = require_same_tenant_for_target_executor(
        &state,
        &auth,
        "/executors/:executor_id/scopes",
        executor_id,
    )?;
    let scoped_state = state_for_tenant(&state, executor.tenant_id);
    let mut scopes = Vec::with_capacity(requests.len());
    for request in requests {
        if let Some(target_entity_id) = request.target_entity_id {
            require_same_tenant_for_target_entity(
                &state,
                &auth,
                "/executors/:executor_id/scopes",
                target_entity_id,
            )?;
        }
        scopes.push(ExecutorScope::new(
            executor_id,
            request.target_entity_id,
            request.entity_type,
            request.relationship_type,
            request.metadata,
        ));
    }

    Ok((
        StatusCode::OK,
        Json(scoped_state.storage.put_executor_scopes(
            scoped_state.tenant_id,
            executor_id,
            scopes,
        )?),
    ))
}

async fn get_executor_scopes(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(executor_id): Path<Uuid>,
) -> Result<Json<Vec<ExecutorScope>>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/executors/:executor_id/scopes",
        "executors:read",
    )?;
    let executor =
        load_executor_for_read(&state, &auth, executor_id, "/executors/:executor_id/scopes")?;
    Ok(Json(
        state
            .storage
            .list_executor_scopes(executor.tenant_id, executor_id)?,
    ))
}

async fn poll_executor_pending_commands(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(executor_id): Path<Uuid>,
) -> Result<Json<Vec<Command>>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/executors/:executor_id/commands/pending",
        "executors:poll",
    )?;
    ensure_executor_exists(&state, executor_id)?;
    let commands = state
        .storage
        .query_commands(state.tenant_id, None, Some(CommandStatus::Pending))?
        .into_iter()
        .filter(|command| executor_can_run_command_route(&state, executor_id, command))
        .collect::<Vec<_>>();

    Ok(Json(commands))
}

async fn claim_executor_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((executor_id, command_id)): Path<(Uuid, Uuid)>,
    request: Option<Json<ExecutorClaimCommandRequest>>,
) -> Result<Json<Command>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/executors/:executor_id/commands/:command_id/claim",
        "executors:claim",
    )?;
    let executor = get_executor_agent(&state, executor_id)?;
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
        request.and_then(|request| request.metadata),
    )?;
    record_executor_event(
        &state,
        "aion:ExecutorClaimedCommand",
        &executor,
        Some(&command),
        None,
    )?;

    Ok(Json(command))
}

async fn complete_executor_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((executor_id, command_id)): Path<(Uuid, Uuid)>,
    Json(request): Json<ExecutorCompleteCommandRequest>,
) -> Result<Json<ExecutorCommandCompletionResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/executors/:executor_id/commands/:command_id/complete",
        "executors:report",
    )?;
    let executor = get_executor_agent(&state, executor_id)?;
    let command = get_command_for_executor_mutation(&state, command_id, &executor.agent_key)?;
    let now = Utc::now();
    let action = Action::new(
        state.tenant_id,
        command.id,
        None,
        command.command_type.clone(),
        "completed",
        command.claimed_at,
        Some(now),
        Some(json!({
            "executor_id": executor.id,
            "agent_key": executor.agent_key,
            "source": "executor_api"
        })),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let action = state.storage.store_action(action)?;
    let action_result = ActionResult::new(
        state.tenant_id,
        command.id,
        action.id,
        request.status.unwrap_or_else(|| "succeeded".to_string()),
        request.verified.unwrap_or(true),
        request.result_payload,
        now,
        Some(enrich_executor_result_metadata(&executor, request.metadata)),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let action_result = state.storage.store_action_result(action_result)?;
    let command = crate::mutate_command_raw(&state, command_id, |command, now| {
        command.mark_executed(now)
    })?;
    mark_active_lease_completed(&state, command_id, executor_id)?;
    record_command_event(
        &state,
        "aion:CommandExecuted",
        EventSeverity::Info,
        &command,
        None,
    )?;
    record_executor_event(
        &state,
        "aion:ExecutorCompletedCommand",
        &executor,
        Some(&command),
        Some(json!({"action_id": action.id, "action_result_id": action_result.id})),
    )?;

    Ok(Json(ExecutorCommandCompletionResponse {
        command,
        action,
        action_result,
    }))
}

async fn fail_executor_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((executor_id, command_id)): Path<(Uuid, Uuid)>,
    Json(request): Json<ExecutorFailCommandRequest>,
) -> Result<Json<ExecutorCommandCompletionResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/executors/:executor_id/commands/:command_id/fail",
        "executors:report",
    )?;
    let executor = get_executor_agent(&state, executor_id)?;
    let command = get_command_for_executor_mutation(&state, command_id, &executor.agent_key)?;
    let now = Utc::now();
    let action = Action::new(
        state.tenant_id,
        command.id,
        None,
        command.command_type.clone(),
        "failed",
        command.claimed_at,
        Some(now),
        Some(json!({
            "executor_id": executor.id,
            "agent_key": executor.agent_key,
            "source": "executor_api"
        })),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let action = state.storage.store_action(action)?;
    let action_result = ActionResult::new(
        state.tenant_id,
        command.id,
        action.id,
        "failed",
        false,
        request
            .result_payload
            .unwrap_or_else(|| json!({"failure_reason": request.failure_reason})),
        now,
        Some(enrich_executor_result_metadata(&executor, request.metadata)),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let action_result = state.storage.store_action_result(action_result)?;
    let command = crate::mutate_command_raw(&state, command_id, |command, now| {
        command.mark_failed(request.failure_reason, now)
    })?;
    mark_active_lease_failed(&state, command_id, executor_id)?;
    record_command_event(
        &state,
        "aion:CommandFailed",
        EventSeverity::Error,
        &command,
        None,
    )?;
    record_executor_event(
        &state,
        "aion:ExecutorFailedCommand",
        &executor,
        Some(&command),
        Some(json!({"action_id": action.id, "action_result_id": action_result.id})),
    )?;

    Ok(Json(ExecutorCommandCompletionResponse {
        command,
        action,
        action_result,
    }))
}

fn load_executor_for_read(
    state: &AppState,
    auth: &AuthContext,
    executor_id: Uuid,
    endpoint: &'static str,
) -> Result<ExecutorAgent, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        return state
            .storage
            .get_executor(state.tenant_id, executor_id)?
            .ok_or_else(ApiError::not_found);
    }
    if is_admin_all(auth) {
        return state
            .storage
            .get_executor_any_tenant(executor_id)?
            .ok_or_else(ApiError::not_found);
    }

    let tenant_id = principal_tenant_id(auth)?;
    match state.storage.get_executor(tenant_id, executor_id)? {
        Some(executor) => Ok(executor),
        None => {
            if state
                .storage
                .get_executor_any_tenant(executor_id)?
                .is_some()
            {
                Err(ApiError::forbidden(format!(
                    "principal tenant does not own the resource for {endpoint}",
                )))
            } else {
                Err(ApiError::not_found())
            }
        }
    }
}

fn executor_can_run_command_route(state: &AppState, executor_id: Uuid, command: &Command) -> bool {
    crate::executor_can_run_command(state, executor_id, command).unwrap_or(false)
}
