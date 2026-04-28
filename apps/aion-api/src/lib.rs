use aion_action::{
    Action, ActionResult, ApprovalStatus, Capability, Command, CommandStatus, Policy,
};
use aion_entity::Entity;
use aion_event::{Event, EventSeverity};
use aion_observation::{Observation, ObservationValue};
use aion_payload::{
    CanonicalJsonDecoder, DecodeInput, PayloadDecoder, PayloadFormat, SenMlJsonDecoder,
    UltraLightDecoder,
};
use aion_raw_message::{NormalizationStatus, RawMessage, RawMessageSource};
use aion_relationship::Relationship;
use aion_storage::{
    ActionResultStore, ActionStore, CapabilityStore, CommandStore, EntityStore, EventFilter,
    EventStore, InMemoryStorage, ObservationStore, PayloadProfile, PayloadProfileStore,
    PolicyStore, RawMessageStore, RelationshipStore, StorageError,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct AppState {
    storage: InMemoryStorage,
    tenant_id: Uuid,
}

impl AppState {
    pub fn local() -> Self {
        Self {
            storage: InMemoryStorage::new(),
            tenant_id: Uuid::nil(),
        }
    }

    pub fn with_storage(storage: InMemoryStorage, tenant_id: Uuid) -> Self {
        Self { storage, tenant_id }
    }
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    storage: &'static str,
}

#[derive(Debug, Deserialize)]
pub struct CreateEntityRequest {
    pub entity_key: String,
    pub entity_type: String,
    pub jsonld: Value,
}

#[derive(Debug)]
struct EntityInput {
    entity_key: String,
    entity_type: String,
    jsonld: Value,
}

#[derive(Debug, Deserialize)]
pub struct CreateRelationshipRequest {
    pub source_entity_id: Uuid,
    pub relationship_type: String,
    pub target_entity_id: Uuid,
    #[serde(default = "empty_object")]
    pub jsonld: Value,
}

#[derive(Debug, Deserialize)]
pub struct CreateObservationRequest {
    pub producer_entity_id: Uuid,
    pub feature_of_interest_id: Uuid,
    pub observed_property: String,
    pub value: ObservationValue,
    pub unit: Option<String>,
    pub observed_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub protocol: String,
    pub payload_format: String,
    pub raw_message_id: Option<Uuid>,
    #[serde(default = "empty_object")]
    pub quality: Value,
    #[serde(default = "empty_object")]
    pub metadata: Value,
}

#[derive(Debug, Deserialize)]
pub struct HttpIngestRequest {
    pub producer_entity_id: Uuid,
    pub feature_of_interest_id: Uuid,
    pub payload_format: String,
    pub protocol: String,
    pub content_type: Option<String>,
    pub observed_at: Option<DateTime<Utc>>,
    pub payload: Value,
    pub mapping: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct HttpIngestResponse {
    pub raw_message_id: Uuid,
    pub observations: Vec<Observation>,
}

#[derive(Debug, Deserialize)]
pub struct PutPayloadProfileRequest {
    pub payload_format: String,
    pub protocol: Option<String>,
    pub content_type: Option<String>,
    pub attribute_mapping: Option<Value>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct PutCapabilityRequest {
    pub capability_name: String,
    pub command_type: String,
    pub protocol: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct CreateCommandRequest {
    pub target_entity_id: Uuid,
    pub command_type: String,
    pub payload: Value,
    pub requested_by: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ClaimCommandRequest {
    pub claimed_by: String,
}

#[derive(Debug, Deserialize)]
pub struct MarkFailedCommandRequest {
    pub failure_reason: String,
}

#[derive(Debug, Deserialize)]
pub struct PutPolicyRequest {
    pub target_entity_id: Option<Uuid>,
    pub command_type: Option<String>,
    pub requires_approval: bool,
    pub auto_execute_allowed: bool,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct PolicyQuery {
    pub target_entity_id: Option<Uuid>,
    pub command_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CommandQuery {
    pub target_entity_id: Option<Uuid>,
    pub status: Option<CommandStatus>,
}

#[derive(Debug, Deserialize)]
pub struct CreateActionRequest {
    pub command_id: Uuid,
    pub executor_entity_id: Option<Uuid>,
    pub action_type: String,
    pub status: String,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ActionQuery {
    pub command_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct CreateActionResultRequest {
    pub command_id: Uuid,
    pub action_id: Uuid,
    pub status: String,
    pub verified: bool,
    pub result_payload: Value,
    pub observed_at: DateTime<Utc>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ActionResultQuery {
    pub action_id: Option<Uuid>,
    pub command_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct CreateEventRequest {
    pub event_type: String,
    pub severity: EventSeverity,
    pub source_entity_id: Option<Uuid>,
    pub target_entity_id: Option<Uuid>,
    pub message: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub observed_at: Option<DateTime<Utc>>,
    pub correlation_id: Option<String>,
    pub raw_message_id: Option<Uuid>,
    pub observation_id: Option<Uuid>,
    pub command_id: Option<Uuid>,
    pub action_id: Option<Uuid>,
    pub action_result_id: Option<Uuid>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct EventQuery {
    pub source_entity_id: Option<Uuid>,
    pub target_entity_id: Option<Uuid>,
    pub event_type: Option<String>,
    pub severity: Option<EventSeverity>,
    pub command_id: Option<Uuid>,
    pub raw_message_id: Option<Uuid>,
    pub correlation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ObservationQuery {
    pub feature_of_interest_id: Option<Uuid>,
    pub observed_property: Option<String>,
    pub raw_message_id: Option<Uuid>,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct RawMessageQuery {
    pub producer_entity_id: Option<Uuid>,
    pub feature_of_interest_id: Option<Uuid>,
    pub payload_format: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RawMessageResponse {
    pub id: Uuid,
    pub raw_message_id: Uuid,
    pub source_type: RawMessageSource,
    pub protocol: Option<String>,
    pub content_type: Option<String>,
    pub payload_format: Option<String>,
    pub producer_entity_id: Option<Uuid>,
    pub feature_of_interest_id: Option<Uuid>,
    pub received_at: DateTime<Utc>,
    pub normalization_status: NormalizationStatus,
    pub normalization_error: Option<String>,
    pub decoder_metadata: Value,
    pub payload: Value,
}

#[derive(Debug, Serialize)]
pub struct EntityContextResponse {
    pub entity: Entity,
    pub outgoing_relationships: Vec<Relationship>,
    pub incoming_relationships: Vec<Relationship>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

pub fn app() -> Router {
    app_with_state(AppState::local())
}

pub fn app_with_state(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/entities", post(create_entity).get(list_entities))
        .route("/entities/:entity_id", get(get_entity))
        .route("/entities/:entity_id/context", get(get_entity_context))
        .route(
            "/entities/:entity_id/capabilities",
            put(put_capabilities).get(get_capabilities),
        )
        .route(
            "/entities/:entity_id/payload-profile",
            put(put_payload_profile).get(get_payload_profile),
        )
        .route("/relationships", post(create_relationship))
        .route("/policies", put(put_policies).get(query_policies))
        .route("/commands", post(create_command).get(query_commands))
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
        .route("/events", post(create_event).get(query_events))
        .route("/events/:event_id", get(get_event))
        .route("/ingest/http", post(ingest_http))
        .route("/raw-messages", get(query_raw_messages))
        .route("/raw-messages/:raw_message_id", get(get_raw_message))
        .route(
            "/observations",
            post(create_observation).get(query_observations),
        )
        .with_state(state)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "aion-api",
        storage: "memory",
    })
}

async fn create_entity(
    State(state): State<AppState>,
    Json(request): Json<Value>,
) -> Result<(StatusCode, Json<Entity>), ApiError> {
    let request = parse_entity_input(request)?;
    let entity = Entity::new(
        state.tenant_id,
        request.entity_key,
        request.entity_type,
        request.jsonld,
        Utc::now(),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let entity = state.storage.create_entity(entity)?;
    Ok((StatusCode::CREATED, Json(entity)))
}

fn parse_entity_input(value: Value) -> Result<EntityInput, ApiError> {
    if value.get("jsonld").is_some() {
        let request: CreateEntityRequest =
            serde_json::from_value(value).map_err(|err| ApiError::bad_request(err.to_string()))?;
        return Ok(EntityInput {
            entity_key: request.entity_key,
            entity_type: request.entity_type,
            jsonld: request.jsonld,
        });
    }

    let object = value
        .as_object()
        .ok_or_else(|| ApiError::bad_request("entity request must be a JSON object"))?;

    if !object.contains_key("@context") {
        return Err(ApiError::bad_request(
            "native JSON-LD entity must include @context",
        ));
    }

    let jsonld_id = object
        .get("@id")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::bad_request("native JSON-LD entity must include string @id"))?;
    let entity_type = extract_jsonld_type(object.get("@type"))
        .ok_or_else(|| ApiError::bad_request("native JSON-LD entity must include string @type"))?;
    let entity_key = extract_jsonld_entity_key(object)
        .or_else(|| derive_entity_key(jsonld_id))
        .ok_or_else(|| {
            ApiError::bad_request("could not derive entity_key from native JSON-LD @id")
        })?;

    Ok(EntityInput {
        entity_key,
        entity_type,
        jsonld: value,
    })
}

fn extract_jsonld_type(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.clone()),
        Some(Value::Array(values)) => values
            .iter()
            .find_map(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        _ => None,
    }
}

fn extract_jsonld_entity_key(object: &serde_json::Map<String, Value>) -> Option<String> {
    object
        .get("entity_key")
        .or_else(|| object.get("aion:entityKey"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn derive_entity_key(jsonld_id: &str) -> Option<String> {
    let trimmed = jsonld_id.trim();
    if trimmed.is_empty() {
        return None;
    }

    let segments = trimmed
        .split(['/', '#', ':'])
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let last = segments.last()?;

    if is_generic_numeric_suffix(last) {
        return segments
            .iter()
            .rev()
            .skip(1)
            .find(|segment| !is_generic_numeric_suffix(segment))
            .map(|prefix| format!("{prefix}-{last}"));
    }

    Some((*last).to_string())
}

fn is_generic_numeric_suffix(segment: &str) -> bool {
    segment.chars().all(|character| character.is_ascii_digit())
}

async fn get_entity(
    State(state): State<AppState>,
    Path(entity_id): Path<Uuid>,
) -> Result<Json<Entity>, ApiError> {
    let entity = state
        .storage
        .get_entity(state.tenant_id, entity_id)?
        .ok_or_else(ApiError::not_found)?;

    Ok(Json(entity))
}

async fn list_entities(State(state): State<AppState>) -> Result<Json<Vec<Entity>>, ApiError> {
    Ok(Json(state.storage.list_entities(state.tenant_id)?))
}

async fn create_relationship(
    State(state): State<AppState>,
    Json(request): Json<CreateRelationshipRequest>,
) -> Result<(StatusCode, Json<Relationship>), ApiError> {
    ensure_entity_exists(&state, request.source_entity_id)?;
    ensure_entity_exists(&state, request.target_entity_id)?;

    let relationship = Relationship::new(
        state.tenant_id,
        request.source_entity_id,
        request.relationship_type,
        request.target_entity_id,
        request.jsonld,
        Utc::now(),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let relationship = state.storage.create_relationship(relationship)?;
    Ok((StatusCode::CREATED, Json(relationship)))
}

async fn put_payload_profile(
    State(state): State<AppState>,
    Path(entity_id): Path<Uuid>,
    Json(request): Json<PutPayloadProfileRequest>,
) -> Result<(StatusCode, Json<PayloadProfile>), ApiError> {
    ensure_entity_exists(&state, entity_id)?;
    let profile = PayloadProfile::new(
        entity_id,
        request.payload_format,
        request.protocol,
        request.content_type,
        request.attribute_mapping,
        request.metadata,
    )?;
    let profile = state
        .storage
        .put_payload_profile(state.tenant_id, profile)?;

    Ok((StatusCode::OK, Json(profile)))
}

async fn get_payload_profile(
    State(state): State<AppState>,
    Path(entity_id): Path<Uuid>,
) -> Result<Json<PayloadProfile>, ApiError> {
    ensure_entity_exists(&state, entity_id)?;
    let profile = state
        .storage
        .get_payload_profile(state.tenant_id, entity_id)?
        .ok_or_else(ApiError::not_found)?;

    Ok(Json(profile))
}

async fn get_entity_context(
    State(state): State<AppState>,
    Path(entity_id): Path<Uuid>,
) -> Result<Json<EntityContextResponse>, ApiError> {
    let entity = state
        .storage
        .get_entity(state.tenant_id, entity_id)?
        .ok_or_else(ApiError::not_found)?;

    let outgoing_relationships =
        state
            .storage
            .list_relationships(state.tenant_id, Some(entity_id), None)?;
    let incoming_relationships =
        state
            .storage
            .list_relationships(state.tenant_id, None, Some(entity_id))?;

    Ok(Json(EntityContextResponse {
        entity,
        outgoing_relationships,
        incoming_relationships,
    }))
}

async fn put_capabilities(
    State(state): State<AppState>,
    Path(entity_id): Path<Uuid>,
    Json(requests): Json<Vec<PutCapabilityRequest>>,
) -> Result<(StatusCode, Json<Vec<Capability>>), ApiError> {
    ensure_entity_exists(&state, entity_id)?;
    let capabilities = requests
        .into_iter()
        .map(|request| {
            Capability::new(
                entity_id,
                request.capability_name,
                request.command_type,
                request.protocol,
                request.metadata,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let capabilities = state
        .storage
        .put_capabilities(state.tenant_id, entity_id, capabilities)?;
    Ok((StatusCode::OK, Json(capabilities)))
}

async fn get_capabilities(
    State(state): State<AppState>,
    Path(entity_id): Path<Uuid>,
) -> Result<Json<Vec<Capability>>, ApiError> {
    ensure_entity_exists(&state, entity_id)?;
    Ok(Json(
        state
            .storage
            .list_capabilities(state.tenant_id, entity_id)?,
    ))
}

async fn put_policies(
    State(state): State<AppState>,
    Json(requests): Json<Vec<PutPolicyRequest>>,
) -> Result<(StatusCode, Json<Vec<Policy>>), ApiError> {
    for request in &requests {
        if let Some(target_entity_id) = request.target_entity_id {
            ensure_entity_exists(&state, target_entity_id)?;
        }
    }

    let policies = requests
        .into_iter()
        .map(|request| {
            Policy::new(
                state.tenant_id,
                request.target_entity_id,
                request.command_type,
                request.requires_approval,
                request.auto_execute_allowed,
                request.metadata,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let policies = state.storage.put_policies(state.tenant_id, policies)?;
    Ok((StatusCode::OK, Json(policies)))
}

async fn query_policies(
    State(state): State<AppState>,
    Query(query): Query<PolicyQuery>,
) -> Result<Json<Vec<Policy>>, ApiError> {
    Ok(Json(state.storage.query_policies(
        state.tenant_id,
        query.target_entity_id,
        query.command_type.as_deref(),
    )?))
}

async fn create_command(
    State(state): State<AppState>,
    Json(request): Json<CreateCommandRequest>,
) -> Result<(StatusCode, Json<Command>), ApiError> {
    ensure_entity_exists(&state, request.target_entity_id)?;
    let (approval_status, policy_decision) =
        command_policy_decision(&state, request.target_entity_id, &request.command_type)?;
    let command = Command::new(
        state.tenant_id,
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

    let command = state.storage.store_command(command)?;
    record_command_event(
        &state,
        "aion:CommandCreated",
        EventSeverity::Info,
        &command,
        None,
    )?;
    Ok((StatusCode::CREATED, Json(command)))
}

async fn claim_command(
    State(state): State<AppState>,
    Path(command_id): Path<Uuid>,
    Json(request): Json<ClaimCommandRequest>,
) -> Result<Json<Command>, ApiError> {
    mutate_command(
        &state,
        command_id,
        "aion:CommandClaimed",
        EventSeverity::Info,
        |command, now| command.claim(request.claimed_by, now),
    )
}

async fn release_command(
    State(state): State<AppState>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    mutate_command(
        &state,
        command_id,
        "aion:CommandReleased",
        EventSeverity::Info,
        |command, now| command.release(now),
    )
}

async fn mark_command_executed(
    State(state): State<AppState>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    mutate_command(
        &state,
        command_id,
        "aion:CommandExecuted",
        EventSeverity::Info,
        |command, now| command.mark_executed(now),
    )
}

async fn mark_command_failed(
    State(state): State<AppState>,
    Path(command_id): Path<Uuid>,
    Json(request): Json<MarkFailedCommandRequest>,
) -> Result<Json<Command>, ApiError> {
    mutate_command(
        &state,
        command_id,
        "aion:CommandFailed",
        EventSeverity::Error,
        |command, now| command.mark_failed(request.failure_reason, now),
    )
}

async fn cancel_command(
    State(state): State<AppState>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    mutate_command(
        &state,
        command_id,
        "aion:CommandCancelled",
        EventSeverity::Warning,
        |command, now| command.cancel(now),
    )
}

async fn approve_command(
    State(state): State<AppState>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    mutate_command(
        &state,
        command_id,
        "aion:CommandApproved",
        EventSeverity::Info,
        |command, now| command.approve(now),
    )
}

async fn reject_command(
    State(state): State<AppState>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    mutate_command(
        &state,
        command_id,
        "aion:CommandRejected",
        EventSeverity::Warning,
        |command, now| command.reject(now),
    )
}

async fn get_command(
    State(state): State<AppState>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    let command = state
        .storage
        .get_command(state.tenant_id, command_id)?
        .ok_or_else(ApiError::not_found)?;

    Ok(Json(command))
}

async fn query_commands(
    State(state): State<AppState>,
    Query(query): Query<CommandQuery>,
) -> Result<Json<Vec<Command>>, ApiError> {
    Ok(Json(state.storage.query_commands(
        state.tenant_id,
        query.target_entity_id,
        query.status,
    )?))
}

async fn create_action(
    State(state): State<AppState>,
    Json(request): Json<CreateActionRequest>,
) -> Result<(StatusCode, Json<Action>), ApiError> {
    ensure_command_exists(&state, request.command_id)?;
    if let Some(executor_entity_id) = request.executor_entity_id {
        ensure_entity_exists(&state, executor_entity_id)?;
    }

    let action = Action::new(
        state.tenant_id,
        request.command_id,
        request.executor_entity_id,
        request.action_type,
        request.status,
        request.started_at,
        request.finished_at,
        request.metadata,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let action = state.storage.store_action(action)?;
    Ok((StatusCode::CREATED, Json(action)))
}

async fn get_action(
    State(state): State<AppState>,
    Path(action_id): Path<Uuid>,
) -> Result<Json<Action>, ApiError> {
    let action = state
        .storage
        .get_action(state.tenant_id, action_id)?
        .ok_or_else(ApiError::not_found)?;

    Ok(Json(action))
}

async fn query_actions(
    State(state): State<AppState>,
    Query(query): Query<ActionQuery>,
) -> Result<Json<Vec<Action>>, ApiError> {
    Ok(Json(
        state
            .storage
            .query_actions(state.tenant_id, query.command_id)?,
    ))
}

async fn create_action_result(
    State(state): State<AppState>,
    Json(request): Json<CreateActionResultRequest>,
) -> Result<(StatusCode, Json<ActionResult>), ApiError> {
    ensure_command_exists(&state, request.command_id)?;
    let action = state
        .storage
        .get_action(state.tenant_id, request.action_id)?
        .ok_or_else(ApiError::not_found)?;
    if action.command_id != request.command_id {
        return Err(ApiError::bad_request(
            "action_id does not belong to command_id",
        ));
    }

    let result = ActionResult::new(
        state.tenant_id,
        request.command_id,
        request.action_id,
        request.status,
        request.verified,
        request.result_payload,
        request.observed_at,
        request.metadata,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let result = state.storage.store_action_result(result)?;
    Ok((StatusCode::CREATED, Json(result)))
}

async fn query_action_results(
    State(state): State<AppState>,
    Query(query): Query<ActionResultQuery>,
) -> Result<Json<Vec<ActionResult>>, ApiError> {
    Ok(Json(state.storage.query_action_results(
        state.tenant_id,
        query.action_id,
        query.command_id,
    )?))
}

async fn create_event(
    State(state): State<AppState>,
    Json(request): Json<CreateEventRequest>,
) -> Result<(StatusCode, Json<Event>), ApiError> {
    if let Some(source_entity_id) = request.source_entity_id {
        ensure_entity_exists(&state, source_entity_id)?;
    }
    if let Some(target_entity_id) = request.target_entity_id {
        ensure_entity_exists(&state, target_entity_id)?;
    }
    if let Some(command_id) = request.command_id {
        ensure_command_exists(&state, command_id)?;
    }
    if let Some(action_id) = request.action_id {
        ensure_action_exists(&state, action_id)?;
    }
    if let Some(action_result_id) = request.action_result_id {
        ensure_action_result_exists(&state, action_result_id)?;
    }
    if let Some(raw_message_id) = request.raw_message_id {
        ensure_raw_message_exists(&state, raw_message_id)?;
    }

    let event = Event::new(
        state.tenant_id,
        request.event_type,
        request.severity,
        request.source_entity_id,
        request.target_entity_id,
        request.message,
        request.occurred_at,
        request.observed_at,
        request.correlation_id,
        request.raw_message_id,
        request.observation_id,
        request.command_id,
        request.action_id,
        request.action_result_id,
        request.metadata,
        Utc::now(),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let event = state.storage.store_event(event)?;
    Ok((StatusCode::CREATED, Json(event)))
}

async fn get_event(
    State(state): State<AppState>,
    Path(event_id): Path<Uuid>,
) -> Result<Json<Event>, ApiError> {
    let event = state
        .storage
        .get_event(state.tenant_id, event_id)?
        .ok_or_else(ApiError::not_found)?;

    Ok(Json(event))
}

async fn query_events(
    State(state): State<AppState>,
    Query(query): Query<EventQuery>,
) -> Result<Json<Vec<Event>>, ApiError> {
    Ok(Json(state.storage.query_events(
        state.tenant_id,
        EventFilter {
            source_entity_id: query.source_entity_id,
            target_entity_id: query.target_entity_id,
            event_type: query.event_type,
            severity: query.severity,
            command_id: query.command_id,
            raw_message_id: query.raw_message_id,
            correlation_id: query.correlation_id,
        },
    )?))
}

async fn create_observation(
    State(state): State<AppState>,
    Json(request): Json<CreateObservationRequest>,
) -> Result<(StatusCode, Json<Observation>), ApiError> {
    ensure_entity_exists(&state, request.producer_entity_id)?;
    ensure_entity_exists(&state, request.feature_of_interest_id)?;

    let observation = Observation::new(
        state.tenant_id,
        request.producer_entity_id,
        request.feature_of_interest_id,
        request.observed_property,
        request.value,
        request.unit,
        request.observed_at,
        request.received_at,
        request.protocol,
        request.payload_format,
        request.raw_message_id,
        request.quality,
        request.metadata,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let observation = state.storage.store_observation(observation)?;
    Ok((StatusCode::CREATED, Json(observation)))
}

async fn query_observations(
    State(state): State<AppState>,
    Query(query): Query<ObservationQuery>,
) -> Result<Json<Vec<Observation>>, ApiError> {
    let observations = state.storage.query_observations(
        state.tenant_id,
        query.feature_of_interest_id,
        query.observed_property.as_deref(),
        None,
        None,
        query.limit.unwrap_or(100),
    )?;
    let observations = if let Some(raw_message_id) = query.raw_message_id {
        observations
            .into_iter()
            .filter(|observation| observation.raw_message_id == Some(raw_message_id))
            .collect::<Vec<_>>()
    } else {
        observations
    };

    Ok(Json(observations))
}

async fn get_raw_message(
    State(state): State<AppState>,
    Path(raw_message_id): Path<Uuid>,
) -> Result<Json<RawMessageResponse>, ApiError> {
    let raw_message = state
        .storage
        .get_raw_message(state.tenant_id, raw_message_id)?
        .ok_or_else(ApiError::not_found)?;

    Ok(Json(raw_message_response(raw_message)))
}

async fn query_raw_messages(
    State(state): State<AppState>,
    Query(query): Query<RawMessageQuery>,
) -> Result<Json<Vec<RawMessageResponse>>, ApiError> {
    let raw_messages = state
        .storage
        .list_raw_messages(state.tenant_id)?
        .into_iter()
        .filter(|raw_message| {
            query
                .producer_entity_id
                .map(|id| raw_message_uuid_header(raw_message, "producer_entity_id") == Some(id))
                .unwrap_or(true)
        })
        .filter(|raw_message| {
            query
                .feature_of_interest_id
                .map(|id| {
                    raw_message_uuid_header(raw_message, "feature_of_interest_id") == Some(id)
                })
                .unwrap_or(true)
        })
        .filter(|raw_message| {
            query
                .payload_format
                .as_deref()
                .map(|payload_format| {
                    raw_message_string_header(raw_message, "payload_format")
                        .map(|value| value.eq_ignore_ascii_case(payload_format))
                        .unwrap_or(false)
                })
                .unwrap_or(true)
        })
        .map(raw_message_response)
        .collect::<Vec<_>>();

    Ok(Json(raw_messages))
}

async fn ingest_http(
    State(state): State<AppState>,
    Json(request): Json<HttpIngestRequest>,
) -> Result<(StatusCode, Json<HttpIngestResponse>), ApiError> {
    ensure_entity_exists(&state, request.producer_entity_id)?;
    ensure_entity_exists(&state, request.feature_of_interest_id)?;

    let received_at = Utc::now();
    let payload_bytes = payload_to_bytes(&request.payload);
    let profile = state
        .storage
        .get_payload_profile(state.tenant_id, request.producer_entity_id)?;
    let mapping_source = if request.mapping.is_some() {
        "request"
    } else if profile
        .as_ref()
        .and_then(|profile| profile.attribute_mapping.as_ref())
        .is_some()
    {
        "payload_profile"
    } else {
        "none"
    };
    let mut raw_message = RawMessage::new(
        state.tenant_id,
        RawMessageSource::Http,
        Some("/ingest/http".to_string()),
        Some(request.producer_entity_id.to_string()),
        Some(request.payload_format.clone()),
        request.content_type.clone(),
        json!({
            "protocol": request.protocol,
            "payload_format": request.payload_format,
            "producer_entity_id": request.producer_entity_id,
            "feature_of_interest_id": request.feature_of_interest_id,
            "decoder_metadata": {
                "decoder": request.payload_format,
                "mapping_source": mapping_source
            }
        }),
        payload_bytes.clone(),
        received_at,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    raw_message = state.storage.store_raw_message(raw_message)?;

    let decoder_config = request
        .mapping
        .or_else(|| profile.and_then(|profile| profile.attribute_mapping));
    if payload_format_requires_mapping(&request.payload_format) && decoder_config.is_none() {
        let message = format!(
            "{} payloads require request mapping or producer PayloadProfile attribute_mapping",
            request.payload_format
        );
        state
            .storage
            .mark_raw_message_failed(state.tenant_id, raw_message.id, &message)?;
        record_ingest_event(
            &state,
            "aion:PayloadIngestionFailed",
            EventSeverity::Error,
            request.producer_entity_id,
            request.feature_of_interest_id,
            raw_message.id,
            Some(message.clone()),
            json!({
                "payload_format": request.payload_format,
                "reason": "missing_mapping"
            }),
        )?;
        return Err(ApiError::bad_request(message));
    }

    let decoder = match decoder_for_format(&request.payload_format) {
        Ok(decoder) => decoder,
        Err(err) => {
            state
                .storage
                .mark_raw_message_failed(state.tenant_id, raw_message.id, &err.message)?;
            record_ingest_event(
                &state,
                "aion:PayloadIngestionFailed",
                EventSeverity::Error,
                request.producer_entity_id,
                request.feature_of_interest_id,
                raw_message.id,
                Some(err.message.clone()),
                json!({
                    "payload_format": request.payload_format,
                    "reason": "unsupported_payload_format"
                }),
            )?;
            return Err(err);
        }
    };
    let decode_result = decoder.decode(DecodeInput {
        tenant_id: state.tenant_id,
        device_key: Some(request.producer_entity_id.to_string()),
        format: PayloadFormat::from_str(&request.payload_format).unwrap(),
        content_type: request.content_type,
        payload: payload_bytes,
        received_at: request.observed_at.unwrap_or(received_at),
        config: decoder_config,
    });

    let decoded = match decode_result {
        Ok(decoded) => decoded,
        Err(err) => {
            state.storage.mark_raw_message_failed(
                state.tenant_id,
                raw_message.id,
                err.message(),
            )?;
            record_ingest_event(
                &state,
                "aion:PayloadIngestionFailed",
                EventSeverity::Error,
                request.producer_entity_id,
                request.feature_of_interest_id,
                raw_message.id,
                Some(err.message().to_string()),
                json!({
                    "payload_format": request.payload_format,
                    "reason": "decoder_error"
                }),
            )?;
            return Err(ApiError::bad_request(err.to_string()));
        }
    };

    let mut observations = Vec::with_capacity(decoded.len());
    for measurement in decoded {
        let observation = Observation::new(
            state.tenant_id,
            request.producer_entity_id,
            request.feature_of_interest_id,
            measurement.observed_property,
            measurement.value,
            measurement.unit,
            measurement.time,
            received_at,
            request.protocol.clone(),
            request.payload_format.clone(),
            Some(raw_message.id),
            json!({}),
            measurement.metadata,
        )
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
        observations.push(state.storage.store_observation(observation)?);
    }

    state
        .storage
        .mark_raw_message_normalized(state.tenant_id, raw_message.id)?;
    record_ingest_event(
        &state,
        "aion:PayloadIngested",
        EventSeverity::Info,
        request.producer_entity_id,
        request.feature_of_interest_id,
        raw_message.id,
        Some("Payload ingested and normalized".to_string()),
        json!({
            "payload_format": request.payload_format,
            "observation_count": observations.len()
        }),
    )?;

    Ok((
        StatusCode::CREATED,
        Json(HttpIngestResponse {
            raw_message_id: raw_message.id,
            observations,
        }),
    ))
}

fn decoder_for_format(payload_format: &str) -> Result<Box<dyn PayloadDecoder>, ApiError> {
    let normalized = payload_format.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "senml" | "senml_json" => Ok(Box::new(SenMlJsonDecoder)),
        "ultralight" | "ultra_light" => Ok(Box::new(UltraLightDecoder)),
        "canonical_json" | "canonical" => Ok(Box::new(CanonicalJsonDecoder)),
        _ => Err(ApiError::bad_request(format!(
            "unsupported payload_format: {payload_format}"
        ))),
    }
}

fn payload_format_requires_mapping(payload_format: &str) -> bool {
    matches!(
        payload_format
            .trim()
            .to_ascii_lowercase()
            .replace('-', "_")
            .as_str(),
        "ultralight" | "ultra_light"
    )
}

fn payload_to_bytes(payload: &Value) -> Vec<u8> {
    payload
        .as_str()
        .map(|value| value.as_bytes().to_vec())
        .unwrap_or_else(|| payload.to_string().into_bytes())
}

fn raw_message_response(raw_message: RawMessage) -> RawMessageResponse {
    let protocol = raw_message_string_header(&raw_message, "protocol");
    let payload_format = raw_message_string_header(&raw_message, "payload_format")
        .or(raw_message.decoder_hint.clone());
    let producer_entity_id = raw_message_uuid_header(&raw_message, "producer_entity_id");
    let feature_of_interest_id = raw_message_uuid_header(&raw_message, "feature_of_interest_id");
    let decoder_metadata = raw_message
        .headers
        .get("decoder_metadata")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let payload = raw_payload_value(&raw_message.payload);

    RawMessageResponse {
        id: raw_message.id,
        raw_message_id: raw_message.id,
        source_type: raw_message.source_type,
        protocol,
        content_type: raw_message.content_type,
        payload_format,
        producer_entity_id,
        feature_of_interest_id,
        received_at: raw_message.received_at,
        normalization_status: raw_message.normalization_status,
        normalization_error: raw_message.normalization_error,
        decoder_metadata,
        payload,
    }
}

fn raw_message_string_header(raw_message: &RawMessage, key: &str) -> Option<String> {
    raw_message
        .headers
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn raw_message_uuid_header(raw_message: &RawMessage, key: &str) -> Option<Uuid> {
    raw_message
        .headers
        .get(key)
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
}

fn raw_payload_value(payload: &[u8]) -> Value {
    serde_json::from_slice(payload).unwrap_or_else(|_| {
        String::from_utf8(payload.to_vec())
            .map(Value::String)
            .unwrap_or_else(|_| json!({"encoding": "binary", "byte_length": payload.len()}))
    })
}

fn ensure_entity_exists(state: &AppState, entity_id: Uuid) -> Result<(), ApiError> {
    state
        .storage
        .get_entity(state.tenant_id, entity_id)?
        .map(|_| ())
        .ok_or_else(ApiError::not_found)
}

fn ensure_command_exists(state: &AppState, command_id: Uuid) -> Result<(), ApiError> {
    state
        .storage
        .get_command(state.tenant_id, command_id)?
        .map(|_| ())
        .ok_or_else(ApiError::not_found)
}

fn ensure_action_exists(state: &AppState, action_id: Uuid) -> Result<(), ApiError> {
    state
        .storage
        .get_action(state.tenant_id, action_id)?
        .map(|_| ())
        .ok_or_else(ApiError::not_found)
}

fn ensure_action_result_exists(state: &AppState, action_result_id: Uuid) -> Result<(), ApiError> {
    state
        .storage
        .query_action_results(state.tenant_id, None, None)?
        .into_iter()
        .find(|result| result.id == action_result_id)
        .map(|_| ())
        .ok_or_else(ApiError::not_found)
}

fn ensure_raw_message_exists(state: &AppState, raw_message_id: Uuid) -> Result<(), ApiError> {
    state
        .storage
        .get_raw_message(state.tenant_id, raw_message_id)?
        .map(|_| ())
        .ok_or_else(ApiError::not_found)
}

fn record_command_event(
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

#[allow(clippy::too_many_arguments)]
fn record_ingest_event(
    state: &AppState,
    event_type: impl Into<String>,
    severity: EventSeverity,
    source_entity_id: Uuid,
    target_entity_id: Uuid,
    raw_message_id: Uuid,
    message: Option<String>,
    metadata: Value,
) -> Result<Event, ApiError> {
    record_event(
        state,
        EventDraft {
            event_type: event_type.into(),
            severity,
            source_entity_id: Some(source_entity_id),
            target_entity_id: Some(target_entity_id),
            message,
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: None,
            raw_message_id: Some(raw_message_id),
            observation_id: None,
            command_id: None,
            action_id: None,
            action_result_id: None,
            metadata: Some(metadata),
        },
    )
}

fn record_event(state: &AppState, draft: EventDraft) -> Result<Event, ApiError> {
    let now = Utc::now();
    let event = Event::new(
        state.tenant_id,
        draft.event_type,
        draft.severity,
        draft.source_entity_id,
        draft.target_entity_id,
        draft.message,
        draft.occurred_at,
        draft.observed_at,
        draft.correlation_id,
        draft.raw_message_id,
        draft.observation_id,
        draft.command_id,
        draft.action_id,
        draft.action_result_id,
        draft.metadata,
        now,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    Ok(state.storage.store_event(event)?)
}

struct EventDraft {
    event_type: String,
    severity: EventSeverity,
    source_entity_id: Option<Uuid>,
    target_entity_id: Option<Uuid>,
    message: Option<String>,
    occurred_at: DateTime<Utc>,
    observed_at: Option<DateTime<Utc>>,
    correlation_id: Option<String>,
    raw_message_id: Option<Uuid>,
    observation_id: Option<Uuid>,
    command_id: Option<Uuid>,
    action_id: Option<Uuid>,
    action_result_id: Option<Uuid>,
    metadata: Option<Value>,
}

fn mutate_command(
    state: &AppState,
    command_id: Uuid,
    event_type: &'static str,
    severity: EventSeverity,
    mutate: impl FnOnce(&mut Command, DateTime<Utc>) -> Result<(), aion_action::ActionModelError>,
) -> Result<Json<Command>, ApiError> {
    let mut command = state
        .storage
        .get_command(state.tenant_id, command_id)?
        .ok_or_else(ApiError::not_found)?;
    mutate(&mut command, Utc::now()).map_err(|err| ApiError::bad_request(err.to_string()))?;
    let command = state.storage.update_command(command)?;
    record_command_event(state, event_type, severity, &command, None)?;
    Ok(Json(command))
}

fn command_policy_decision(
    state: &AppState,
    target_entity_id: Uuid,
    command_type: &str,
) -> Result<(ApprovalStatus, Value), ApiError> {
    let policies = state.storage.query_policies(state.tenant_id, None, None)?;
    let mut matching_policies = policies
        .into_iter()
        .filter(|policy| policy.matches(target_entity_id, command_type))
        .collect::<Vec<_>>();

    matching_policies.sort_by_key(|policy| {
        (
            policy.target_entity_id.is_none(),
            policy.command_type.is_none(),
            policy.id,
        )
    });

    let requires_approval = matching_policies
        .iter()
        .any(|policy| policy.requires_approval);
    let auto_execute_allowed = matching_policies
        .iter()
        .any(|policy| policy.auto_execute_allowed);
    let approval_status = if requires_approval {
        ApprovalStatus::Required
    } else {
        ApprovalStatus::NotRequired
    };
    let matched_policy_ids = matching_policies
        .iter()
        .map(|policy| policy.id)
        .collect::<Vec<_>>();
    let matched_policy_count = matched_policy_ids.len();

    Ok((
        approval_status,
        json!({
            "matched_policy_ids": matched_policy_ids,
            "matched_policy_count": matched_policy_count,
            "requires_approval": requires_approval,
            "auto_execute_allowed": auto_execute_allowed,
            "safe_default": matched_policy_count == 0
        }),
    ))
}

fn empty_object() -> Value {
    json!({})
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: "record was not found".to_string(),
        }
    }
}

impl From<StorageError> for ApiError {
    fn from(value: StorageError) -> Self {
        match value {
            StorageError::NotFound => Self::not_found(),
            StorageError::Conflict => Self {
                status: StatusCode::CONFLICT,
                message: value.to_string(),
            },
            StorageError::InvalidInput(message) => Self::bad_request(message),
            StorageError::Backend(message) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message,
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use serde_json::json;
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_reports_memory_storage() {
        let response = app()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_json(response).await;
        assert_eq!(body["status"], "ok");
        assert_eq!(body["storage"], "memory");
    }

    #[tokio::test]
    async fn creates_entity_from_envelope_and_returns_context() {
        let app = app();
        let entity_body = json!({
            "entity_key": "sensor-01",
            "entity_type": "aion:Sensor",
            "jsonld": {
                "@context": {"aion": "https://aioncore.org/ns#"},
                "@id": "urn:aion:sensor:sensor-01",
                "@type": "aion:Sensor",
                "name": "Sensor 01"
            }
        });

        let response = app
            .clone()
            .oneshot(json_request("POST", "/entities", entity_body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let entity = to_json(response).await;
        let entity_id = entity["id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/entities/{entity_id}/context"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let context = to_json(response).await;
        assert_eq!(context["entity"]["entity_key"], "sensor-01");
        assert_eq!(
            context["outgoing_relationships"].as_array().unwrap().len(),
            0
        );
        assert_eq!(
            context["incoming_relationships"].as_array().unwrap().len(),
            0
        );
    }

    #[tokio::test]
    async fn creates_entity_from_native_jsonld() {
        let response = app()
            .oneshot(json_ld_request(
                "POST",
                "/entities",
                json!({
                    "@context": {"aion": "https://aioncore.org/ns#"},
                    "@id": "urn:aion:sensor:sensor-ld-01",
                    "@type": "aion:Sensor",
                    "name": "Sensor LD 01"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let entity = to_json(response).await;
        assert_eq!(entity["entity_key"], "sensor-ld-01");
        assert_eq!(entity["entity_type"], "aion:Sensor");
        assert_eq!(entity["jsonld"]["@id"], "urn:aion:sensor:sensor-ld-01");
        assert_eq!(entity["jsonld"]["name"], "Sensor LD 01");
    }

    #[test]
    fn derives_entity_key_from_native_jsonld_fields_first() {
        let explicit = json!({
            "entity_key": "explicit-zone-key",
            "aion:entityKey": "semantic-zone-key"
        });
        assert_eq!(
            extract_jsonld_entity_key(explicit.as_object().unwrap()).as_deref(),
            Some("explicit-zone-key")
        );

        let semantic = json!({
            "aion:entityKey": "semantic-zone-key"
        });
        assert_eq!(
            extract_jsonld_entity_key(semantic.as_object().unwrap()).as_deref(),
            Some("semantic-zone-key")
        );
    }

    #[test]
    fn derives_semantic_entity_key_from_jsonld_id() {
        assert_eq!(
            derive_entity_key("urn:aion:farm:01:zone:01").as_deref(),
            Some("zone-01")
        );
        assert_eq!(
            derive_entity_key("urn:aion:farm:01:soil-sensor:01").as_deref(),
            Some("soil-sensor-01")
        );
        assert_eq!(
            derive_entity_key("urn:aion:sensor:runtime-jsonld-01").as_deref(),
            Some("runtime-jsonld-01")
        );
    }

    #[tokio::test]
    async fn creates_native_jsonld_entities_with_numeric_suffixes_without_conflict() {
        let app = app();

        let zone_response = app
            .clone()
            .oneshot(json_ld_request(
                "POST",
                "/entities",
                json!({
                    "@context": {"aion": "https://aioncore.org/ns#"},
                    "@id": "urn:aion:farm:01:zone:01",
                    "@type": "aion:IrrigationZone",
                    "name": "Zone 01"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(zone_response.status(), StatusCode::CREATED);
        let zone = to_json(zone_response).await;
        assert_eq!(zone["entity_key"], "zone-01");

        let sensor_response = app
            .oneshot(json_ld_request(
                "POST",
                "/entities",
                json!({
                    "@context": {"aion": "https://aioncore.org/ns#"},
                    "@id": "urn:aion:farm:01:soil-sensor:01",
                    "@type": "aion:SoilSensor",
                    "name": "Soil Sensor 01"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(sensor_response.status(), StatusCode::CREATED);
        let sensor = to_json(sensor_response).await;
        assert_eq!(sensor["entity_key"], "soil-sensor-01");
    }

    #[tokio::test]
    async fn creates_and_queries_observation() {
        let app = app();
        let sensor_id = create_test_entity(&app, "sensor-01", "aion:Sensor").await;
        let room_id = create_test_entity(&app, "room-01", "aion:Room").await;

        let observation_body = json!({
            "producer_entity_id": sensor_id,
            "feature_of_interest_id": room_id,
            "observed_property": "temperature",
            "value": {"type": "number", "value": 21.4},
            "unit": "Cel",
            "observed_at": "2026-04-27T13:00:00Z",
            "received_at": "2026-04-27T13:00:01Z",
            "protocol": "http",
            "payload_format": "json_mapping",
            "quality": {},
            "metadata": {}
        });

        let response = app
            .clone()
            .oneshot(json_request("POST", "/observations", observation_body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/observations?feature_of_interest_id={room_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let observations = to_json(response).await;
        assert_eq!(observations.as_array().unwrap().len(), 1);
        assert_eq!(observations[0]["observed_property"], "temperature");
    }

    #[tokio::test]
    async fn creates_payload_profile() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;

        let response = app
            .oneshot(json_request(
                "PUT",
                &format!("/entities/{sensor_id}/payload-profile"),
                json!({
                    "payload_format": "ultralight",
                    "protocol": "http",
                    "content_type": "text/plain",
                    "attribute_mapping": {
                        "m": {
                            "observed_property": "aion:SoilMoisture",
                            "unit": "%"
                        }
                    },
                    "metadata": {
                        "profile_version": 1
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let profile = to_json(response).await;
        assert_eq!(profile["entity_id"], sensor_id);
        assert_eq!(profile["payload_format"], "ultralight");
        assert_eq!(
            profile["attribute_mapping"]["m"]["observed_property"],
            "aion:SoilMoisture"
        );
    }

    #[tokio::test]
    async fn retrieves_payload_profile() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/entities/{sensor_id}/payload-profile"),
                json!({
                    "payload_format": "ultralight",
                    "protocol": "http",
                    "content_type": "text/plain",
                    "attribute_mapping": {
                        "t": {
                            "observed_property": "aion:SoilTemperature",
                            "unit": "Cel"
                        }
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/entities/{sensor_id}/payload-profile"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let profile = to_json(response).await;
        assert_eq!(profile["entity_id"], sensor_id);
        assert_eq!(
            profile["attribute_mapping"]["t"]["observed_property"],
            "aion:SoilTemperature"
        );
    }

    #[tokio::test]
    async fn manages_capabilities_commands_actions_and_results() {
        let app = app();
        let pump_id = create_test_entity(&app, "pump-01", "aion:Pump").await;
        let executor_id = create_test_entity(&app, "executor-01", "aion:Executor").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/entities/{pump_id}/capabilities"),
                json!([
                    {
                        "capability_name": "StartPump",
                        "command_type": "StartPump",
                        "protocol": "http",
                        "metadata": {
                            "description": "Start pump motor"
                        }
                    },
                    {
                        "capability_name": "StopPump",
                        "command_type": "StopPump",
                        "protocol": "http"
                    }
                ]),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let capabilities = to_json(response).await;
        assert_eq!(capabilities.as_array().unwrap().len(), 2);
        assert_eq!(capabilities[0]["entity_id"], pump_id);
        assert_eq!(capabilities[0]["capability_name"], "StartPump");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/entities/{pump_id}/capabilities"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let capabilities = to_json(response).await;
        assert_eq!(capabilities.as_array().unwrap().len(), 2);

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/commands",
                json!({
                    "target_entity_id": pump_id,
                    "command_type": "StartPump",
                    "payload": {
                        "target_state": "running"
                    },
                    "requested_by": "operator@example.com",
                    "reason": "water tank below minimum level"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let command = to_json(response).await;
        let command_id = command["id"].as_str().unwrap();
        assert_eq!(command["target_entity_id"], pump_id);
        assert_eq!(command["status"], "pending");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/commands/{command_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(to_json(response).await["id"], command_id);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/commands?target_entity_id={pump_id}&status=pending"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let commands = to_json(response).await;
        assert_eq!(commands.as_array().unwrap().len(), 1);
        assert_eq!(commands[0]["id"], command_id);

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/actions",
                json!({
                    "command_id": command_id,
                    "executor_entity_id": executor_id,
                    "action_type": "StartPump",
                    "status": "started",
                    "started_at": "2026-04-27T13:00:00Z",
                    "metadata": {
                        "external_correlation_id": "exec-001"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let action = to_json(response).await;
        let action_id = action["id"].as_str().unwrap();
        assert_eq!(action["command_id"], command_id);
        assert_eq!(action["executor_entity_id"], executor_id);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/actions/{action_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(to_json(response).await["id"], action_id);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/actions?command_id={command_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let actions = to_json(response).await;
        assert_eq!(actions.as_array().unwrap().len(), 1);
        assert_eq!(actions[0]["command_id"], command_id);

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/action-results",
                json!({
                    "command_id": command_id,
                    "action_id": action_id,
                    "status": "succeeded",
                    "verified": true,
                    "result_payload": {
                        "pump_state": "running"
                    },
                    "observed_at": "2026-04-27T13:00:05Z",
                    "metadata": {
                        "verification_source": "simulated_executor"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let result = to_json(response).await;
        assert_eq!(result["command_id"], command_id);
        assert_eq!(result["action_id"], action_id);
        assert_eq!(result["verified"], true);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/action-results?action_id={action_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let results = to_json(response).await;
        assert_eq!(results.as_array().unwrap().len(), 1);
        assert_eq!(results[0]["action_id"], action_id);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/action-results?command_id={command_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let results = to_json(response).await;
        assert_eq!(results.as_array().unwrap().len(), 1);
        assert_eq!(results[0]["command_id"], command_id);
        assert_eq!(results[0]["action_id"], action_id);
    }

    #[tokio::test]
    async fn claims_pending_command_and_rejects_second_claim() {
        let app = app();
        let pump_id = create_test_entity(&app, "pump-claim-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/commands/{command_id}/claim"),
                json!({"claimed_by": "executor-01"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let command = to_json(response).await;
        assert_eq!(command["status"], "claimed");
        assert_eq!(command["claimed_by"], "executor-01");

        let response = app
            .oneshot(json_request(
                "POST",
                &format!("/commands/{command_id}/claim"),
                json!({"claimed_by": "executor-02"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(to_json(response).await["error"]
            .as_str()
            .unwrap()
            .contains("only be claimed when status is pending"));
    }

    #[tokio::test]
    async fn releases_claimed_command_back_to_pending() {
        let app = app();
        let pump_id = create_test_entity(&app, "pump-release-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        claim_test_command(&app, command_id, "executor-01").await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/commands/{command_id}/release"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let command = to_json(response).await;
        assert_eq!(command["status"], "pending");
        assert!(command["claimed_by"].is_null());
        assert!(command["claimed_at"].is_null());
    }

    #[tokio::test]
    async fn marks_claimed_command_executed() {
        let app = app();
        let pump_id = create_test_entity(&app, "pump-executed-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        claim_test_command(&app, command_id, "executor-01").await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/commands/{command_id}/mark-executed"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let command = to_json(response).await;
        assert_eq!(command["status"], "executed");
        assert!(command["completed_at"].as_str().is_some());
    }

    #[tokio::test]
    async fn marks_claimed_command_failed() {
        let app = app();
        let pump_id = create_test_entity(&app, "pump-failed-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        claim_test_command(&app, command_id, "executor-01").await;

        let response = app
            .oneshot(json_request(
                "POST",
                &format!("/commands/{command_id}/mark-failed"),
                json!({"failure_reason": "controller timeout"}),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let command = to_json(response).await;
        assert_eq!(command["status"], "failed");
        assert_eq!(command["failure_reason"], "controller timeout");
        assert!(command["completed_at"].as_str().is_some());
    }

    #[tokio::test]
    async fn cancels_pending_command() {
        let app = app();
        let pump_id = create_test_entity(&app, "pump-cancel-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/commands/{command_id}/cancel"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let command = to_json(response).await;
        assert_eq!(command["status"], "cancelled");
        assert!(command["completed_at"].as_str().is_some());
    }

    #[tokio::test]
    async fn policy_requires_approval_before_claim() {
        let app = app();
        let pump_id = create_test_entity(&app, "pump-policy-01", "aion:Pump").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "PUT",
                "/policies",
                json!([
                    {
                        "target_entity_id": pump_id,
                        "command_type": "StartPump",
                        "requires_approval": true,
                        "auto_execute_allowed": false,
                        "metadata": {
                            "reason": "physical actuation"
                        }
                    }
                ]),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let policies = to_json(response).await;
        assert_eq!(policies.as_array().unwrap().len(), 1);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/policies?target_entity_id={pump_id}&command_type=StartPump"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(to_json(response).await.as_array().unwrap().len(), 1);

        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        assert_eq!(command["approval_status"], "required");
        assert_eq!(command["policy_decision"]["requires_approval"], true);

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/commands/{command_id}/claim"),
                json!({"claimed_by": "executor-01"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(to_json(response).await["error"]
            .as_str()
            .unwrap()
            .contains("requires approval"));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/commands/{command_id}/approve"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(to_json(response).await["approval_status"], "approved");

        let claimed = claim_test_command(&app, command_id, "executor-01").await;
        assert_eq!(claimed["status"], "claimed");
    }

    #[tokio::test]
    async fn rejected_command_cannot_be_claimed() {
        let app = app();
        let pump_id = create_test_entity(&app, "pump-rejected-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/commands/{command_id}/reject"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(to_json(response).await["approval_status"], "rejected");

        let response = app
            .oneshot(json_request(
                "POST",
                &format!("/commands/{command_id}/claim"),
                json!({"claimed_by": "executor-01"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(to_json(response).await["error"]
            .as_str()
            .unwrap()
            .contains("rejected"));
    }

    #[tokio::test]
    async fn creates_retrieves_and_filters_events() {
        let app = app();
        let source_id = create_test_entity(&app, "event-source-01", "aion:Sensor").await;
        let target_id = create_test_entity(&app, "event-target-01", "aion:Pump").await;
        let command = create_test_command(&app, &target_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/events",
                json!({
                    "event_type": "aion:ManualAuditEvent",
                    "severity": "warning",
                    "source_entity_id": source_id,
                    "target_entity_id": target_id,
                    "message": "Manual audit event",
                    "occurred_at": "2026-04-27T13:00:00Z",
                    "correlation_id": "manual-event-001",
                    "command_id": command_id,
                    "metadata": {
                        "source": "test"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let event = to_json(response).await;
        let event_id = event["id"].as_str().unwrap();
        assert_eq!(event["event_type"], "aion:ManualAuditEvent");
        assert_eq!(event["severity"], "warning");
        assert_eq!(event["target_entity_id"], target_id);
        assert_eq!(event["command_id"], command_id);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/events/{event_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(to_json(response).await["id"], event_id);

        for uri in [
            format!("/events?target_entity_id={target_id}"),
            "/events?event_type=aion:ManualAuditEvent".to_string(),
            "/events?severity=warning".to_string(),
            format!("/events?command_id={command_id}"),
            "/events?correlation_id=manual-event-001".to_string(),
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let events = to_json(response).await;
            assert!(events
                .as_array()
                .unwrap()
                .iter()
                .any(|event| event["id"] == event_id));
        }
    }

    #[tokio::test]
    async fn ingestion_success_creates_payload_ingested_event() {
        let app = app();
        let sensor_id = create_test_entity(&app, "event-soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "event-plot-01", "aion:Plot").await;
        let ingest = ingest_test_senml(&app, &sensor_id, &plot_id).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/events?raw_message_id={raw_message_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let events = to_json(response).await;
        assert_eq!(events.as_array().unwrap().len(), 1);
        assert_eq!(events[0]["event_type"], "aion:PayloadIngested");
        assert_eq!(events[0]["severity"], "info");
        assert_eq!(events[0]["raw_message_id"], raw_message_id);
        assert_eq!(events[0]["source_entity_id"], sensor_id);
        assert_eq!(events[0]["target_entity_id"], plot_id);
    }

    #[tokio::test]
    async fn command_lifecycle_transitions_create_events() {
        let app = app();
        let pump_id = create_test_entity(&app, "event-pump-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/commands/{command_id}/approve"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        claim_test_command(&app, command_id, "executor-01").await;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/commands/{command_id}/mark-executed"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/events?command_id={command_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let events = to_json(response).await;
        let event_types = events
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["event_type"].as_str().unwrap())
            .collect::<Vec<_>>();

        assert!(event_types.contains(&"aion:CommandCreated"));
        assert!(event_types.contains(&"aion:CommandApproved"));
        assert!(event_types.contains(&"aion:CommandClaimed"));
        assert!(event_types.contains(&"aion:CommandExecuted"));
    }

    #[tokio::test]
    async fn ingests_senml_json_payload() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;

        let response = app
            .oneshot(json_request(
                "POST",
                "/ingest/http",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload_format": "senml-json",
                    "protocol": "http",
                    "content_type": "application/senml+json",
                    "payload": [
                        {
                            "bn": "urn:aion:farm:01:soil-sensor:01:",
                            "bt": 1777294800,
                            "n": "soil_moisture",
                            "u": "%",
                            "v": 18.5
                        },
                        {
                            "n": "soil_temperature",
                            "u": "Cel",
                            "v": 24.1
                        }
                    ]
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let ingest = to_json(response).await;
        assert!(ingest["raw_message_id"].as_str().is_some());
        assert_eq!(ingest["observations"].as_array().unwrap().len(), 2);
        assert_eq!(
            ingest["observations"][0]["observed_property"],
            "soil_moisture"
        );
        assert_eq!(
            ingest["observations"][1]["observed_property"],
            "soil_temperature"
        );
    }

    #[tokio::test]
    async fn queries_raw_message_by_id() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;
        let ingest = ingest_test_senml(&app, &sensor_id, &plot_id).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/raw-messages/{raw_message_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let raw_message = to_json(response).await;
        assert_eq!(raw_message["id"], raw_message_id);
        assert_eq!(raw_message["raw_message_id"], raw_message_id);
        assert_eq!(raw_message["protocol"], "http");
        assert_eq!(raw_message["content_type"], "application/senml+json");
        assert_eq!(raw_message["payload_format"], "senml-json");
        assert_eq!(raw_message["producer_entity_id"], sensor_id);
        assert_eq!(raw_message["feature_of_interest_id"], plot_id);
        assert_eq!(raw_message["normalization_status"], "normalized");
        assert_eq!(raw_message["decoder_metadata"]["decoder"], "senml-json");
        assert_eq!(raw_message["payload"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn queries_raw_messages_by_producer_entity_id() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;
        let ingest = ingest_test_senml(&app, &sensor_id, &plot_id).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/raw-messages?producer_entity_id={sensor_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let raw_messages = to_json(response).await;
        assert_eq!(raw_messages.as_array().unwrap().len(), 1);
        assert_eq!(raw_messages[0]["id"], raw_message_id);
    }

    #[tokio::test]
    async fn queries_raw_messages_by_feature_of_interest_id() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;
        let other_plot_id = create_test_entity(&app, "plot-02", "aion:Plot").await;
        let ingest = ingest_test_senml(&app, &sensor_id, &plot_id).await;
        ingest_test_senml(&app, &sensor_id, &other_plot_id).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/raw-messages?feature_of_interest_id={plot_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let raw_messages = to_json(response).await;
        assert_eq!(raw_messages.as_array().unwrap().len(), 1);
        assert_eq!(raw_messages[0]["raw_message_id"], raw_message_id);
        assert_eq!(raw_messages[0]["feature_of_interest_id"], plot_id);
    }

    #[tokio::test]
    async fn queries_raw_messages_by_payload_format() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;
        let ingest = ingest_test_senml(&app, &sensor_id, &plot_id).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/raw-messages?payload_format=senml-json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let raw_messages = to_json(response).await;
        assert_eq!(raw_messages.as_array().unwrap().len(), 1);
        assert_eq!(raw_messages[0]["id"], raw_message_id);
    }

    #[tokio::test]
    async fn raw_message_is_linked_to_generated_observations() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;
        let ingest = ingest_test_senml(&app, &sensor_id, &plot_id).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/observations?raw_message_id={raw_message_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let observations = to_json(response).await;
        assert_eq!(observations.as_array().unwrap().len(), 2);
        assert!(observations
            .as_array()
            .unwrap()
            .iter()
            .all(|observation| observation["raw_message_id"] == raw_message_id));
    }

    #[tokio::test]
    async fn ingests_ultralight_payload() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;

        let response = app
            .oneshot(json_request(
                "POST",
                "/ingest/http",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload_format": "ultralight",
                    "protocol": "http",
                    "content_type": "text/plain",
                    "observed_at": "2026-04-27T13:00:00Z",
                    "payload": "m|18.5|t|24.1",
                    "mapping": {
                        "m": {
                            "observed_property": "aion:SoilMoisture",
                            "unit": "%"
                        },
                        "t": {
                            "observed_property": "aion:SoilTemperature",
                            "unit": "Cel"
                        }
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let ingest = to_json(response).await;
        assert_eq!(ingest["observations"].as_array().unwrap().len(), 2);
        assert_eq!(
            ingest["observations"][0]["observed_property"],
            "aion:SoilMoisture"
        );
        assert_eq!(ingest["observations"][0]["unit"], "%");
        assert_eq!(
            ingest["observations"][1]["observed_property"],
            "aion:SoilTemperature"
        );
    }

    #[tokio::test]
    async fn ingests_ultralight_payload_using_payload_profile() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/entities/{sensor_id}/payload-profile"),
                json!({
                    "payload_format": "ultralight",
                    "protocol": "http",
                    "content_type": "text/plain",
                    "attribute_mapping": {
                        "m": {
                            "observed_property": "aion:SoilMoisture",
                            "unit": "%"
                        },
                        "t": {
                            "observed_property": "aion:SoilTemperature",
                            "unit": "Cel"
                        }
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(json_request(
                "POST",
                "/ingest/http",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload_format": "ultralight",
                    "protocol": "http",
                    "content_type": "text/plain",
                    "observed_at": "2026-04-27T13:00:00Z",
                    "payload": "m|18.5|t|24.1"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let ingest = to_json(response).await;
        assert_eq!(ingest["observations"].as_array().unwrap().len(), 2);
        assert_eq!(
            ingest["observations"][0]["observed_property"],
            "aion:SoilMoisture"
        );
        assert_eq!(
            ingest["observations"][1]["observed_property"],
            "aion:SoilTemperature"
        );
    }

    #[tokio::test]
    async fn rejects_ultralight_payload_without_mapping_or_payload_profile() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;

        let response = app
            .oneshot(json_request(
                "POST",
                "/ingest/http",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload_format": "ultralight",
                    "protocol": "http",
                    "content_type": "text/plain",
                    "observed_at": "2026-04-27T13:00:00Z",
                    "payload": "m|18.5|t|24.1"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error = to_json(response).await;
        assert!(error["error"]
            .as_str()
            .unwrap()
            .contains("request mapping or producer PayloadProfile attribute_mapping"));
    }

    #[tokio::test]
    async fn ingests_canonical_json_payload() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingest/http",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload_format": "canonical-json",
                    "protocol": "http",
                    "content_type": "application/json",
                    "payload": {
                        "observations": [
                            {
                                "observed_property": "aion:SoilMoisture",
                                "value": {"type": "number", "value": 18.5},
                                "unit": "%",
                                "observed_at": "2026-04-27T13:00:00Z"
                            }
                        ]
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let ingest = to_json(response).await;
        assert_eq!(ingest["observations"].as_array().unwrap().len(), 1);
        assert_eq!(
            ingest["observations"][0]["raw_message_id"],
            ingest["raw_message_id"]
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/observations?feature_of_interest_id={plot_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let observations = to_json(response).await;
        assert_eq!(observations.as_array().unwrap().len(), 1);
        assert_eq!(observations[0]["observed_property"], "aion:SoilMoisture");
    }

    #[tokio::test]
    async fn rejects_invalid_ingest_payload_after_raw_storage() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;

        let response = app
            .oneshot(json_request(
                "POST",
                "/ingest/http",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload_format": "senml-json",
                    "protocol": "http",
                    "content_type": "application/senml+json",
                    "payload": "not json"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error = to_json(response).await;
        assert!(error["error"]
            .as_str()
            .unwrap()
            .contains("invalid SenML JSON payload"));
    }

    async fn create_test_entity(app: &Router, key: &str, entity_type: &str) -> String {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/entities",
                json!({
                    "entity_key": key,
                    "entity_type": entity_type,
                    "jsonld": {
                        "@context": {"aion": "https://aioncore.org/ns#"},
                        "@id": format!("urn:aion:test:{key}"),
                        "@type": entity_type
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await["id"].as_str().unwrap().to_string()
    }

    async fn create_test_command(
        app: &Router,
        target_entity_id: &str,
        command_type: &str,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/commands",
                json!({
                    "target_entity_id": target_entity_id,
                    "command_type": command_type,
                    "payload": {
                        "target_state": "running"
                    },
                    "requested_by": "test",
                    "reason": "test command"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn claim_test_command(app: &Router, command_id: &str, claimed_by: &str) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/commands/{command_id}/claim"),
                json!({
                    "claimed_by": claimed_by
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn ingest_test_senml(app: &Router, sensor_id: &str, plot_id: &str) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingest/http",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload_format": "senml-json",
                    "protocol": "http",
                    "content_type": "application/senml+json",
                    "payload": [
                        {
                            "bn": "urn:aion:farm:01:soil-sensor:01:",
                            "bt": 1777294800,
                            "n": "soil_moisture",
                            "u": "%",
                            "v": 18.5
                        },
                        {
                            "n": "soil_temperature",
                            "u": "Cel",
                            "v": 24.1
                        }
                    ]
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    fn json_request(method: &str, uri: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    fn json_ld_request(method: &str, uri: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/ld+json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    async fn to_json(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }
}
