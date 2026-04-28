use aion_action::{
    Action, ActionResult, ApprovalStatus, Capability, Command, CommandStatus, Policy,
};
use aion_entity::Entity;
use aion_event::{Event, EventSeverity};
use aion_mcp::{ToolDefinition, ToolRequest, ToolResponse};
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
    body::Bytes,
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

#[derive(Debug, Deserialize)]
pub struct AiContextQuery {
    pub include_observations: Option<bool>,
    pub include_events: Option<bool>,
    pub include_commands: Option<bool>,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct McpRecentObservationsArgs {
    pub feature_of_interest_id: Option<Uuid>,
    pub producer_entity_id: Option<Uuid>,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct McpEventsArgs {
    pub entity_id: Option<Uuid>,
    pub event_type: Option<String>,
    pub severity: Option<EventSeverity>,
    pub command_id: Option<Uuid>,
    pub raw_message_id: Option<Uuid>,
    pub correlation_id: Option<String>,
    pub limit: Option<u32>,
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
pub struct AiEntityContextResponse {
    pub target_entity: Entity,
    pub outgoing_relationships: Vec<Relationship>,
    pub incoming_relationships: Vec<Relationship>,
    pub recent_observations: Vec<Observation>,
    pub recent_events: Vec<Event>,
    pub related_commands: Vec<Command>,
    pub related_actions: Vec<Action>,
    pub related_action_results: Vec<ActionResult>,
    pub raw_message_refs: Vec<Uuid>,
    pub generated_at: DateTime<Utc>,
    pub metadata: Value,
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
        .route("/ai/context/entity/:entity_id", get(get_ai_entity_context))
        .route("/mcp", post(handle_mcp_json_rpc))
        .route("/mcp/tools", get(list_mcp_tools))
        .route("/mcp/tools/:tool_name", post(invoke_mcp_tool))
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

async fn get_ai_entity_context(
    State(state): State<AppState>,
    Path(entity_id): Path<Uuid>,
    Query(query): Query<AiContextQuery>,
) -> Result<Json<AiEntityContextResponse>, ApiError> {
    Ok(Json(build_ai_entity_context(&state, entity_id, query)?))
}

fn build_ai_entity_context(
    state: &AppState,
    entity_id: Uuid,
    query: AiContextQuery,
) -> Result<AiEntityContextResponse, ApiError> {
    let target_entity = state
        .storage
        .get_entity(state.tenant_id, entity_id)?
        .ok_or_else(ApiError::not_found)?;

    let limit = query.limit.unwrap_or(10);
    let include_observations = query.include_observations.unwrap_or(true);
    let include_events = query.include_events.unwrap_or(true);
    let include_commands = query.include_commands.unwrap_or(true);

    let outgoing_relationships =
        state
            .storage
            .list_relationships(state.tenant_id, Some(entity_id), None)?;
    let incoming_relationships =
        state
            .storage
            .list_relationships(state.tenant_id, None, Some(entity_id))?;

    let recent_observations = if include_observations {
        state.storage.query_observations(
            state.tenant_id,
            Some(entity_id),
            None,
            None,
            None,
            limit,
        )?
    } else {
        Vec::new()
    };

    let recent_events = if include_events {
        query_events_for_entity(&state, entity_id, limit)?
    } else {
        Vec::new()
    };

    let related_commands = if include_commands {
        state
            .storage
            .query_commands(state.tenant_id, Some(entity_id), None)?
            .into_iter()
            .take(limit as usize)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    let mut related_actions = Vec::new();
    let mut related_action_results = Vec::new();
    if include_commands {
        for command in &related_commands {
            related_actions.extend(
                state
                    .storage
                    .query_actions(state.tenant_id, Some(command.id))?,
            );
            related_action_results.extend(state.storage.query_action_results(
                state.tenant_id,
                None,
                Some(command.id),
            )?);
        }
        related_actions.sort_by(|left, right| {
            right
                .started_at
                .cmp(&left.started_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        related_action_results.sort_by(|left, right| {
            right
                .observed_at
                .cmp(&left.observed_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        related_actions.truncate(limit as usize);
        related_action_results.truncate(limit as usize);
    }

    let mut raw_message_refs = Vec::new();
    for raw_message_id in recent_observations
        .iter()
        .filter_map(|observation| observation.raw_message_id)
        .chain(
            recent_events
                .iter()
                .filter_map(|event| event.raw_message_id),
        )
    {
        if !raw_message_refs.contains(&raw_message_id) {
            raw_message_refs.push(raw_message_id);
        }
    }

    Ok(AiEntityContextResponse {
        target_entity,
        outgoing_relationships,
        incoming_relationships,
        recent_observations,
        recent_events,
        related_commands,
        related_actions,
        related_action_results,
        raw_message_refs,
        generated_at: Utc::now(),
        metadata: json!({
            "builder": "aion:AiContextBuilder",
            "domain_agnostic": true,
            "llm_invoked": false,
            "include_observations": include_observations,
            "include_events": include_events,
            "include_commands": include_commands,
            "limit": limit
        }),
    })
}

fn query_events_for_entity(
    state: &AppState,
    entity_id: Uuid,
    limit: u32,
) -> Result<Vec<Event>, ApiError> {
    let mut events = state.storage.query_events(
        state.tenant_id,
        EventFilter {
            target_entity_id: Some(entity_id),
            ..Default::default()
        },
    )?;

    for event in state.storage.query_events(
        state.tenant_id,
        EventFilter {
            source_entity_id: Some(entity_id),
            ..Default::default()
        },
    )? {
        if !events.iter().any(|existing| existing.id == event.id) {
            events.push(event);
        }
    }

    events.sort_by(|left, right| {
        right
            .occurred_at
            .cmp(&left.occurred_at)
            .then_with(|| right.id.cmp(&left.id))
    });
    events.truncate(limit as usize);
    Ok(events)
}

async fn list_mcp_tools() -> Json<Vec<ToolDefinition>> {
    Json(mcp_tool_definitions())
}

async fn handle_mcp_json_rpc(
    State(state): State<AppState>,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    let request = match serde_json::from_slice::<Value>(&body) {
        Ok(request) => request,
        Err(error) => {
            return (
                StatusCode::OK,
                Json(json_rpc_error(
                    Value::Null,
                    -32700,
                    format!("parse error: {error}"),
                    None,
                )),
            );
        }
    };

    let object = match request.as_object() {
        Some(object) => object,
        None => {
            return (
                StatusCode::OK,
                Json(json_rpc_error(
                    Value::Null,
                    -32600,
                    "invalid JSON-RPC request",
                    None,
                )),
            );
        }
    };

    let id = object.get("id").cloned().unwrap_or(Value::Null);
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return (
            StatusCode::OK,
            Json(json_rpc_error(id, -32600, "jsonrpc must be \"2.0\"", None)),
        );
    }

    let Some(method) = object.get("method").and_then(Value::as_str) else {
        return (
            StatusCode::OK,
            Json(json_rpc_error(id, -32600, "method is required", None)),
        );
    };

    let response = match method {
        "tools/list" => json_rpc_success(
            id,
            json!({
                "tools": mcp_tool_definitions()
                    .into_iter()
                    .map(mcp_compatible_tool_definition)
                    .collect::<Vec<_>>()
            }),
        ),
        "tools/call" => match parse_mcp_tools_call_params(object.get("params")) {
            Ok((tool_name, arguments)) => {
                match invoke_local_mcp_tool(&state, &tool_name, arguments) {
                    Ok(content) => json_rpc_success(id, mcp_compatible_tool_result(content)),
                    Err(error) => json_rpc_error(
                        id,
                        json_rpc_code_for_tool_failure(&error),
                        error.message,
                        Some(json!({
                            "code": error.code,
                            "isError": true
                        })),
                    ),
                }
            }
            Err(error) => json_rpc_error(
                id,
                -32602,
                error.message,
                Some(json!({
                    "code": error.code,
                    "isError": true
                })),
            ),
        },
        _ => json_rpc_error(
            id,
            -32601,
            format!("unknown JSON-RPC method '{method}'"),
            None,
        ),
    };

    (StatusCode::OK, Json(response))
}

async fn invoke_mcp_tool(
    State(state): State<AppState>,
    Path(tool_name): Path<String>,
    Json(request): Json<ToolRequest>,
) -> (StatusCode, Json<ToolResponse>) {
    match invoke_local_mcp_tool(&state, &tool_name, request.arguments) {
        Ok(content) => (
            StatusCode::OK,
            Json(ToolResponse::success(tool_name, content)),
        ),
        Err(error) => (
            error.status,
            Json(ToolResponse::error(tool_name, error.code, error.message)),
        ),
    }
}

fn mcp_tool_definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "list_entities".to_string(),
            description: "List known entities with compact identity metadata.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDefinition {
            name: "get_entity".to_string(),
            description: "Get one entity by entity_id.".to_string(),
            input_schema: json!({
                "type": "object",
                "required": ["entity_id"],
                "properties": {
                    "entity_id": {"type": "string", "format": "uuid"}
                }
            }),
        },
        ToolDefinition {
            name: "get_entity_context".to_string(),
            description: "Get one entity with incoming and outgoing relationships.".to_string(),
            input_schema: json!({
                "type": "object",
                "required": ["entity_id"],
                "properties": {
                    "entity_id": {"type": "string", "format": "uuid"}
                }
            }),
        },
        ToolDefinition {
            name: "get_recent_observations".to_string(),
            description: "Get recent observations by feature_of_interest_id or producer_entity_id."
                .to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "feature_of_interest_id": {"type": "string", "format": "uuid"},
                    "producer_entity_id": {"type": "string", "format": "uuid"},
                    "limit": {"type": "integer", "minimum": 1}
                }
            }),
        },
        ToolDefinition {
            name: "get_events".to_string(),
            description: "Get events by entity or optional event filters.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "entity_id": {"type": "string", "format": "uuid"},
                    "event_type": {"type": "string"},
                    "severity": {"type": "string"},
                    "command_id": {"type": "string", "format": "uuid"},
                    "raw_message_id": {"type": "string", "format": "uuid"},
                    "correlation_id": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1}
                }
            }),
        },
        ToolDefinition {
            name: "get_pending_commands".to_string(),
            description: "Get pending commands, optionally scoped to a target entity.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "target_entity_id": {"type": "string", "format": "uuid"}
                }
            }),
        },
        ToolDefinition {
            name: "build_ai_context".to_string(),
            description: "Build the AI context package for an entity.".to_string(),
            input_schema: json!({
                "type": "object",
                "required": ["entity_id"],
                "properties": {
                    "entity_id": {"type": "string", "format": "uuid"},
                    "include_observations": {"type": "boolean"},
                    "include_events": {"type": "boolean"},
                    "include_commands": {"type": "boolean"},
                    "limit": {"type": "integer", "minimum": 1}
                }
            }),
        },
    ]
}

fn mcp_compatible_tool_definition(tool: ToolDefinition) -> Value {
    json!({
        "name": tool.name,
        "description": tool.description,
        "inputSchema": mcp_compatible_input_schema(tool.input_schema)
    })
}

fn mcp_compatible_input_schema(input_schema: Value) -> Value {
    let has_parameters = input_schema
        .get("properties")
        .and_then(Value::as_object)
        .map(|properties| !properties.is_empty())
        .unwrap_or(false)
        || input_schema
            .get("required")
            .and_then(Value::as_array)
            .map(|required| !required.is_empty())
            .unwrap_or(false);

    if has_parameters {
        input_schema
    } else {
        json!({
            "type": "object",
            "additionalProperties": false
        })
    }
}

fn parse_mcp_tools_call_params(params: Option<&Value>) -> Result<(String, Value), McpToolFailure> {
    let params = params.ok_or_else(|| {
        McpToolFailure::bad_request("missing_params", "params is required for tools/call")
    })?;
    let object = params.as_object().ok_or_else(|| {
        McpToolFailure::bad_request("invalid_params", "params must be a JSON object")
    })?;
    let tool_name = object
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| McpToolFailure::bad_request("missing_argument", "params.name is required"))?
        .to_string();
    let arguments = object
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !arguments.is_object() {
        return Err(McpToolFailure::bad_request(
            "invalid_arguments",
            "params.arguments must be a JSON object",
        ));
    }

    Ok((tool_name, arguments))
}

fn mcp_compatible_tool_result(content: Value) -> Value {
    let text = serde_json::to_string(&content)
        .unwrap_or_else(|_| "{\"error\":\"failed to serialize tool result\"}".to_string());

    json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "structuredContent": content,
        "isError": false
    })
}

fn json_rpc_success(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
}

fn json_rpc_error(id: Value, code: i64, message: impl Into<String>, data: Option<Value>) -> Value {
    let mut error = json!({
        "code": code,
        "message": message.into()
    });
    if let Some(data) = data {
        if let Some(object) = error.as_object_mut() {
            object.insert("data".to_string(), data);
        }
    }

    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": error
    })
}

fn json_rpc_code_for_tool_failure(error: &McpToolFailure) -> i64 {
    match error.status {
        StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST => -32602,
        _ => -32000,
    }
}

fn invoke_local_mcp_tool(
    state: &AppState,
    tool_name: &str,
    arguments: Value,
) -> Result<Value, McpToolFailure> {
    match tool_name {
        "list_entities" => mcp_list_entities(state),
        "get_entity" => mcp_get_entity(state, &arguments),
        "get_entity_context" => mcp_get_entity_context(state, &arguments),
        "get_recent_observations" => mcp_get_recent_observations(state, arguments),
        "get_events" => mcp_get_events(state, arguments),
        "get_pending_commands" => mcp_get_pending_commands(state, &arguments),
        "build_ai_context" => mcp_build_ai_context(state, arguments),
        _ => Err(McpToolFailure::not_found(format!(
            "unknown MCP tool '{tool_name}'"
        ))),
    }
}

fn mcp_list_entities(state: &AppState) -> Result<Value, McpToolFailure> {
    let entities = state
        .storage
        .list_entities(state.tenant_id)
        .map_err(McpToolFailure::from_storage)?
        .into_iter()
        .map(|entity| {
            json!({
                "id": entity.id,
                "entity_key": entity.entity_key,
                "entity_type": entity.entity_type,
                "jsonld_id": entity.jsonld.get("@id").and_then(Value::as_str)
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({ "entities": entities }))
}

fn mcp_get_entity(state: &AppState, arguments: &Value) -> Result<Value, McpToolFailure> {
    let entity_id = required_uuid(arguments, "entity_id")?;
    let entity = state
        .storage
        .get_entity(state.tenant_id, entity_id)
        .map_err(McpToolFailure::from_storage)?
        .ok_or_else(|| McpToolFailure::not_found("entity was not found"))?;

    Ok(json!({ "entity": entity }))
}

fn mcp_get_entity_context(state: &AppState, arguments: &Value) -> Result<Value, McpToolFailure> {
    let entity_id = required_uuid(arguments, "entity_id")?;
    let entity = state
        .storage
        .get_entity(state.tenant_id, entity_id)
        .map_err(McpToolFailure::from_storage)?
        .ok_or_else(|| McpToolFailure::not_found("entity was not found"))?;
    let outgoing_relationships = state
        .storage
        .list_relationships(state.tenant_id, Some(entity_id), None)
        .map_err(McpToolFailure::from_storage)?;
    let incoming_relationships = state
        .storage
        .list_relationships(state.tenant_id, None, Some(entity_id))
        .map_err(McpToolFailure::from_storage)?;

    Ok(json!({
        "entity": entity,
        "outgoing_relationships": outgoing_relationships,
        "incoming_relationships": incoming_relationships
    }))
}

fn mcp_get_recent_observations(
    state: &AppState,
    arguments: Value,
) -> Result<Value, McpToolFailure> {
    let args: McpRecentObservationsArgs = parse_tool_args(arguments)?;
    let limit = args.limit.unwrap_or(10);
    if args.feature_of_interest_id.is_none() && args.producer_entity_id.is_none() {
        return Err(McpToolFailure::bad_request(
            "missing_argument",
            "feature_of_interest_id or producer_entity_id is required",
        ));
    }

    let query_limit = if args.producer_entity_id.is_some() {
        u32::MAX
    } else {
        limit
    };
    let mut observations = state
        .storage
        .query_observations(
            state.tenant_id,
            args.feature_of_interest_id,
            None,
            None,
            None,
            query_limit,
        )
        .map_err(McpToolFailure::from_storage)?;

    if let Some(producer_entity_id) = args.producer_entity_id {
        observations.retain(|observation| observation.producer_entity_id == producer_entity_id);
        observations.truncate(limit as usize);
    }

    Ok(json!({ "observations": observations }))
}

fn mcp_get_events(state: &AppState, arguments: Value) -> Result<Value, McpToolFailure> {
    let args: McpEventsArgs = parse_tool_args(arguments)?;
    let limit = args.limit.unwrap_or(10);
    let filter = EventFilter {
        event_type: args.event_type,
        severity: args.severity,
        command_id: args.command_id,
        raw_message_id: args.raw_message_id,
        correlation_id: args.correlation_id,
        ..Default::default()
    };

    let mut events = if let Some(entity_id) = args.entity_id {
        let mut target_filter = filter.clone();
        target_filter.target_entity_id = Some(entity_id);
        let mut events = state
            .storage
            .query_events(state.tenant_id, target_filter)
            .map_err(McpToolFailure::from_storage)?;

        let mut source_filter = filter;
        source_filter.source_entity_id = Some(entity_id);
        for event in state
            .storage
            .query_events(state.tenant_id, source_filter)
            .map_err(McpToolFailure::from_storage)?
        {
            if !events.iter().any(|existing| existing.id == event.id) {
                events.push(event);
            }
        }
        events.sort_by(|left, right| {
            right
                .occurred_at
                .cmp(&left.occurred_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        events
    } else {
        state
            .storage
            .query_events(state.tenant_id, filter)
            .map_err(McpToolFailure::from_storage)?
    };

    events.truncate(limit as usize);
    Ok(json!({ "events": events }))
}

fn mcp_get_pending_commands(state: &AppState, arguments: &Value) -> Result<Value, McpToolFailure> {
    let target_entity_id = optional_uuid(arguments, "target_entity_id")?;
    let commands = state
        .storage
        .query_commands(
            state.tenant_id,
            target_entity_id,
            Some(CommandStatus::Pending),
        )
        .map_err(McpToolFailure::from_storage)?;

    Ok(json!({ "commands": commands }))
}

fn mcp_build_ai_context(state: &AppState, arguments: Value) -> Result<Value, McpToolFailure> {
    let entity_id = required_uuid(&arguments, "entity_id")?;
    let query = AiContextQuery {
        include_observations: optional_bool(&arguments, "include_observations")?,
        include_events: optional_bool(&arguments, "include_events")?,
        include_commands: optional_bool(&arguments, "include_commands")?,
        limit: optional_u32(&arguments, "limit")?,
    };
    let context =
        build_ai_entity_context(state, entity_id, query).map_err(McpToolFailure::from_api)?;

    Ok(json!({ "context": context }))
}

fn parse_tool_args<T>(arguments: Value) -> Result<T, McpToolFailure>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(arguments).map_err(|err| {
        McpToolFailure::bad_request("invalid_arguments", format!("invalid arguments: {err}"))
    })
}

fn required_uuid(arguments: &Value, field: &str) -> Result<Uuid, McpToolFailure> {
    optional_uuid(arguments, field)?.ok_or_else(|| {
        McpToolFailure::bad_request("missing_argument", format!("{field} is required"))
    })
}

fn optional_uuid(arguments: &Value, field: &str) -> Result<Option<Uuid>, McpToolFailure> {
    match arguments.get(field) {
        Some(Value::String(value)) => Uuid::parse_str(value).map(Some).map_err(|err| {
            McpToolFailure::bad_request(
                "invalid_argument",
                format!("{field} must be a UUID: {err}"),
            )
        }),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(McpToolFailure::bad_request(
            "invalid_argument",
            format!("{field} must be a UUID string"),
        )),
    }
}

fn optional_bool(arguments: &Value, field: &str) -> Result<Option<bool>, McpToolFailure> {
    match arguments.get(field) {
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(McpToolFailure::bad_request(
            "invalid_argument",
            format!("{field} must be a boolean"),
        )),
    }
}

fn optional_u32(arguments: &Value, field: &str) -> Result<Option<u32>, McpToolFailure> {
    match arguments.get(field) {
        Some(Value::Number(value)) => value
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .map(Some)
            .ok_or_else(|| {
                McpToolFailure::bad_request(
                    "invalid_argument",
                    format!("{field} must be a non-negative integer within u32 range"),
                )
            }),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(McpToolFailure::bad_request(
            "invalid_argument",
            format!("{field} must be an integer"),
        )),
    }
}

#[derive(Debug)]
struct McpToolFailure {
    status: StatusCode,
    code: String,
    message: String,
}

impl McpToolFailure {
    fn bad_request(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: code.into(),
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found".to_string(),
            message: message.into(),
        }
    }

    fn from_storage(error: StorageError) -> Self {
        match error {
            StorageError::NotFound => Self::not_found("record was not found"),
            StorageError::InvalidInput(message) => Self::bad_request("invalid_input", message),
            StorageError::Conflict => Self {
                status: StatusCode::CONFLICT,
                code: "conflict".to_string(),
                message: "record conflicts with existing data".to_string(),
            },
            StorageError::Backend(message) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "backend_error".to_string(),
                message,
            },
        }
    }

    fn from_api(error: ApiError) -> Self {
        Self {
            status: error.status,
            code: match error.status {
                StatusCode::NOT_FOUND => "not_found",
                StatusCode::BAD_REQUEST => "invalid_arguments",
                _ => "tool_error",
            }
            .to_string(),
            message: error.message,
        }
    }
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
    async fn builds_ai_context_for_entity_with_relationships_only() {
        let app = app();
        let tank_id = create_test_entity(&app, "context-tank-01", "aion:WaterTank").await;
        let pump_id = create_test_entity(&app, "context-pump-01", "aion:Pump").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/relationships",
                json!({
                    "source_entity_id": pump_id,
                    "relationship_type": "aion:fills",
                    "target_entity_id": tank_id,
                    "jsonld": {
                        "@type": "aion:Relationship"
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/ai/context/entity/{tank_id}?include_observations=false&include_events=false&include_commands=false"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let context = to_json(response).await;
        assert_eq!(context["target_entity"]["id"], tank_id);
        assert_eq!(
            context["incoming_relationships"].as_array().unwrap().len(),
            1
        );
        assert_eq!(
            context["outgoing_relationships"].as_array().unwrap().len(),
            0
        );
        assert_eq!(context["recent_observations"].as_array().unwrap().len(), 0);
        assert_eq!(context["recent_events"].as_array().unwrap().len(), 0);
        assert_eq!(context["related_commands"].as_array().unwrap().len(), 0);
        assert_eq!(context["metadata"]["llm_invoked"], false);
    }

    #[tokio::test]
    async fn builds_ai_context_with_observations() {
        let app = app();
        let sensor_id = create_test_entity(&app, "context-level-sensor-01", "aion:Sensor").await;
        let tank_id = create_test_entity(&app, "context-tank-02", "aion:WaterTank").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/observations",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": tank_id,
                    "observed_property": "water_level",
                    "value": {
                        "type": "number",
                        "value": 42.0
                    },
                    "unit": "%",
                    "observed_at": "2026-04-27T13:00:00Z",
                    "received_at": "2026-04-27T13:00:01Z",
                    "protocol": "http",
                    "payload_format": "json_mapping",
                    "quality": {},
                    "metadata": {}
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/ai/context/entity/{tank_id}?include_events=false&include_commands=false"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let context = to_json(response).await;
        let observations = context["recent_observations"].as_array().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0]["feature_of_interest_id"], tank_id);
        assert_eq!(observations[0]["observed_property"], "water_level");
    }

    #[tokio::test]
    async fn builds_ai_context_with_events() {
        let app = app();
        let tank_id = create_test_entity(&app, "context-tank-03", "aion:WaterTank").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/events",
                json!({
                    "event_type": "aion:LowWaterLevel",
                    "severity": "warning",
                    "target_entity_id": tank_id,
                    "message": "Water level is below threshold",
                    "occurred_at": "2026-04-27T13:00:00Z",
                    "metadata": {
                        "threshold": 30
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/ai/context/entity/{tank_id}?include_observations=false&include_commands=false"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let context = to_json(response).await;
        let events = context["recent_events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event_type"], "aion:LowWaterLevel");
        assert_eq!(events[0]["target_entity_id"], tank_id);
    }

    #[tokio::test]
    async fn builds_ai_context_with_command_action_result_history() {
        let app = app();
        let pump_id = create_test_entity(&app, "context-pump-02", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        claim_test_command(&app, command_id, "executor-01").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/actions",
                json!({
                    "command_id": command_id,
                    "executor_entity_id": pump_id,
                    "action_type": "StartPump",
                    "status": "started",
                    "started_at": "2026-04-27T13:01:00Z",
                    "metadata": {
                        "executor": "test"
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let action = to_json(response).await;
        let action_id = action["id"].as_str().unwrap();

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
                    "observed_at": "2026-04-27T13:01:30Z",
                    "metadata": {
                        "source": "test"
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/ai/context/entity/{pump_id}?include_observations=false&include_events=false&include_commands=true"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let context = to_json(response).await;
        assert_eq!(context["related_commands"].as_array().unwrap().len(), 1);
        assert_eq!(context["related_commands"][0]["id"], command_id);
        assert_eq!(context["related_actions"].as_array().unwrap().len(), 1);
        assert_eq!(context["related_actions"][0]["command_id"], command_id);
        assert_eq!(
            context["related_action_results"].as_array().unwrap().len(),
            1
        );
        assert_eq!(
            context["related_action_results"][0]["command_id"],
            command_id
        );
        assert_eq!(context["related_action_results"][0]["action_id"], action_id);
    }

    #[tokio::test]
    async fn ai_context_limit_is_respected() {
        let app = app();
        let sensor_id = create_test_entity(&app, "context-level-sensor-02", "aion:Sensor").await;
        let tank_id = create_test_entity(&app, "context-tank-04", "aion:WaterTank").await;

        for (observed_at, value) in [
            ("2026-04-27T13:00:00Z", 41.0),
            ("2026-04-27T13:05:00Z", 39.5),
        ] {
            let response = app
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/observations",
                    json!({
                        "producer_entity_id": sensor_id,
                        "feature_of_interest_id": tank_id,
                        "observed_property": "water_level",
                        "value": {
                            "type": "number",
                            "value": value
                        },
                        "unit": "%",
                        "observed_at": observed_at,
                        "received_at": observed_at,
                        "protocol": "http",
                        "payload_format": "json_mapping",
                        "quality": {},
                        "metadata": {}
                    }),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/ai/context/entity/{tank_id}?limit=1&include_events=false&include_commands=false"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let context = to_json(response).await;
        let observations = context["recent_observations"].as_array().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0]["value"]["value"], 39.5);
    }

    #[tokio::test]
    async fn lists_mcp_tool_definitions() {
        let app = app();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/mcp/tools")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let tools = to_json(response).await;
        let tool_names = tools
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect::<Vec<_>>();

        assert!(tool_names.contains(&"list_entities"));
        assert!(tool_names.contains(&"get_entity"));
        assert!(tool_names.contains(&"build_ai_context"));
    }

    #[tokio::test]
    async fn invokes_mcp_list_entities() {
        let app = app();
        let tank_id = create_test_entity(&app, "mcp-tank-01", "aion:WaterTank").await;

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp/tools/list_entities",
                json!({
                    "arguments": {}
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let tool_response = to_json(response).await;
        assert!(tool_response["error"].is_null());
        assert!(tool_response["result"]["content"]["entities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entity| entity["id"] == tank_id
                && entity["entity_key"] == "mcp-tank-01"
                && entity["entity_type"] == "aion:WaterTank"));
    }

    #[tokio::test]
    async fn invokes_mcp_get_entity() {
        let app = app();
        let pump_id = create_test_entity(&app, "mcp-pump-01", "aion:Pump").await;

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp/tools/get_entity",
                json!({
                    "arguments": {
                        "entity_id": pump_id
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let tool_response = to_json(response).await;
        assert_eq!(tool_response["result"]["content"]["entity"]["id"], pump_id);
        assert_eq!(
            tool_response["result"]["content"]["entity"]["entity_type"],
            "aion:Pump"
        );
    }

    #[tokio::test]
    async fn invokes_mcp_build_ai_context() {
        let app = app();
        let tank_id = create_test_entity(&app, "mcp-context-tank-01", "aion:WaterTank").await;
        let pump_id = create_test_entity(&app, "mcp-context-pump-01", "aion:Pump").await;
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/relationships",
                json!({
                    "source_entity_id": pump_id,
                    "relationship_type": "aion:fills",
                    "target_entity_id": tank_id,
                    "jsonld": {
                        "@type": "aion:Relationship"
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp/tools/build_ai_context",
                json!({
                    "arguments": {
                        "entity_id": tank_id,
                        "include_observations": false,
                        "include_events": false,
                        "include_commands": false,
                        "limit": 5
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let tool_response = to_json(response).await;
        let context = &tool_response["result"]["content"]["context"];
        assert_eq!(context["target_entity"]["id"], tank_id);
        assert_eq!(
            context["incoming_relationships"].as_array().unwrap().len(),
            1
        );
        assert_eq!(context["metadata"]["llm_invoked"], false);
    }

    #[tokio::test]
    async fn mcp_invalid_tool_name_returns_clear_error() {
        let app = app();

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp/tools/no_such_tool",
                json!({
                    "arguments": {}
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let tool_response = to_json(response).await;
        assert!(tool_response["result"].is_null());
        assert_eq!(tool_response["error"]["code"], "not_found");
        assert!(tool_response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("unknown MCP tool"));
    }

    #[tokio::test]
    async fn mcp_missing_required_tool_argument_returns_clear_error() {
        let app = app();

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp/tools/get_entity",
                json!({
                    "arguments": {}
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let tool_response = to_json(response).await;
        assert!(tool_response["result"].is_null());
        assert_eq!(tool_response["error"]["code"], "missing_argument");
        assert_eq!(tool_response["error"]["message"], "entity_id is required");
    }

    #[tokio::test]
    async fn mcp_json_rpc_tools_list_returns_tool_definitions() {
        let app = app();

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp",
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/list",
                    "params": {}
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response = to_json(response).await;
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 1);
        let tools = response["result"]["tools"].as_array().unwrap();
        assert!(tools.iter().any(|tool| tool["name"] == "list_entities"
            && tool["inputSchema"]["additionalProperties"] == false));
        assert!(tools.iter().any(|tool| tool["name"] == "build_ai_context"
            && tool["inputSchema"]["required"]
                .as_array()
                .unwrap()
                .contains(&json!("entity_id"))));
    }

    #[tokio::test]
    async fn mcp_json_rpc_tools_call_build_ai_context_works() {
        let app = app();
        let tank_id = create_test_entity(&app, "json-rpc-tank-01", "aion:WaterTank").await;

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp",
                json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "build_ai_context",
                        "arguments": {
                            "entity_id": tank_id,
                            "include_observations": false,
                            "include_events": false,
                            "include_commands": false
                        }
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response = to_json(response).await;
        assert_eq!(response["id"], 2);
        assert_eq!(response["result"]["isError"], false);
        assert_eq!(
            response["result"]["structuredContent"]["context"]["target_entity"]["id"],
            tank_id
        );
        let text = response["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("json-rpc-tank-01"));
    }

    #[tokio::test]
    async fn mcp_json_rpc_tools_call_list_entities_works() {
        let app = app();
        let entity_id = create_test_entity(&app, "json-rpc-entity-01", "aion:Sensor").await;

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp",
                json!({
                    "jsonrpc": "2.0",
                    "id": "list-entities",
                    "method": "tools/call",
                    "params": {
                        "name": "list_entities",
                        "arguments": {}
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response = to_json(response).await;
        assert_eq!(response["id"], "list-entities");
        assert!(response["result"]["structuredContent"]["entities"]
            .as_array()
            .unwrap()
            .iter()
            .any(
                |entity| entity["id"] == entity_id && entity["entity_key"] == "json-rpc-entity-01"
            ));
    }

    #[tokio::test]
    async fn mcp_json_rpc_unknown_method_returns_error() {
        let app = app();

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp",
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "resources/list",
                    "params": {}
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response = to_json(response).await;
        assert_eq!(response["error"]["code"], -32601);
        assert!(response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("unknown JSON-RPC method"));
    }

    #[tokio::test]
    async fn mcp_json_rpc_unknown_tool_returns_error() {
        let app = app();

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp",
                json!({
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "tools/call",
                    "params": {
                        "name": "no_such_tool",
                        "arguments": {}
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response = to_json(response).await;
        assert_eq!(response["error"]["code"], -32602);
        assert_eq!(response["error"]["data"]["code"], "not_found");
        assert_eq!(response["error"]["data"]["isError"], true);
    }

    #[tokio::test]
    async fn mcp_json_rpc_missing_required_tool_argument_returns_error() {
        let app = app();

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp",
                json!({
                    "jsonrpc": "2.0",
                    "id": 5,
                    "method": "tools/call",
                    "params": {
                        "name": "build_ai_context",
                        "arguments": {}
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response = to_json(response).await;
        assert_eq!(response["error"]["code"], -32602);
        assert_eq!(response["error"]["data"]["code"], "missing_argument");
        assert_eq!(response["error"]["message"], "entity_id is required");
    }

    #[tokio::test]
    async fn mcp_json_rpc_malformed_request_returns_error() {
        let app = app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from("{not-json"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response = to_json(response).await;
        assert_eq!(response["jsonrpc"], "2.0");
        assert!(response["id"].is_null());
        assert_eq!(response["error"]["code"], -32700);
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
