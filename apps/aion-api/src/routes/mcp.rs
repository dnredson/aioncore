use crate::{
    auth::{require_scope, AuthContext},
    build_ai_entity_context,
    error::ApiError,
    AiContextQuery, AppState,
};
use aion_action::CommandStatus;
use aion_event::EventSeverity;
use aion_mcp::{ToolDefinition, ToolRequest, ToolResponse};
use aion_storage::{EventFilter, StorageError};
use axum::{
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/mcp", post(handle_mcp_json_rpc))
        .route("/mcp/tools", get(list_mcp_tools))
        .route("/mcp/tools/:tool_name", post(invoke_mcp_tool))
}

#[derive(Debug, Deserialize)]
struct McpRecentObservationsArgs {
    feature_of_interest_id: Option<Uuid>,
    producer_entity_id: Option<Uuid>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct McpEventsArgs {
    entity_id: Option<Uuid>,
    event_type: Option<String>,
    severity: Option<EventSeverity>,
    command_id: Option<Uuid>,
    raw_message_id: Option<Uuid>,
    correlation_id: Option<String>,
    limit: Option<u32>,
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
            StorageError::ConflictWithMessage(message) => Self {
                status: StatusCode::CONFLICT,
                code: "conflict".to_string(),
                message,
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

async fn list_mcp_tools(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Vec<ToolDefinition>>, ApiError> {
    require_scope(&state, &auth, "/mcp/tools", "mcp:tools")?;
    Ok(Json(mcp_tool_definitions()))
}

async fn handle_mcp_json_rpc(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    require_scope(&state, &auth, "/mcp", "mcp:tools")?;
    let request = match serde_json::from_slice::<Value>(&body) {
        Ok(request) => request,
        Err(error) => {
            return Ok((
                StatusCode::OK,
                Json(json_rpc_error(
                    Value::Null,
                    -32700,
                    format!("parse error: {error}"),
                    None,
                )),
            ));
        }
    };

    let object = match request.as_object() {
        Some(object) => object,
        None => {
            return Ok((
                StatusCode::OK,
                Json(json_rpc_error(
                    Value::Null,
                    -32600,
                    "invalid JSON-RPC request",
                    None,
                )),
            ));
        }
    };

    let id = object.get("id").cloned().unwrap_or(Value::Null);
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Ok((
            StatusCode::OK,
            Json(json_rpc_error(id, -32600, "jsonrpc must be \"2.0\"", None)),
        ));
    }

    let Some(method) = object.get("method").and_then(Value::as_str) else {
        return Ok((
            StatusCode::OK,
            Json(json_rpc_error(id, -32600, "method is required", None)),
        ));
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

    Ok((StatusCode::OK, Json(response)))
}

async fn invoke_mcp_tool(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(tool_name): Path<String>,
    Json(request): Json<ToolRequest>,
) -> Result<(StatusCode, Json<ToolResponse>), ApiError> {
    require_scope(&state, &auth, "/mcp/tools/:tool_name", "mcp:tools")?;
    match invoke_local_mcp_tool(&state, &tool_name, request.arguments) {
        Ok(content) => Ok((
            StatusCode::OK,
            Json(ToolResponse::success(tool_name, content)),
        )),
        Err(error) => Ok((
            error.status,
            Json(ToolResponse::error(tool_name, error.code, error.message)),
        )),
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
