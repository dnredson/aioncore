use crate::{
    auth::{
        is_admin_all, principal_tenant_id, require_scope, require_scope_for_write,
        tenant_for_created_resource, AuthContext,
    },
    ensure_entity_exists,
    error::ApiError,
    require_same_tenant_for_target_entity, state_for_tenant, AppState, AuthMode,
};
use aion_action::Capability;
use aion_entity::Entity;
use aion_relationship::Relationship;
use aion_storage::PayloadProfile;
use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{get, post, put},
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
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
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateEntityRequest {
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
pub(crate) struct CreateRelationshipRequest {
    pub source_entity_id: Uuid,
    pub relationship_type: String,
    pub target_entity_id: Uuid,
    #[serde(default = "empty_object")]
    pub jsonld: Value,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PutPayloadProfileRequest {
    pub payload_format: String,
    pub protocol: Option<String>,
    pub content_type: Option<String>,
    pub attribute_mapping: Option<Value>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PutCapabilityRequest {
    pub capability_name: String,
    pub command_type: String,
    pub protocol: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Serialize)]
pub(crate) struct EntityContextResponse {
    pub entity: Entity,
    pub outgoing_relationships: Vec<Relationship>,
    pub incoming_relationships: Vec<Relationship>,
}

async fn create_entity(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<Value>,
) -> Result<(StatusCode, Json<Entity>), ApiError> {
    require_scope_for_write(&state, &auth, "/entities", "entities:write")?;
    let request = parse_entity_input(request)?;
    let tenant_id = tenant_for_created_resource(&state, &auth)?;
    let entity = Entity::new(
        tenant_id,
        request.entity_key,
        request.entity_type,
        request.jsonld,
        Utc::now(),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let entity = state.storage.create_entity(entity)?;
    Ok((StatusCode::CREATED, Json(entity)))
}

async fn get_entity(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(entity_id): Path<Uuid>,
) -> Result<Json<Entity>, ApiError> {
    require_scope(&state, &auth, "/entities/:entity_id", "entities:read")?;
    let entity = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_entity(state.tenant_id, entity_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_entity_any_tenant(entity_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_entity(tenant_id, entity_id)? {
            Some(entity) => entity,
            None => {
                if state.storage.get_entity_any_tenant(entity_id)?.is_some() {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /entities/:entity_id",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };

    Ok(Json(entity))
}

async fn list_entities(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Vec<Entity>>, ApiError> {
    require_scope(&state, &auth, "/entities", "entities:read")?;
    let entities = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state.storage.list_entities(state.tenant_id)?
    } else if is_admin_all(&auth) {
        state.storage.list_all_entities()?
    } else {
        state.storage.list_entities(principal_tenant_id(&auth)?)?
    };
    Ok(Json(entities))
}

async fn create_relationship(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateRelationshipRequest>,
) -> Result<(StatusCode, Json<Relationship>), ApiError> {
    require_scope_for_write(&state, &auth, "/relationships", "relationships:write")?;
    require_same_tenant_for_target_entity(
        &state,
        &auth,
        "/relationships",
        request.source_entity_id,
    )?;
    require_same_tenant_for_target_entity(
        &state,
        &auth,
        "/relationships",
        request.target_entity_id,
    )?;
    let tenant_id = tenant_for_created_resource(&state, &auth)?;

    let relationship = Relationship::new(
        tenant_id,
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
    Extension(auth): Extension<AuthContext>,
    Path(entity_id): Path<Uuid>,
) -> Result<Json<EntityContextResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/entities/:entity_id/context",
        "entities:read",
    )?;
    let entity = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_entity(state.tenant_id, entity_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_entity_any_tenant(entity_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_entity(tenant_id, entity_id)? {
            Some(entity) => entity,
            None => {
                if state.storage.get_entity_any_tenant(entity_id)?.is_some() {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /entities/:entity_id/context",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };

    let outgoing_relationships = state
        .storage
        .list_relationships(entity.tenant_id, Some(entity_id), None)?
        .into_iter()
        .filter(|relationship| {
            state
                .storage
                .get_entity(relationship.tenant_id, relationship.source_entity_id)
                .ok()
                .flatten()
                .is_some()
                && state
                    .storage
                    .get_entity(relationship.tenant_id, relationship.target_entity_id)
                    .ok()
                    .flatten()
                    .is_some()
        })
        .collect::<Vec<_>>();
    let incoming_relationships = state
        .storage
        .list_relationships(entity.tenant_id, None, Some(entity_id))?
        .into_iter()
        .filter(|relationship| {
            state
                .storage
                .get_entity(relationship.tenant_id, relationship.source_entity_id)
                .ok()
                .flatten()
                .is_some()
                && state
                    .storage
                    .get_entity(relationship.tenant_id, relationship.target_entity_id)
                    .ok()
                    .flatten()
                    .is_some()
        })
        .collect::<Vec<_>>();

    Ok(Json(EntityContextResponse {
        entity,
        outgoing_relationships,
        incoming_relationships,
    }))
}

async fn put_capabilities(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(entity_id): Path<Uuid>,
    Json(requests): Json<Vec<PutCapabilityRequest>>,
) -> Result<(StatusCode, Json<Vec<Capability>>), ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/entities/:entity_id/capabilities",
        "capabilities:write",
    )?;
    let entity = require_same_tenant_for_target_entity(
        &state,
        &auth,
        "/entities/:entity_id/capabilities",
        entity_id,
    )?;
    let scoped_state = state_for_tenant(&state, entity.tenant_id);
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

    let capabilities =
        scoped_state
            .storage
            .put_capabilities(scoped_state.tenant_id, entity_id, capabilities)?;
    Ok((StatusCode::OK, Json(capabilities)))
}

async fn get_capabilities(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(entity_id): Path<Uuid>,
) -> Result<Json<Vec<Capability>>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/entities/:entity_id/capabilities",
        "capabilities:read",
    )?;
    let entity = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_entity(state.tenant_id, entity_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_entity_any_tenant(entity_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_entity(tenant_id, entity_id)? {
            Some(entity) => entity,
            None => {
                if state.storage.get_entity_any_tenant(entity_id)?.is_some() {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /entities/:entity_id/capabilities",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };
    Ok(Json(
        state
            .storage
            .list_capabilities(entity.tenant_id, entity_id)?,
    ))
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

pub(crate) fn extract_jsonld_entity_key(object: &serde_json::Map<String, Value>) -> Option<String> {
    object
        .get("entity_key")
        .or_else(|| object.get("aion:entityKey"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub(crate) fn derive_entity_key(jsonld_id: &str) -> Option<String> {
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

fn empty_object() -> Value {
    Value::Object(Default::default())
}
