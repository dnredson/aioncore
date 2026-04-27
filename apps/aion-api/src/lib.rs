use aion_entity::Entity;
use aion_observation::{Observation, ObservationValue};
use aion_relationship::Relationship;
use aion_storage::{
    EntityStore, InMemoryStorage, ObservationStore, RelationshipStore, StorageError,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
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
pub struct ObservationQuery {
    pub feature_of_interest_id: Option<Uuid>,
    pub observed_property: Option<String>,
    pub limit: Option<u32>,
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
        .route("/relationships", post(create_relationship))
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
    Json(request): Json<CreateEntityRequest>,
) -> Result<(StatusCode, Json<Entity>), ApiError> {
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

    Ok(Json(observations))
}

fn ensure_entity_exists(state: &AppState, entity_id: Uuid) -> Result<(), ApiError> {
    state
        .storage
        .get_entity(state.tenant_id, entity_id)?
        .map(|_| ())
        .ok_or_else(ApiError::not_found)
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
    async fn creates_entity_and_returns_context() {
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

    fn json_request(method: &str, uri: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    async fn to_json(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }
}
