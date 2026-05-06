use crate::{
    auth::{
        is_admin_all, principal_tenant_id, require_scope, require_scope_for_write,
        tenant_for_created_resource, AuthContext,
    },
    error::ApiError,
    evaluate_rules_for_observation, require_same_tenant_for_target_entity, state_for_tenant,
    AppState, AuthMode,
};
use aion_observation::{Observation, ObservationValue};
use axum::{
    extract::{Extension, Query, State},
    http::StatusCode,
    routing::post,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new().route(
        "/observations",
        post(create_observation).get(query_observations),
    )
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateObservationRequest {
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
pub(crate) struct ObservationQuery {
    pub feature_of_interest_id: Option<Uuid>,
    pub observed_property: Option<String>,
    pub raw_message_id: Option<Uuid>,
    pub limit: Option<u32>,
}

async fn create_observation(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateObservationRequest>,
) -> Result<(StatusCode, Json<Observation>), ApiError> {
    require_scope_for_write(&state, &auth, "/observations", "observations:write")?;
    require_same_tenant_for_target_entity(
        &state,
        &auth,
        "/observations",
        request.producer_entity_id,
    )?;
    require_same_tenant_for_target_entity(
        &state,
        &auth,
        "/observations",
        request.feature_of_interest_id,
    )?;
    let scoped_state = state_for_tenant(&state, tenant_for_created_resource(&state, &auth)?);

    let observation = Observation::new(
        scoped_state.tenant_id,
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

    let observation = scoped_state.storage.store_observation(observation)?;
    evaluate_rules_for_observation(&scoped_state, &observation, true)?;
    Ok((StatusCode::CREATED, Json(observation)))
}

async fn query_observations(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<ObservationQuery>,
) -> Result<Json<Vec<Observation>>, ApiError> {
    require_scope(&state, &auth, "/observations", "observations:read")?;
    let observations = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state.storage.query_observations(
            state.tenant_id,
            query.feature_of_interest_id,
            query.observed_property.as_deref(),
            None,
            None,
            query.limit.unwrap_or(100),
        )?
    } else if is_admin_all(&auth) {
        let mut observations = state.storage.list_all_observations()?;
        if let Some(feature_of_interest_id) = query.feature_of_interest_id {
            observations
                .retain(|observation| observation.feature_of_interest_id == feature_of_interest_id);
        }
        if let Some(observed_property) = query.observed_property.as_deref() {
            observations.retain(|observation| observation.observed_property == observed_property);
        }
        observations.truncate(query.limit.unwrap_or(100) as usize);
        observations
    } else {
        state.storage.query_observations(
            principal_tenant_id(&auth)?,
            query.feature_of_interest_id,
            query.observed_property.as_deref(),
            None,
            None,
            query.limit.unwrap_or(100),
        )?
    };
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

fn empty_object() -> Value {
    Value::Object(Default::default())
}
