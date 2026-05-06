use crate::{
    auth::{require_scope, AuthContext},
    error::ApiError,
    require_same_tenant_for_target_entity, state_for_tenant, AppState,
};
use aion_observation::{Observation, ObservationValue};
use axum::{
    extract::{Extension, Path, Query, State},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use uuid::Uuid;

const DEFAULT_LIMIT: u32 = 1_000;
const MAX_LIMIT: u32 = 10_000;
const UNBOUNDED_QUERY_LIMIT: u32 = u32::MAX;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/timeseries/query", get(query_timeseries))
        .route(
            "/timeseries/entities/:entity_id/properties",
            get(list_entity_timeseries_properties),
        )
}

#[derive(Debug, Deserialize)]
pub(crate) struct TimeseriesQuery {
    pub entity_id: Uuid,
    pub observed_property: String,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[serde(default)]
    pub aggregation: TimeseriesAggregation,
    pub interval: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TimeseriesAggregation {
    #[default]
    None,
    Last,
    Count,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Serialize)]
pub(crate) struct TimeseriesQueryResponse {
    pub entity_id: Uuid,
    pub observed_property: String,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub aggregation: TimeseriesAggregation,
    pub interval: Option<String>,
    pub points: Vec<TimeseriesPoint>,
    pub count: usize,
    pub limit: u32,
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct TimeseriesPoint {
    pub time: DateTime<Utc>,
    pub value: ObservationValue,
    pub unit: Option<String>,
    pub quality: Value,
    pub observation_id: Option<Uuid>,
    pub raw_message_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
pub(crate) struct EntityTimeseriesPropertiesResponse {
    pub entity_id: Uuid,
    pub properties: Vec<EntityTimeseriesProperty>,
}

#[derive(Debug, Serialize)]
pub(crate) struct EntityTimeseriesProperty {
    pub observed_property: String,
    pub units: Vec<String>,
    pub count: usize,
    pub first_observed_at: Option<DateTime<Utc>>,
    pub last_observed_at: Option<DateTime<Utc>>,
}

async fn query_timeseries(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<TimeseriesQuery>,
) -> Result<Json<TimeseriesQueryResponse>, ApiError> {
    require_scope(&state, &auth, "/timeseries/query", "timeseries:read")?;
    validate_timeseries_query(&query)?;
    let entity =
        require_same_tenant_for_target_entity(&state, &auth, "/timeseries/query", query.entity_id)?;
    let scoped_state = state_for_tenant(&state, entity.tenant_id);

    if query.interval.is_some() {
        return Err(ApiError::bad_request(
            "interval aggregation is not implemented for /timeseries/query yet",
        ));
    }

    let response = match query.aggregation {
        TimeseriesAggregation::None => query_raw_timeseries(&scoped_state, &query)?,
        TimeseriesAggregation::Last => query_last_timeseries_point(&scoped_state, &query)?,
        TimeseriesAggregation::Count => {
            aggregate_timeseries_numeric(&scoped_state, &query, AggregationKind::Count)?
        }
        TimeseriesAggregation::Avg => {
            aggregate_timeseries_numeric(&scoped_state, &query, AggregationKind::Avg)?
        }
        TimeseriesAggregation::Min => {
            aggregate_timeseries_numeric(&scoped_state, &query, AggregationKind::Min)?
        }
        TimeseriesAggregation::Max => {
            aggregate_timeseries_numeric(&scoped_state, &query, AggregationKind::Max)?
        }
    };

    Ok(Json(response))
}

async fn list_entity_timeseries_properties(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(entity_id): Path<Uuid>,
) -> Result<Json<EntityTimeseriesPropertiesResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/timeseries/entities/:entity_id/properties",
        "timeseries:read",
    )?;
    let entity = require_same_tenant_for_target_entity(
        &state,
        &auth,
        "/timeseries/entities/:entity_id/properties",
        entity_id,
    )?;
    let observations = state_for_tenant(&state, entity.tenant_id)
        .storage
        .query_observations_chronological(
            entity.tenant_id,
            Some(entity_id),
            None,
            None,
            None,
            UNBOUNDED_QUERY_LIMIT,
        )?;

    let mut properties = Vec::<EntityTimeseriesProperty>::new();
    for observation in observations {
        match properties
            .iter_mut()
            .find(|property| property.observed_property == observation.observed_property)
        {
            Some(property) => {
                property.count += 1;
                if let Some(unit) = observation.unit.as_deref() {
                    if !property.units.iter().any(|existing| existing == unit) {
                        property.units.push(unit.to_string());
                    }
                }
                property.first_observed_at = Some(
                    property
                        .first_observed_at
                        .map(|value| value.min(observation.observed_at))
                        .unwrap_or(observation.observed_at),
                );
                property.last_observed_at = Some(
                    property
                        .last_observed_at
                        .map(|value| value.max(observation.observed_at))
                        .unwrap_or(observation.observed_at),
                );
            }
            None => {
                let mut units = Vec::new();
                if let Some(unit) = observation.unit.as_deref() {
                    units.push(unit.to_string());
                }
                properties.push(EntityTimeseriesProperty {
                    observed_property: observation.observed_property,
                    units,
                    count: 1,
                    first_observed_at: Some(observation.observed_at),
                    last_observed_at: Some(observation.observed_at),
                });
            }
        }
    }

    properties.sort_by(|left, right| left.observed_property.cmp(&right.observed_property));
    for property in &mut properties {
        property.units.sort();
        property.units.dedup();
    }

    Ok(Json(EntityTimeseriesPropertiesResponse {
        entity_id,
        properties,
    }))
}

fn validate_timeseries_query(query: &TimeseriesQuery) -> Result<(), ApiError> {
    if query.observed_property.trim().is_empty() {
        return Err(ApiError::bad_request(
            "observed_property is required for /timeseries/query",
        ));
    }

    if let (Some(from), Some(to)) = (query.from, query.to) {
        if from > to {
            return Err(ApiError::bad_request(
                "from must be less than or equal to to for /timeseries/query",
            ));
        }
    }

    Ok(())
}

fn query_raw_timeseries(
    state: &AppState,
    query: &TimeseriesQuery,
) -> Result<TimeseriesQueryResponse, ApiError> {
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let observations = state.storage.query_observations_chronological(
        state.tenant_id,
        Some(query.entity_id),
        Some(query.observed_property.as_str()),
        query.from,
        query.to,
        limit.saturating_add(1),
    )?;
    let truncated = observations.len() > limit as usize;
    let points = observations
        .into_iter()
        .take(limit as usize)
        .map(timeseries_point_from_observation)
        .collect::<Vec<_>>();

    Ok(TimeseriesQueryResponse {
        entity_id: query.entity_id,
        observed_property: query.observed_property.clone(),
        from: query.from,
        to: query.to,
        aggregation: query.aggregation,
        interval: query.interval.clone(),
        count: points.len(),
        points,
        limit,
        truncated,
    })
}

fn query_last_timeseries_point(
    state: &AppState,
    query: &TimeseriesQuery,
) -> Result<TimeseriesQueryResponse, ApiError> {
    let observations = state.storage.query_observations_chronological(
        state.tenant_id,
        Some(query.entity_id),
        Some(query.observed_property.as_str()),
        query.from,
        query.to,
        UNBOUNDED_QUERY_LIMIT,
    )?;
    let points = observations
        .last()
        .cloned()
        .map(timeseries_point_from_observation)
        .into_iter()
        .collect::<Vec<_>>();

    Ok(TimeseriesQueryResponse {
        entity_id: query.entity_id,
        observed_property: query.observed_property.clone(),
        from: query.from,
        to: query.to,
        aggregation: query.aggregation,
        interval: query.interval.clone(),
        count: points.len(),
        points,
        limit: query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT),
        truncated: false,
    })
}

#[derive(Debug, Clone, Copy)]
enum AggregationKind {
    Count,
    Avg,
    Min,
    Max,
}

fn aggregate_timeseries_numeric(
    state: &AppState,
    query: &TimeseriesQuery,
    aggregation: AggregationKind,
) -> Result<TimeseriesQueryResponse, ApiError> {
    let observations = state.storage.query_observations_chronological(
        state.tenant_id,
        Some(query.entity_id),
        Some(query.observed_property.as_str()),
        query.from,
        query.to,
        UNBOUNDED_QUERY_LIMIT,
    )?;

    let points = match aggregation {
        AggregationKind::Count => aggregate_count_point(&observations),
        AggregationKind::Avg | AggregationKind::Min | AggregationKind::Max => {
            aggregate_numeric_point(&observations, aggregation)?
        }
    };

    Ok(TimeseriesQueryResponse {
        entity_id: query.entity_id,
        observed_property: query.observed_property.clone(),
        from: query.from,
        to: query.to,
        aggregation: query.aggregation,
        interval: query.interval.clone(),
        count: points.len(),
        points,
        limit: query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT),
        truncated: false,
    })
}

fn aggregate_count_point(observations: &[Observation]) -> Vec<TimeseriesPoint> {
    observations
        .last()
        .map(|observation| TimeseriesPoint {
            time: observation.observed_at,
            value: ObservationValue::Number {
                value: observations.len() as f64,
            },
            unit: None,
            quality: json!({}),
            observation_id: None,
            raw_message_id: None,
        })
        .into_iter()
        .collect()
}

fn aggregate_numeric_point(
    observations: &[Observation],
    aggregation: AggregationKind,
) -> Result<Vec<TimeseriesPoint>, ApiError> {
    let numeric_observations = observations
        .iter()
        .filter_map(|observation| match observation.value {
            ObservationValue::Number { value } => Some((observation, value)),
            _ => None,
        })
        .collect::<Vec<_>>();

    if observations
        .iter()
        .any(|observation| !matches!(observation.value, ObservationValue::Number { .. }))
        && numeric_observations.is_empty()
    {
        return Err(ApiError::bad_request(
            "numeric aggregation requires at least one numeric observation value",
        ));
    }

    if numeric_observations.is_empty() {
        return Ok(Vec::new());
    }

    let aggregated_value = match aggregation {
        AggregationKind::Avg => {
            numeric_observations
                .iter()
                .map(|(_, value)| *value)
                .sum::<f64>()
                / numeric_observations.len() as f64
        }
        AggregationKind::Min => numeric_observations
            .iter()
            .map(|(_, value)| *value)
            .fold(f64::INFINITY, f64::min),
        AggregationKind::Max => numeric_observations
            .iter()
            .map(|(_, value)| *value)
            .fold(f64::NEG_INFINITY, f64::max),
        AggregationKind::Count => unreachable!(),
    };

    let representative = numeric_observations
        .last()
        .map(|(observation, _)| *observation)
        .unwrap();
    let unit = shared_numeric_unit(&numeric_observations);

    Ok(vec![TimeseriesPoint {
        time: representative.observed_at,
        value: ObservationValue::Number {
            value: aggregated_value,
        },
        unit,
        quality: json!({}),
        observation_id: None,
        raw_message_id: None,
    }])
}

fn shared_numeric_unit(numeric_observations: &[(&Observation, f64)]) -> Option<String> {
    let units = numeric_observations
        .iter()
        .filter_map(|(observation, _)| observation.unit.as_deref())
        .collect::<BTreeSet<_>>();
    if units.len() == 1 {
        units.iter().next().map(|value| (*value).to_string())
    } else {
        None
    }
}

fn timeseries_point_from_observation(observation: Observation) -> TimeseriesPoint {
    TimeseriesPoint {
        time: observation.observed_at,
        value: observation.value,
        unit: observation.unit,
        quality: observation.quality,
        observation_id: Some(observation.id),
        raw_message_id: observation.raw_message_id,
    }
}
