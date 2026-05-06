use crate::{
    auth::{require_scope, AuthContext},
    ensure_entity_exists,
    error::ApiError,
    get_connector, record_connector_worker_event, record_ttn_device_mapping_event, AppState,
};
use aion_event::{Event, EventSeverity};
use aion_storage::{
    ConnectorProfile, ConnectorSecret, ConnectorSecretType, IngestionConnector,
    IngestionConnectorType, TtnDeviceMapping,
};
use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{get, post, put},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use std::time::Instant;
use tokio::time;
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/ingestion/connectors/:connector_id/validate",
            get(validate_ingestion_connector),
        )
        .route(
            "/ingestion/connectors/:connector_id/ttn-live-readiness-plan",
            get(get_ttn_live_readiness_plan),
        )
        .route(
            "/ingestion/connectors/:connector_id/ttn-live-validate",
            post(ttn_live_validate_connector),
        )
        .route(
            "/ingestion/connectors/:connector_id/ttn-device-mappings",
            post(create_ttn_device_mapping).get(list_ttn_device_mappings),
        )
        .route(
            "/ingestion/connectors/:connector_id/ttn-device-mappings/:mapping_id",
            get(get_ttn_device_mapping)
                .patch(update_ttn_device_mapping)
                .delete(delete_ttn_device_mapping),
        )
        .route(
            "/ingestion/connectors/:connector_id/ttn-device-mappings/:mapping_id/enable",
            put(enable_ttn_device_mapping),
        )
        .route(
            "/ingestion/connectors/:connector_id/ttn-device-mappings/:mapping_id/disable",
            put(disable_ttn_device_mapping),
        )
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateTtnDeviceMappingRequest {
    pub ttn_application_id: Option<String>,
    pub ttn_device_id: String,
    pub producer_entity_id: Uuid,
    pub feature_of_interest_id: Option<Uuid>,
    pub enabled: Option<bool>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateTtnDeviceMappingRequest {
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub ttn_application_id: Option<Option<String>>,
    pub ttn_device_id: Option<String>,
    pub producer_entity_id: Option<Uuid>,
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub feature_of_interest_id: Option<Option<Uuid>>,
    pub enabled: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub metadata: Option<Option<Value>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct TtnDeviceMappingResponse {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub connector_id: Uuid,
    pub ttn_application_id: Option<String>,
    pub ttn_device_id: String,
    pub producer_entity_id: Uuid,
    pub feature_of_interest_id: Option<Uuid>,
    pub enabled: bool,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

fn deserialize_optional_nullable<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TtnConnectorReadiness {
    Ready,
    Degraded,
    Invalid,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct TtnConnectorValidationIssue {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TtnConnectorValidation {
    pub connector_id: Uuid,
    pub connector_key: String,
    pub valid: bool,
    pub readiness: TtnConnectorReadiness,
    pub issues: Vec<TtnConnectorValidationIssue>,
    pub warnings: Vec<TtnConnectorValidationIssue>,
    pub detected_profile: ConnectorProfile,
    pub expected_topic_shape: &'static str,
    pub mapping_count: usize,
    pub enabled_mapping_count: usize,
    pub has_secret_ref: bool,
    pub secret_configured: bool,
    pub secret_type: Option<ConnectorSecretType>,
    pub payload_format_supported: bool,
    pub operator_hints: Vec<String>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub(crate) enum TtnLiveReadinessCheckStatus {
    Pass,
    Warn,
    Fail,
    Skipped,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TtnLiveReadinessCheck {
    pub check_key: &'static str,
    pub description: &'static str,
    pub status: TtnLiveReadinessCheckStatus,
    pub reason: Option<String>,
    pub future_live_check: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TtnLiveReadinessPlan {
    pub connector_id: Uuid,
    pub connector_key: String,
    pub dry_run: bool,
    pub can_attempt_live_validation: bool,
    pub readiness: TtnConnectorReadiness,
    pub checks: Vec<TtnLiveReadinessCheck>,
    pub blockers: Vec<TtnConnectorValidationIssue>,
    pub warnings: Vec<TtnConnectorValidationIssue>,
    pub required_operator_steps: Vec<String>,
    pub safe_to_connect: bool,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TtnLiveValidationRequest {
    pub timeout_seconds: Option<u64>,
    pub expect_message: Option<bool>,
    pub client_id_suffix: Option<String>,
    pub dry_run_only: Option<bool>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TtnLiveValidationResultStatus {
    Success,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TtnLiveValidationPlanSummary {
    pub safe_to_connect: bool,
    pub can_attempt_live_validation: bool,
    pub readiness: TtnConnectorReadiness,
    pub blocker_count: usize,
    pub warning_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TtnLiveValidationResponse {
    pub connector_id: Uuid,
    pub connector_key: String,
    pub attempted_live_connection: bool,
    pub dry_run_passed: bool,
    pub connected: bool,
    pub subscribed: bool,
    pub message_received: bool,
    pub broker_url_redacted_or_safe: Option<String>,
    pub topic_filter: Option<String>,
    pub duration_ms: u128,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub result: TtnLiveValidationResultStatus,
    pub errors: Vec<TtnConnectorValidationIssue>,
    pub warnings: Vec<TtnConnectorValidationIssue>,
    pub dry_run_plan_summary: TtnLiveValidationPlanSummary,
    pub secret_exposed: bool,
}

async fn validate_ingestion_connector(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
) -> Result<Json<TtnConnectorValidation>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/validate",
        "connectors:read",
    )?;
    let connector = get_connector(&state, connector_id)?;
    Ok(Json(connector_validation(&state, &connector)?))
}

async fn get_ttn_live_readiness_plan(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
) -> Result<Json<TtnLiveReadinessPlan>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-live-readiness-plan",
        "connectors:read",
    )?;
    let connector = get_connector(&state, connector_id)?;
    Ok(Json(ttn_live_readiness_plan(&state, &connector)?))
}

async fn ttn_live_validate_connector(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
    Json(request): Json<TtnLiveValidationRequest>,
) -> Result<Json<TtnLiveValidationResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-live-validate",
        "connectors:admin",
    )?;
    let connector = get_connector(&state, connector_id)?;
    if connector.connector_profile != ConnectorProfile::TtnV3 {
        return Err(ApiError::bad_request(
            "TTN live validation applies only to ttn-v3 connectors",
        ));
    }

    Ok(Json(
        ttn_live_validation_preflight(&state, &connector, request).await?,
    ))
}

async fn create_ttn_device_mapping(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
    Json(request): Json<CreateTtnDeviceMappingRequest>,
) -> Result<(StatusCode, Json<TtnDeviceMappingResponse>), ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-device-mappings",
        "connectors:admin",
    )?;
    let connector = get_connector(&state, connector_id)?;
    ensure_ttn_connector(&connector)?;
    ensure_entity_exists(&state, request.producer_entity_id)?;
    if let Some(feature_of_interest_id) = request.feature_of_interest_id {
        ensure_entity_exists(&state, feature_of_interest_id)?;
    }

    let now = Utc::now();
    let mut mapping = TtnDeviceMapping::new(
        state.tenant_id,
        connector_id,
        request.ttn_application_id,
        request.ttn_device_id,
        request.producer_entity_id,
        request.feature_of_interest_id,
        request.enabled.unwrap_or(true),
        request.metadata,
        now,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    mapping = state.storage.create_ttn_device_mapping(mapping)?;
    record_ttn_device_mapping_event(
        &state,
        "aion:TtnDeviceMappingCreated",
        &mapping,
        Some("TTN device mapping created".to_string()),
    )?;

    Ok((
        StatusCode::CREATED,
        Json(ttn_device_mapping_response(mapping)),
    ))
}

async fn list_ttn_device_mappings(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
) -> Result<Json<Vec<TtnDeviceMappingResponse>>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-device-mappings",
        "connectors:read",
    )?;
    let connector = get_connector(&state, connector_id)?;
    ensure_ttn_connector(&connector)?;
    let mappings = state
        .storage
        .list_ttn_device_mappings(state.tenant_id, connector_id)?
        .into_iter()
        .map(ttn_device_mapping_response)
        .collect();
    Ok(Json(mappings))
}

async fn get_ttn_device_mapping(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((connector_id, mapping_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<TtnDeviceMappingResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-device-mappings/:mapping_id",
        "connectors:read",
    )?;
    let connector = get_connector(&state, connector_id)?;
    ensure_ttn_connector(&connector)?;
    let mapping = state
        .storage
        .get_ttn_device_mapping(state.tenant_id, connector_id, mapping_id)?
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(ttn_device_mapping_response(mapping)))
}

async fn update_ttn_device_mapping(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((connector_id, mapping_id)): Path<(Uuid, Uuid)>,
    Json(request): Json<UpdateTtnDeviceMappingRequest>,
) -> Result<Json<TtnDeviceMappingResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-device-mappings/:mapping_id",
        "connectors:admin",
    )?;
    let connector = get_connector(&state, connector_id)?;
    ensure_ttn_connector(&connector)?;
    let mut mapping = state
        .storage
        .get_ttn_device_mapping(state.tenant_id, connector_id, mapping_id)?
        .ok_or_else(ApiError::not_found)?;

    if let Some(producer_entity_id) = request.producer_entity_id {
        ensure_entity_exists(&state, producer_entity_id)?;
    }
    if let Some(Some(feature_of_interest_id)) = request.feature_of_interest_id {
        ensure_entity_exists(&state, feature_of_interest_id)?;
    }

    mapping
        .update_fields(
            request.ttn_application_id,
            request.ttn_device_id,
            request.producer_entity_id,
            request.feature_of_interest_id,
            request.enabled,
            request.metadata,
            Utc::now(),
        )
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let mapping = state.storage.update_ttn_device_mapping(mapping)?;
    record_ttn_device_mapping_event(
        &state,
        "aion:TtnDeviceMappingUpdated",
        &mapping,
        Some("TTN device mapping updated".to_string()),
    )?;
    Ok(Json(ttn_device_mapping_response(mapping)))
}

async fn delete_ttn_device_mapping(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((connector_id, mapping_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-device-mappings/:mapping_id",
        "connectors:admin",
    )?;
    let connector = get_connector(&state, connector_id)?;
    ensure_ttn_connector(&connector)?;
    let mapping = state
        .storage
        .get_ttn_device_mapping(state.tenant_id, connector_id, mapping_id)?
        .ok_or_else(ApiError::not_found)?;
    state
        .storage
        .delete_ttn_device_mapping(state.tenant_id, connector_id, mapping_id)?;
    record_ttn_device_mapping_event(
        &state,
        "aion:TtnDeviceMappingDeleted",
        &mapping,
        Some("TTN device mapping deleted".to_string()),
    )?;
    Ok(StatusCode::NO_CONTENT)
}

async fn enable_ttn_device_mapping(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((connector_id, mapping_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<TtnDeviceMappingResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-device-mappings/:mapping_id/enable",
        "connectors:admin",
    )?;
    set_ttn_device_mapping_enabled(state, connector_id, mapping_id, true).await
}

async fn disable_ttn_device_mapping(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((connector_id, mapping_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<TtnDeviceMappingResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-device-mappings/:mapping_id/disable",
        "connectors:admin",
    )?;
    set_ttn_device_mapping_enabled(state, connector_id, mapping_id, false).await
}

async fn set_ttn_device_mapping_enabled(
    state: AppState,
    connector_id: Uuid,
    mapping_id: Uuid,
    enabled: bool,
) -> Result<Json<TtnDeviceMappingResponse>, ApiError> {
    let connector = get_connector(&state, connector_id)?;
    ensure_ttn_connector(&connector)?;
    let mut mapping = state
        .storage
        .get_ttn_device_mapping(state.tenant_id, connector_id, mapping_id)?
        .ok_or_else(ApiError::not_found)?;
    mapping.set_enabled(enabled, Utc::now());
    let mapping = state.storage.update_ttn_device_mapping(mapping)?;
    record_ttn_device_mapping_event(
        &state,
        if enabled {
            "aion:TtnDeviceMappingEnabled"
        } else {
            "aion:TtnDeviceMappingDisabled"
        },
        &mapping,
        Some(if enabled {
            "TTN device mapping enabled".to_string()
        } else {
            "TTN device mapping disabled".to_string()
        }),
    )?;
    Ok(Json(ttn_device_mapping_response(mapping)))
}

fn ensure_ttn_connector(connector: &IngestionConnector) -> Result<(), ApiError> {
    if connector.connector_profile != ConnectorProfile::TtnV3 {
        return Err(ApiError::bad_request(
            "TTN device mappings require connector_profile ttn-v3",
        ));
    }
    Ok(())
}

fn connector_validation(
    state: &AppState,
    connector: &IngestionConnector,
) -> Result<TtnConnectorValidation, ApiError> {
    let mut issues = Vec::new();
    let mut warnings = Vec::new();
    let mappings = if connector.connector_profile == ConnectorProfile::TtnV3 {
        state
            .storage
            .list_ttn_device_mappings(state.tenant_id, connector.id)?
    } else {
        Vec::new()
    };
    let mapping_count = mappings.len();
    let enabled_mapping_count = mappings.iter().filter(|mapping| mapping.enabled).count();
    let payload_format_supported = connector
        .payload_format
        .as_deref()
        .map(crate::is_ttn_uplink_payload_format)
        .unwrap_or(false);
    let mut secret_configured = false;
    let mut secret_type = None;
    let mut operator_hints = Vec::new();

    if connector.connector_profile != ConnectorProfile::TtnV3 {
        warnings.push(ttn_validation_issue(
            "profile_specific_validation_unavailable",
            "profile-specific validation is currently available only for ttn-v3 connectors",
        ));
    } else {
        operator_hints.extend(ttn_operator_hints());
        if connector.connector_type != IngestionConnectorType::Mqtt {
            issues.push(ttn_validation_issue(
                "invalid_connector_type",
                "TTN v3 connectors must use connector_type mqtt",
            ));
        }
        if connector
            .broker_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
        {
            issues.push(ttn_validation_issue(
                "missing_broker_url",
                "TTN v3 connectors should include broker_url before live MQTT operation",
            ));
        }
        match connector.topic_filter.as_deref().map(str::trim) {
            Some(topic_filter) if !topic_filter.is_empty() => {
                if !is_plausible_ttn_topic_filter(topic_filter) {
                    issues.push(ttn_validation_issue(
                        "implausible_ttn_topic_filter",
                        "TTN v3 topic_filter should look like v3/{application_id}/devices/+/up",
                    ));
                }
            }
            _ => issues.push(ttn_validation_issue(
                "missing_topic_filter",
                "TTN v3 connectors should include topic_filter before live MQTT operation",
            )),
        }
        if !payload_format_supported {
            issues.push(ttn_validation_issue(
                "unsupported_ttn_payload_format",
                "TTN v3 connectors require payload_format ttn-uplink-json",
            ));
        }
        if mapping_count == 0 {
            warnings.push(ttn_validation_issue(
                "missing_ttn_device_mappings",
                "TTN v3 connector has no device mappings; unmapped uplinks without explicit entity IDs will fail safely",
            ));
        } else if enabled_mapping_count == 0 {
            warnings.push(ttn_validation_issue(
                "no_enabled_ttn_device_mappings",
                "TTN v3 connector has mappings, but none are enabled",
            ));
        }
        if connector
            .broker_url
            .as_deref()
            .map(is_public_ttn_broker_url)
            .unwrap_or(false)
            && connector.secret_ref_id.is_none()
        {
            warnings.push(ttn_validation_issue(
                "missing_secret_ref",
                "public TTN/The Things Stack brokers usually require MQTT authentication; configure secret_ref_id before live operation",
            ));
        }
        if let Some(secret_ref_id) = connector.secret_ref_id {
            match state
                .storage
                .get_connector_secret(state.tenant_id, secret_ref_id)?
            {
                Some(secret) => {
                    secret_type = Some(secret.secret_type.clone());
                    let has_secret_value = !secret.secret_value.is_empty();
                    if secret.secret_type != ConnectorSecretType::MqttBasicAuth {
                        issues.push(ttn_validation_issue(
                            "incompatible_secret_type",
                            "TTN v3 MQTT authentication currently expects a mqtt_basic_auth connector secret",
                        ));
                    }
                    if secret.secret_type == ConnectorSecretType::MqttBasicAuth
                        && secret
                            .username
                            .as_deref()
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .is_none()
                    {
                        issues.push(ttn_validation_issue(
                            "missing_secret_username",
                            "TTN v3 mqtt_basic_auth secrets should include the MQTT username/application identifier",
                        ));
                    }
                    if !has_secret_value {
                        issues.push(ttn_validation_issue(
                            "missing_secret_value",
                            "referenced connector secret does not contain an internal secret value",
                        ));
                    }
                    secret_configured = secret.secret_type == ConnectorSecretType::MqttBasicAuth
                        && secret
                            .username
                            .as_deref()
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .is_some()
                        && has_secret_value;
                }
                None => issues.push(ttn_validation_issue(
                    "secret_ref_not_found",
                    "connector secret_ref_id does not reference an existing connector secret",
                )),
            }
        }
        if !connector.enabled {
            warnings.push(ttn_validation_issue(
                "connector_disabled",
                "connector is disabled; configuration can be valid, but runtime readiness is degraded until enabled",
            ));
        }
    }

    let readiness = if !issues.is_empty() {
        TtnConnectorReadiness::Invalid
    } else if connector.connector_profile == ConnectorProfile::TtnV3
        && connector.enabled
        && enabled_mapping_count > 0
        && warnings.is_empty()
    {
        TtnConnectorReadiness::Ready
    } else {
        TtnConnectorReadiness::Degraded
    };

    Ok(TtnConnectorValidation {
        connector_id: connector.id,
        connector_key: connector.connector_key.clone(),
        valid: issues.is_empty(),
        readiness,
        issues,
        warnings,
        detected_profile: connector.connector_profile.clone(),
        expected_topic_shape: "v3/{application_id}/devices/{device_id}/up",
        mapping_count,
        enabled_mapping_count,
        has_secret_ref: connector.secret_ref_id.is_some(),
        secret_configured,
        secret_type,
        payload_format_supported,
        operator_hints,
        generated_at: Utc::now(),
    })
}

fn ttn_validation_issue(
    code: impl Into<String>,
    message: impl Into<String>,
) -> TtnConnectorValidationIssue {
    TtnConnectorValidationIssue {
        code: code.into(),
        message: message.into(),
    }
}

fn ttn_operator_hints() -> Vec<String> {
    vec![
        "Public TTN/The Things Stack MQTT brokers typically require authentication.".to_string(),
        "The MQTT username is usually application-specific and may include tenant or deployment context.".to_string(),
        "Store the MQTT password or API token as a connector secret; validation never returns secret_value.".to_string(),
        "Use a topic_filter shaped like v3/{application_id}/devices/{device_id}/up or v3/{application_id}/devices/+/up.".to_string(),
        "No live credential or broker verification is performed by this validation endpoint.".to_string(),
    ]
}

fn ttn_live_readiness_plan(
    state: &AppState,
    connector: &IngestionConnector,
) -> Result<TtnLiveReadinessPlan, ApiError> {
    let validation = connector_validation(state, connector)?;
    let mut checks = Vec::new();
    let mut blockers = Vec::new();
    let mut warnings = Vec::new();
    let mut required_operator_steps = Vec::new();

    if connector.connector_profile != ConnectorProfile::TtnV3 {
        checks.push(ttn_live_check(
            "connector_profile_is_ttn_v3",
            "Connector profile is TTN v3",
            TtnLiveReadinessCheckStatus::Skipped,
            Some("profile-specific TTN live readiness planning is not applicable".to_string()),
            false,
        ));
        warnings.push(ttn_validation_issue(
            "not_applicable",
            "TTN live readiness planning applies only to ttn-v3 connectors",
        ));
        return Ok(TtnLiveReadinessPlan {
            connector_id: connector.id,
            connector_key: connector.connector_key.clone(),
            dry_run: true,
            can_attempt_live_validation: false,
            readiness: TtnConnectorReadiness::Degraded,
            checks,
            blockers,
            warnings,
            required_operator_steps,
            safe_to_connect: false,
            generated_at: Utc::now(),
        });
    }

    let profile_ok = connector.connector_profile == ConnectorProfile::TtnV3;
    checks.push(ttn_live_check_from_bool(
        "connector_profile_is_ttn_v3",
        "Connector profile is TTN v3",
        profile_ok,
        "connector_profile must be ttn-v3",
        false,
    ));
    if !profile_ok {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "connector_profile_not_ttn_v3",
            "connector profile must be ttn-v3",
            "Create or select a connector with connector_profile = ttn-v3.",
        );
    }

    let connector_type_ok = connector.connector_type == IngestionConnectorType::Mqtt;
    checks.push(ttn_live_check_from_bool(
        "connector_type_is_mqtt",
        "Connector type is MQTT",
        connector_type_ok,
        "connector_type must be mqtt",
        false,
    ));
    if !connector_type_ok {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "connector_type_not_mqtt",
            "connector_type must be mqtt",
            "Create or update the TTN connector so connector_type = mqtt.",
        );
    }

    let broker_url_present = connector
        .broker_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some();
    checks.push(ttn_live_check_from_bool(
        "broker_url_present",
        "Broker URL is configured",
        broker_url_present,
        "broker_url is missing",
        true,
    ));
    if !broker_url_present {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "missing_broker_url",
            "broker_url is required before any future live TTN validation attempt",
            "Set broker_url to the TTN/The Things Stack MQTT endpoint for the deployment.",
        );
    }

    let topic_filter_present = connector
        .topic_filter
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some();
    checks.push(ttn_live_check_from_bool(
        "topic_filter_present",
        "Topic filter is configured",
        topic_filter_present,
        "topic_filter is missing",
        true,
    ));
    if !topic_filter_present {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "missing_topic_filter",
            "topic_filter is required before any future live TTN validation attempt",
            "Set topic_filter to a TTN uplink topic such as v3/{application_id}/devices/+/up.",
        );
    }

    let topic_filter_plausibly_ttn = connector
        .topic_filter
        .as_deref()
        .map(is_plausible_ttn_topic_filter)
        .unwrap_or(false);
    checks.push(ttn_live_check(
        "topic_filter_plausibly_ttn",
        "Topic filter looks like a TTN uplink topic",
        if topic_filter_present {
            if topic_filter_plausibly_ttn {
                TtnLiveReadinessCheckStatus::Pass
            } else {
                TtnLiveReadinessCheckStatus::Fail
            }
        } else {
            TtnLiveReadinessCheckStatus::Skipped
        },
        if topic_filter_present && !topic_filter_plausibly_ttn {
            Some("topic_filter should contain v3/, /devices/, and /up".to_string())
        } else if !topic_filter_present {
            Some("topic_filter is missing".to_string())
        } else {
            None
        },
        true,
    ));
    if topic_filter_present && !topic_filter_plausibly_ttn {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "implausible_ttn_topic_filter",
            "topic_filter does not look like a TTN uplink topic",
            "Set topic_filter to match the application/device uplink topic shape.",
        );
    }

    checks.push(ttn_live_check_from_bool(
        "payload_format_is_ttn_uplink_json",
        "Payload format is ttn-uplink-json",
        validation.payload_format_supported,
        "payload_format must be ttn-uplink-json",
        false,
    ));
    if !validation.payload_format_supported {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "unsupported_ttn_payload_format",
            "payload_format must be ttn-uplink-json",
            "Set payload_format = ttn-uplink-json.",
        );
    }

    checks.push(ttn_live_check_from_bool(
        "secret_ref_present",
        "Connector references a connector secret",
        validation.has_secret_ref,
        "secret_ref_id is missing",
        false,
    ));
    if !validation.has_secret_ref {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "missing_secret_ref",
            "secret_ref_id is required before any future live TTN validation attempt",
            "Create a mqtt_basic_auth connector secret and attach it with secret_ref_id.",
        );
    }

    let secret_ref_resolves =
        validation.has_secret_ref && !ttn_validation_has_issue(&validation, "secret_ref_not_found");
    checks.push(ttn_live_check(
        "secret_ref_resolves",
        "Connector secret reference resolves",
        if validation.has_secret_ref {
            if secret_ref_resolves {
                TtnLiveReadinessCheckStatus::Pass
            } else {
                TtnLiveReadinessCheckStatus::Fail
            }
        } else {
            TtnLiveReadinessCheckStatus::Skipped
        },
        if validation.has_secret_ref && !secret_ref_resolves {
            Some("secret_ref_id does not reference an existing connector secret".to_string())
        } else if !validation.has_secret_ref {
            Some("secret_ref_id is missing".to_string())
        } else {
            None
        },
        false,
    ));
    if validation.has_secret_ref && !secret_ref_resolves {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "secret_ref_not_found",
            "secret_ref_id does not reference an existing connector secret",
            "Attach secret_ref_id to an existing connector secret.",
        );
    }

    let secret_type_is_mqtt_basic_auth =
        validation.secret_type == Some(ConnectorSecretType::MqttBasicAuth);
    checks.push(ttn_live_check(
        "secret_type_is_mqtt_basic_auth",
        "Connector secret type is mqtt_basic_auth",
        if secret_ref_resolves {
            if secret_type_is_mqtt_basic_auth {
                TtnLiveReadinessCheckStatus::Pass
            } else {
                TtnLiveReadinessCheckStatus::Fail
            }
        } else {
            TtnLiveReadinessCheckStatus::Skipped
        },
        if secret_ref_resolves && !secret_type_is_mqtt_basic_auth {
            Some("referenced secret must use secret_type mqtt_basic_auth".to_string())
        } else if !secret_ref_resolves {
            Some("secret reference does not resolve".to_string())
        } else {
            None
        },
        false,
    ));
    if secret_ref_resolves && !secret_type_is_mqtt_basic_auth {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "incompatible_secret_type",
            "referenced secret must use secret_type mqtt_basic_auth",
            "Create or attach a connector secret with secret_type = mqtt_basic_auth.",
        );
    }

    let secret_username_present = secret_type_is_mqtt_basic_auth
        && !ttn_validation_has_issue(&validation, "missing_secret_username");
    checks.push(ttn_live_check(
        "secret_username_present",
        "Connector secret has a username",
        if secret_type_is_mqtt_basic_auth {
            if secret_username_present {
                TtnLiveReadinessCheckStatus::Pass
            } else {
                TtnLiveReadinessCheckStatus::Fail
            }
        } else {
            TtnLiveReadinessCheckStatus::Skipped
        },
        if secret_type_is_mqtt_basic_auth && !secret_username_present {
            Some("mqtt_basic_auth secret is missing username".to_string())
        } else if !secret_type_is_mqtt_basic_auth {
            Some("secret type is not mqtt_basic_auth".to_string())
        } else {
            None
        },
        false,
    ));
    if secret_type_is_mqtt_basic_auth && !secret_username_present {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "missing_secret_username",
            "mqtt_basic_auth secret is missing username",
            "Set the connector secret username to the TTN MQTT username for the deployment.",
        );
    }

    let secret_value_present = validation.secret_configured
        && !ttn_validation_has_issue(&validation, "missing_secret_value");
    checks.push(ttn_live_check(
        "secret_value_present_internally",
        "Connector secret has an internal secret value",
        if secret_type_is_mqtt_basic_auth {
            if secret_value_present {
                TtnLiveReadinessCheckStatus::Pass
            } else {
                TtnLiveReadinessCheckStatus::Fail
            }
        } else {
            TtnLiveReadinessCheckStatus::Skipped
        },
        if secret_type_is_mqtt_basic_auth && !secret_value_present {
            Some("mqtt_basic_auth secret is missing secret_value".to_string())
        } else if !secret_type_is_mqtt_basic_auth {
            Some("secret type is not mqtt_basic_auth".to_string())
        } else {
            None
        },
        false,
    ));
    if secret_type_is_mqtt_basic_auth && !secret_value_present {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "missing_secret_value",
            "mqtt_basic_auth secret is missing secret_value",
            "Store the TTN MQTT password or API token in the connector secret_value.",
        );
    }

    checks.push(ttn_live_check_from_bool(
        "at_least_one_enabled_ttn_mapping",
        "At least one enabled TTN device mapping exists",
        validation.enabled_mapping_count > 0,
        "no enabled TTN device mapping exists",
        false,
    ));
    if validation.enabled_mapping_count == 0 {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "missing_enabled_ttn_device_mapping",
            "at least one enabled TTN device mapping is required before live validation",
            "Create and enable a TTN device mapping for the connector.",
        );
    }

    checks.push(ttn_live_check(
        "no_network_call_performed",
        "Dry-run plan did not contact TTN or any broker",
        TtnLiveReadinessCheckStatus::Pass,
        Some("this endpoint is deterministic and non-network".to_string()),
        false,
    ));

    if !connector.enabled {
        warnings.push(ttn_validation_issue(
            "connector_disabled",
            "connector is disabled; enable it before any future live validation attempt",
        ));
        push_unique_step(
            &mut required_operator_steps,
            "Enable the TTN connector before attempting live validation.",
        );
    }

    for issue in &validation.issues {
        push_unique_issue(&mut blockers, issue.clone());
    }
    for warning in &validation.warnings {
        push_unique_issue(&mut warnings, warning.clone());
    }

    let safe_to_connect = connector.enabled && blockers.is_empty();
    let readiness = if safe_to_connect {
        TtnConnectorReadiness::Ready
    } else if blockers.is_empty() {
        TtnConnectorReadiness::Degraded
    } else {
        TtnConnectorReadiness::Invalid
    };

    Ok(TtnLiveReadinessPlan {
        connector_id: connector.id,
        connector_key: connector.connector_key.clone(),
        dry_run: true,
        can_attempt_live_validation: safe_to_connect,
        readiness,
        checks,
        blockers,
        warnings,
        required_operator_steps,
        safe_to_connect,
        generated_at: Utc::now(),
    })
}

async fn ttn_live_validation_preflight(
    state: &AppState,
    connector: &IngestionConnector,
    request: TtnLiveValidationRequest,
) -> Result<TtnLiveValidationResponse, ApiError> {
    let started_at = Utc::now();
    let timer = Instant::now();
    let requested_timeout_seconds = request.timeout_seconds.unwrap_or(5);
    let timeout_seconds = requested_timeout_seconds.clamp(1, 60);
    let expect_message = request.expect_message.unwrap_or(false);
    let dry_run_only = request.dry_run_only.unwrap_or(false);
    let plan = ttn_live_readiness_plan(state, connector)?;
    let plan_summary = ttn_live_validation_plan_summary(&plan);
    let mut response_warnings = plan.warnings.clone();
    if requested_timeout_seconds > 60 {
        response_warnings.push(ttn_validation_issue(
            "timeout_seconds_capped",
            "timeout_seconds was capped at 60 seconds",
        ));
    }
    let broker_url_redacted_or_safe = connector
        .broker_url
        .as_deref()
        .map(redact_broker_url_for_response);
    let topic_filter = connector.topic_filter.clone();

    if dry_run_only {
        let response = ttn_live_validation_response(
            connector,
            false,
            plan.safe_to_connect,
            false,
            false,
            false,
            broker_url_redacted_or_safe,
            topic_filter,
            timer.elapsed().as_millis(),
            started_at,
            TtnLiveValidationResultStatus::Skipped,
            Vec::new(),
            response_warnings.clone(),
            plan_summary,
        );
        record_ttn_live_validation_event(
            state,
            "aion:TtnLiveValidationSkipped",
            connector,
            &response,
            Some("TTN live validation skipped because dry_run_only=true".to_string()),
        )?;
        return Ok(response);
    }

    if !plan.safe_to_connect {
        let response = ttn_live_validation_response(
            connector,
            false,
            false,
            false,
            false,
            false,
            broker_url_redacted_or_safe,
            topic_filter,
            timer.elapsed().as_millis(),
            started_at,
            TtnLiveValidationResultStatus::Skipped,
            plan.blockers.clone(),
            response_warnings.clone(),
            plan_summary,
        );
        record_ttn_live_validation_event(
            state,
            "aion:TtnLiveValidationSkipped",
            connector,
            &response,
            Some("TTN live validation skipped because dry-run blockers remain".to_string()),
        )?;
        return Ok(response);
    }

    record_ttn_live_validation_started_event(state, connector, timeout_seconds, expect_message)?;

    let Some(secret_ref_id) = connector.secret_ref_id else {
        let response = ttn_live_validation_response(
            connector,
            false,
            true,
            false,
            false,
            false,
            broker_url_redacted_or_safe,
            topic_filter,
            timer.elapsed().as_millis(),
            started_at,
            TtnLiveValidationResultStatus::Failed,
            vec![ttn_validation_issue(
                "missing_secret_ref",
                "connector has no secret_ref_id for TTN MQTT authentication",
            )],
            response_warnings.clone(),
            plan_summary,
        );
        record_ttn_live_validation_event(
            state,
            "aion:TtnLiveValidationFailed",
            connector,
            &response,
            Some("TTN live validation failed before connection".to_string()),
        )?;
        return Ok(response);
    };

    let secret = state
        .storage
        .get_connector_secret(state.tenant_id, secret_ref_id)?
        .ok_or_else(|| ApiError::bad_request("connector secret_ref_id does not resolve"))?;
    if secret.secret_type != ConnectorSecretType::MqttBasicAuth {
        return Err(ApiError::bad_request(
            "TTN live validation requires a mqtt_basic_auth connector secret",
        ));
    }

    let broker_url = connector
        .broker_url
        .as_deref()
        .ok_or_else(|| ApiError::bad_request("broker_url is required"))?;
    let topic_filter_value = connector
        .topic_filter
        .as_deref()
        .ok_or_else(|| ApiError::bad_request("topic_filter is required"))?;

    let live_result = run_ttn_mqtt_live_preflight(
        connector,
        &secret,
        broker_url,
        topic_filter_value,
        timeout_seconds,
        expect_message,
        request.client_id_suffix.as_deref(),
    )
    .await;

    let duration_ms = timer.elapsed().as_millis();
    let response = match live_result {
        Ok(result) => {
            let succeeded = result.connected
                && result.subscribed
                && (!expect_message || result.message_received);
            ttn_live_validation_response(
                connector,
                true,
                true,
                result.connected,
                result.subscribed,
                result.message_received,
                broker_url_redacted_or_safe,
                topic_filter,
                duration_ms,
                started_at,
                if succeeded {
                    TtnLiveValidationResultStatus::Success
                } else {
                    TtnLiveValidationResultStatus::Failed
                },
                result.errors,
                response_warnings.clone(),
                plan_summary,
            )
        }
        Err(error) => ttn_live_validation_response(
            connector,
            true,
            true,
            false,
            false,
            false,
            broker_url_redacted_or_safe,
            topic_filter,
            duration_ms,
            started_at,
            TtnLiveValidationResultStatus::Failed,
            vec![ttn_validation_issue(
                "live_validation_failed",
                sanitize_live_validation_error(error, &secret.secret_value),
            )],
            response_warnings.clone(),
            plan_summary,
        ),
    };

    let event_type = if response.result == TtnLiveValidationResultStatus::Success {
        "aion:TtnLiveValidationSucceeded"
    } else {
        "aion:TtnLiveValidationFailed"
    };
    record_ttn_live_validation_event(
        state,
        event_type,
        connector,
        &response,
        Some("TTN live validation preflight completed".to_string()),
    )?;

    Ok(response)
}

struct TtnMqttLivePreflightResult {
    connected: bool,
    subscribed: bool,
    message_received: bool,
    errors: Vec<TtnConnectorValidationIssue>,
}

async fn run_ttn_mqtt_live_preflight(
    connector: &IngestionConnector,
    secret: &ConnectorSecret,
    broker_url: &str,
    topic_filter: &str,
    timeout_seconds: u64,
    expect_message: bool,
    client_id_suffix: Option<&str>,
) -> Result<TtnMqttLivePreflightResult, String> {
    let (host, port) = parse_mqtt_broker_url_for_live_preflight(broker_url)?;
    let client_id = ttn_live_validation_client_id(connector, client_id_suffix);
    let mut options = rumqttc::MqttOptions::new(client_id, host, port);
    options.set_keep_alive(std::time::Duration::from_secs(timeout_seconds.max(5)));
    if let Some(username) = secret.username.as_deref() {
        options.set_credentials(username, &secret.secret_value);
    }

    let (client, mut eventloop) = rumqttc::AsyncClient::new(options, 10);
    client
        .subscribe(topic_filter, rumqttc::QoS::AtLeastOnce)
        .await
        .map_err(|err| format!("failed to send MQTT subscribe request: {err}"))?;

    let deadline = std::time::Duration::from_secs(timeout_seconds);
    let mut connected = false;
    let mut subscribed = false;
    let mut message_received = false;
    let mut errors = Vec::new();

    let poll_result = time::timeout(deadline, async {
        loop {
            match eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => {
                    connected = true;
                }
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::SubAck(_))) => {
                    subscribed = true;
                    if !expect_message {
                        break;
                    }
                }
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(_publish))) => {
                    message_received = true;
                    break;
                }
                Ok(_) => {}
                Err(err) => {
                    errors.push(ttn_validation_issue(
                        "mqtt_event_loop_error",
                        format!("MQTT event loop failed: {err}"),
                    ));
                    break;
                }
            }
        }
    })
    .await;

    if poll_result.is_err() {
        let code = if expect_message {
            "message_wait_timeout"
        } else {
            "subscribe_timeout"
        };
        let message = if expect_message {
            format!(
                "MQTT connection/subscription did not receive a matching message within {timeout_seconds} seconds"
            )
        } else {
            format!(
                "MQTT connection/subscription did not complete within {timeout_seconds} seconds"
            )
        };
        errors.push(ttn_validation_issue(code, message));
    }

    let _ = client.disconnect().await;

    Ok(TtnMqttLivePreflightResult {
        connected,
        subscribed,
        message_received,
        errors,
    })
}

fn parse_mqtt_broker_url_for_live_preflight(value: &str) -> Result<(String, u16), String> {
    let trimmed = value.trim();
    let without_scheme = trimmed
        .strip_prefix("mqtt://")
        .ok_or_else(|| "unsupported MQTT broker URL; expected mqtt://host:port".to_string())?;
    let host_port = without_scheme.split('/').next().unwrap_or(without_scheme);
    let host_port = host_port.split('@').next_back().unwrap_or(host_port);
    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, port)) => {
            let port = port
                .parse::<u16>()
                .map_err(|err| format!("invalid MQTT broker port: {err}"))?;
            (host.to_string(), port)
        }
        None => (host_port.to_string(), 1883),
    };

    if host.trim().is_empty() {
        return Err("invalid MQTT broker URL: host is empty".to_string());
    }

    Ok((host, port))
}

fn ttn_live_validation_client_id(
    connector: &IngestionConnector,
    client_id_suffix: Option<&str>,
) -> String {
    let base = connector
        .client_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("aioncore-ttn-live-{}", connector.connector_key));
    match client_id_suffix
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(suffix) => format!("{base}-{suffix}"),
        None => base,
    }
}

fn ttn_live_validation_response(
    connector: &IngestionConnector,
    attempted_live_connection: bool,
    dry_run_passed: bool,
    connected: bool,
    subscribed: bool,
    message_received: bool,
    broker_url_redacted_or_safe: Option<String>,
    topic_filter: Option<String>,
    duration_ms: u128,
    started_at: DateTime<Utc>,
    result: TtnLiveValidationResultStatus,
    errors: Vec<TtnConnectorValidationIssue>,
    warnings: Vec<TtnConnectorValidationIssue>,
    dry_run_plan_summary: TtnLiveValidationPlanSummary,
) -> TtnLiveValidationResponse {
    TtnLiveValidationResponse {
        connector_id: connector.id,
        connector_key: connector.connector_key.clone(),
        attempted_live_connection,
        dry_run_passed,
        connected,
        subscribed,
        message_received,
        broker_url_redacted_or_safe,
        topic_filter,
        duration_ms,
        started_at,
        finished_at: Utc::now(),
        result,
        errors,
        warnings,
        dry_run_plan_summary,
        secret_exposed: false,
    }
}

fn ttn_live_validation_plan_summary(plan: &TtnLiveReadinessPlan) -> TtnLiveValidationPlanSummary {
    TtnLiveValidationPlanSummary {
        safe_to_connect: plan.safe_to_connect,
        can_attempt_live_validation: plan.can_attempt_live_validation,
        readiness: plan.readiness.clone(),
        blocker_count: plan.blockers.len(),
        warning_count: plan.warnings.len(),
    }
}

fn redact_broker_url_for_response(value: &str) -> String {
    let trimmed = value.trim();
    let Some((scheme, rest)) = trimmed.split_once("://") else {
        return trimmed.to_string();
    };
    if let Some((_, after_userinfo)) = rest.rsplit_once('@') {
        format!("{scheme}://***REDACTED***@{after_userinfo}")
    } else {
        trimmed.to_string()
    }
}

fn sanitize_live_validation_error(error: String, secret_value: &str) -> String {
    if secret_value.is_empty() {
        return error;
    }
    error.replace(secret_value, "***REDACTED***")
}

fn record_ttn_live_validation_started_event(
    state: &AppState,
    connector: &IngestionConnector,
    timeout_seconds: u64,
    expect_message: bool,
) -> Result<Event, ApiError> {
    record_connector_worker_event(
        state,
        "aion:TtnLiveValidationStarted",
        EventSeverity::Info,
        Some("TTN live validation preflight started".to_string()),
        json!({
            "connector_id": connector.id,
            "connector_key": connector.connector_key,
            "connector_profile": connector.connector_profile,
            "broker_url": connector.broker_url.as_deref().map(redact_broker_url_for_response),
            "topic_filter": connector.topic_filter,
            "timeout_seconds": timeout_seconds,
            "expect_message": expect_message,
            "secret_exposed": false
        }),
    )
}

fn record_ttn_live_validation_event(
    state: &AppState,
    event_type: impl Into<String>,
    connector: &IngestionConnector,
    response: &TtnLiveValidationResponse,
    message: Option<String>,
) -> Result<Event, ApiError> {
    record_connector_worker_event(
        state,
        event_type,
        match response.result {
            TtnLiveValidationResultStatus::Success | TtnLiveValidationResultStatus::Skipped => {
                EventSeverity::Info
            }
            TtnLiveValidationResultStatus::Failed => EventSeverity::Error,
        },
        message,
        json!({
            "connector_id": connector.id,
            "connector_key": connector.connector_key,
            "connector_profile": connector.connector_profile,
            "attempted_live_connection": response.attempted_live_connection,
            "dry_run_passed": response.dry_run_passed,
            "connected": response.connected,
            "subscribed": response.subscribed,
            "message_received": response.message_received,
            "broker_url": response.broker_url_redacted_or_safe,
            "topic_filter": response.topic_filter,
            "duration_ms": response.duration_ms,
            "result": response.result,
            "error_codes": response.errors.iter().map(|issue| issue.code.clone()).collect::<Vec<_>>(),
            "warning_codes": response.warnings.iter().map(|issue| issue.code.clone()).collect::<Vec<_>>(),
            "secret_exposed": false
        }),
    )
}

fn ttn_validation_has_issue(validation: &TtnConnectorValidation, code: &str) -> bool {
    validation.issues.iter().any(|issue| issue.code == code)
}

fn ttn_live_check_from_bool(
    check_key: &'static str,
    description: &'static str,
    passed: bool,
    failure_reason: &'static str,
    future_live_check: bool,
) -> TtnLiveReadinessCheck {
    ttn_live_check(
        check_key,
        description,
        if passed {
            TtnLiveReadinessCheckStatus::Pass
        } else {
            TtnLiveReadinessCheckStatus::Fail
        },
        if passed {
            None
        } else {
            Some(failure_reason.to_string())
        },
        future_live_check,
    )
}

fn ttn_live_check(
    check_key: &'static str,
    description: &'static str,
    status: TtnLiveReadinessCheckStatus,
    reason: Option<String>,
    future_live_check: bool,
) -> TtnLiveReadinessCheck {
    TtnLiveReadinessCheck {
        check_key,
        description,
        status,
        reason,
        future_live_check,
    }
}

fn add_ttn_live_blocker(
    blockers: &mut Vec<TtnConnectorValidationIssue>,
    required_operator_steps: &mut Vec<String>,
    code: &'static str,
    message: &'static str,
    step: &'static str,
) {
    push_unique_issue(blockers, ttn_validation_issue(code, message));
    push_unique_step(required_operator_steps, step);
}

fn push_unique_issue(
    issues: &mut Vec<TtnConnectorValidationIssue>,
    issue: TtnConnectorValidationIssue,
) {
    if !issues.iter().any(|existing| existing.code == issue.code) {
        issues.push(issue);
    }
}

fn push_unique_step(steps: &mut Vec<String>, step: &'static str) {
    if !steps.iter().any(|existing| existing == step) {
        steps.push(step.to_string());
    }
}

fn is_plausible_ttn_topic_filter(topic_filter: &str) -> bool {
    let normalized = topic_filter.trim().to_ascii_lowercase();
    normalized.contains("v3/")
        && normalized.contains("/devices/")
        && (normalized.ends_with("/up") || normalized.contains("/up/"))
}

fn is_public_ttn_broker_url(broker_url: &str) -> bool {
    let normalized = broker_url.trim().to_ascii_lowercase();
    normalized.contains("thethings.network")
        || normalized.contains("thethings.industries")
        || normalized.contains("thethingsstack")
}

fn ttn_device_mapping_response(mapping: TtnDeviceMapping) -> TtnDeviceMappingResponse {
    TtnDeviceMappingResponse {
        id: mapping.id,
        tenant_id: mapping.tenant_id,
        connector_id: mapping.connector_id,
        ttn_application_id: mapping.ttn_application_id,
        ttn_device_id: mapping.ttn_device_id,
        producer_entity_id: mapping.producer_entity_id,
        feature_of_interest_id: mapping.feature_of_interest_id,
        enabled: mapping.enabled,
        metadata: mapping.metadata,
        created_at: mapping.created_at,
        updated_at: mapping.updated_at,
    }
}
