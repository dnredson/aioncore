use crate::{
    auth::{require_scope, AuthContext},
    build_ingestion_worker_plan, build_ready_worker_plan_summary, connector_worker_start_decision,
    connector_workers_enabled,
    error::ApiError,
    reconcile_connector_workers, AppState, ConnectorWorkerRuntimeState, IngestionWorkerPlan,
    IngestionWorkerSpec, IngestionWorkerValidationIssue, ReadyWorkerPlanSummary,
};
use aion_storage::{ConnectorProfile, IngestionConnectorType};
use axum::{
    extract::{Extension, State},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/ingestion/workers/plan", get(get_ingestion_worker_plan))
        .route(
            "/ingestion/workers/status",
            get(get_ingestion_workers_status),
        )
        .route(
            "/ingestion/workers/reconcile",
            post(reconcile_ingestion_workers),
        )
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ConnectorWorkerRuntimeStatus {
    pub connector_id: Uuid,
    pub connector_key: String,
    pub connector_type: IngestionConnectorType,
    pub connector_profile: ConnectorProfile,
    pub enabled: bool,
    pub worker_kind: crate::IngestionWorkerKind,
    pub status: ConnectorWorkerRuntimeState,
    pub connected: bool,
    pub subscribed: bool,
    pub broker_url: Option<String>,
    pub client_id: Option<String>,
    pub topic_filter: Option<String>,
    pub http_path: Option<String>,
    pub payload_format: Option<String>,
    pub content_type: Option<String>,
    pub secret_ref_id: Option<Uuid>,
    pub last_error: Option<String>,
    pub last_message_at: Option<DateTime<Utc>>,
    pub last_successful_ingest_at: Option<DateTime<Utc>>,
    pub last_failed_ingest_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub stopped_at: Option<DateTime<Utc>>,
    pub restart_count: u32,
    pub reconnect_attempts: u32,
    pub last_disconnect_at: Option<DateTime<Utc>>,
    pub last_reconnect_at: Option<DateTime<Utc>>,
    pub next_reconnect_at: Option<DateTime<Utc>>,
    pub last_reconciled_at: Option<DateTime<Utc>>,
    pub validation_issues: Vec<IngestionWorkerValidationIssue>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ConnectorWorkersReadiness {
    pub enabled: bool,
    pub total: usize,
    pub running: usize,
    pub degraded: usize,
    pub stopped: usize,
    pub skipped: usize,
    pub invalid: usize,
    pub errors: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct IngestionWorkersStatusResponse {
    pub connector_workers: ConnectorWorkersReadiness,
    pub workers: Vec<ConnectorWorkerRuntimeStatus>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ReconcileConnectorWorkersResponse {
    pub connector_workers: ConnectorWorkersReadiness,
    pub actions: Vec<ConnectorWorkerReconcileAction>,
    pub workers: Vec<ConnectorWorkerRuntimeStatus>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ConnectorWorkerReconcileAction {
    pub connector_id: Uuid,
    pub connector_key: String,
    pub action: String,
    pub reason: Option<String>,
}

async fn get_ingestion_worker_plan(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<IngestionWorkerPlan>, ApiError> {
    require_scope(&state, &auth, "/ingestion/workers/plan", "connectors:read")?;
    Ok(Json(build_ingestion_worker_plan(&state)?))
}

async fn get_ingestion_workers_status(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<IngestionWorkersStatusResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/workers/status",
        "connectors:read",
    )?;
    Ok(Json(connector_workers_status(&state)?))
}

async fn reconcile_ingestion_workers(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<ReconcileConnectorWorkersResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/workers/reconcile",
        "connectors:admin",
    )?;
    reconcile_connector_workers(state, true).await.map(Json)
}

pub(crate) fn worker_plan_summary(state: &AppState) -> ReadyWorkerPlanSummary {
    build_ready_worker_plan_summary(state)
}

pub(crate) fn connector_worker_action(
    spec: &IngestionWorkerSpec,
    action: &str,
    reason: Option<&str>,
) -> ConnectorWorkerReconcileAction {
    ConnectorWorkerReconcileAction {
        connector_id: spec.connector_id,
        connector_key: spec.connector_key.clone(),
        action: action.to_string(),
        reason: reason.map(ToOwned::to_owned),
    }
}

pub(crate) fn connector_runtime_status_from_spec(
    spec: &IngestionWorkerSpec,
) -> ConnectorWorkerRuntimeStatus {
    let decision = connector_worker_start_decision(spec);
    let status = match decision {
        crate::ConnectorWorkerStartDecision::StartMqtt => ConnectorWorkerRuntimeState::Planned,
        crate::ConnectorWorkerStartDecision::Skip => ConnectorWorkerRuntimeState::Skipped,
        crate::ConnectorWorkerStartDecision::Invalid => ConnectorWorkerRuntimeState::Invalid,
        crate::ConnectorWorkerStartDecision::Unsupported => {
            ConnectorWorkerRuntimeState::Unsupported
        }
        crate::ConnectorWorkerStartDecision::PlannedOnly => ConnectorWorkerRuntimeState::Planned,
    };
    let last_error = if matches!(
        status,
        ConnectorWorkerRuntimeState::Invalid | ConnectorWorkerRuntimeState::Unsupported
    ) {
        Some(
            spec.validation_issues
                .iter()
                .map(|issue| issue.message.as_str())
                .collect::<Vec<_>>()
                .join("; "),
        )
        .filter(|value| !value.is_empty())
    } else {
        None
    };

    ConnectorWorkerRuntimeStatus {
        connector_id: spec.connector_id,
        connector_key: spec.connector_key.clone(),
        connector_type: spec.connector_type.clone(),
        connector_profile: spec.connector_profile.clone(),
        enabled: spec.enabled,
        worker_kind: spec.worker_kind.clone(),
        status,
        connected: false,
        subscribed: false,
        broker_url: spec.broker_url.clone(),
        client_id: spec.client_id.clone(),
        topic_filter: spec.topic_filter.clone(),
        http_path: spec.http_path.clone(),
        payload_format: spec.payload_format.clone(),
        content_type: spec.content_type.clone(),
        secret_ref_id: spec.secret_ref_id,
        last_error,
        last_message_at: None,
        last_successful_ingest_at: None,
        last_failed_ingest_at: None,
        started_at: None,
        stopped_at: None,
        restart_count: 0,
        reconnect_attempts: 0,
        last_disconnect_at: None,
        last_reconnect_at: None,
        next_reconnect_at: None,
        last_reconciled_at: None,
        validation_issues: spec.validation_issues.clone(),
        metadata: spec.metadata.clone(),
    }
}

pub(crate) fn connector_workers_status(
    state: &AppState,
) -> Result<IngestionWorkersStatusResponse, ApiError> {
    let plan = build_ingestion_worker_plan(state)?;
    let runtime_statuses = state
        .connector_worker_statuses
        .read()
        .map(|guard| guard.clone())
        .unwrap_or_default();
    let workers = plan
        .specs
        .iter()
        .map(|spec| {
            runtime_statuses
                .get(&spec.connector_id)
                .cloned()
                .unwrap_or_else(|| connector_runtime_status_from_spec(spec))
        })
        .collect::<Vec<_>>();

    Ok(IngestionWorkersStatusResponse {
        connector_workers: connector_workers_readiness_from_workers(
            connector_workers_enabled(state),
            &workers,
        ),
        workers,
    })
}

pub(crate) fn connector_workers_readiness(state: &AppState) -> ConnectorWorkersReadiness {
    connector_workers_status(state)
        .map(|status| status.connector_workers)
        .unwrap_or_else(|_| ConnectorWorkersReadiness {
            enabled: connector_workers_enabled(state),
            total: 0,
            running: 0,
            degraded: 0,
            stopped: 0,
            skipped: 0,
            invalid: 0,
            errors: 1,
        })
}

fn connector_workers_readiness_from_workers(
    enabled: bool,
    workers: &[ConnectorWorkerRuntimeStatus],
) -> ConnectorWorkersReadiness {
    ConnectorWorkersReadiness {
        enabled,
        total: workers.len(),
        running: workers
            .iter()
            .filter(|worker| worker.status == ConnectorWorkerRuntimeState::Running)
            .count(),
        degraded: workers
            .iter()
            .filter(|worker| {
                matches!(
                    worker.status,
                    ConnectorWorkerRuntimeState::Degraded
                        | ConnectorWorkerRuntimeState::Reconnecting
                )
            })
            .count(),
        stopped: workers
            .iter()
            .filter(|worker| worker.status == ConnectorWorkerRuntimeState::Stopped)
            .count(),
        skipped: workers
            .iter()
            .filter(|worker| worker.status == ConnectorWorkerRuntimeState::Skipped)
            .count(),
        invalid: workers
            .iter()
            .filter(|worker| worker.status == ConnectorWorkerRuntimeState::Invalid)
            .count(),
        errors: workers
            .iter()
            .filter(|worker| {
                matches!(
                    worker.status,
                    ConnectorWorkerRuntimeState::Degraded
                        | ConnectorWorkerRuntimeState::Reconnecting
                        | ConnectorWorkerRuntimeState::Invalid
                        | ConnectorWorkerRuntimeState::Error
                )
            })
            .count(),
    }
}
