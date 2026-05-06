use crate::{
    ai_context::{build_ai_entity_context, AiContextQuery, AiEntityContextResponse},
    auth::{require_scope, AuthContext},
    error::ApiError,
    AppState,
};
use axum::{
    extract::{Extension, Path, Query, State},
    routing::get,
    Json, Router,
};
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/ai/context/entity/:entity_id", get(get_ai_entity_context))
}

async fn get_ai_entity_context(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(entity_id): Path<Uuid>,
    Query(query): Query<AiContextQuery>,
) -> Result<Json<AiEntityContextResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ai/context/entity/:entity_id",
        "ai:context:read",
    )?;
    Ok(Json(build_ai_entity_context(&state, entity_id, query)?))
}
