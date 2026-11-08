use std::sync::Arc;

use axum::{Json, extract::State, response::IntoResponse};

use super::RpcState;

pub(super) async fn status(State(state): State<Arc<RpcState>>) -> impl IntoResponse {
    Json(state.registry.status())
}
