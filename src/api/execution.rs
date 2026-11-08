use std::sync::Arc;

use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
};

use super::RpcState;
use crate::services::execution::{ExecuteTxRequest, ExecutionReply};

pub(super) async fn execute_transaction(
    State(state): State<Arc<RpcState>>,
    headers: HeaderMap,
    request: Result<Json<ExecuteTxRequest>, JsonRejection>,
) -> Response {
    let reply = if !state.token.is_empty()
        && headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            != Some(state.token.as_str())
    {
        ExecutionReply::error(
            StatusCode::UNAUTHORIZED,
            "invalid or missing authorization token",
        )
    } else if let Ok(Json(request)) = request {
        state.execution.execute(request).await
    } else {
        ExecutionReply::error(StatusCode::BAD_REQUEST, "invalid execute_tx request")
    };
    (reply.status, Json(reply.body)).into_response()
}
