use std::sync::Arc;

use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
};

use super::RpcState;
use crate::{
    metrics,
    services::reservation::{ReserveGasRequest, ReserveReply},
};

pub(super) async fn reserve_gas(
    State(state): State<Arc<RpcState>>,
    headers: HeaderMap,
    request: Result<Json<ReserveGasRequest>, JsonRejection>,
) -> Response {
    metrics::inc_reserve_gas();
    if !state.token.is_empty()
        && headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            != Some(state.token.as_str())
    {
        return reserve_response(ReserveReply::error(
            StatusCode::UNAUTHORIZED,
            "invalid or missing authorization token",
        ));
    }
    let Ok(Json(request)) = request else {
        return reserve_response(ReserveReply::error(
            StatusCode::BAD_REQUEST,
            "invalid reserve_gas request",
        ));
    };
    reserve_response(state.reservation.reserve_with_retry(&request).await)
}

fn reserve_response(reply: ReserveReply) -> Response {
    if !reply.status.is_success()
        || reply
            .body
            .get("error")
            .is_some_and(|error| !error.is_null())
    {
        metrics::inc_reserve_gas_error();
    }
    (reply.status, Json(reply.body)).into_response()
}
