use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};

use crate::services::execution::ExecutionService;
use crate::services::reservation::ReservationService;

mod execution;
mod health;
mod reservation;

struct RpcState {
    reservation: Arc<ReservationService>,
    execution: Arc<ExecutionService>,
    token: String,
}

pub fn router(
    reservation: Arc<ReservationService>,
    execution: Arc<ExecutionService>,
    token: String,
) -> Router {
    Router::new()
        .route("/health", get(health::health))
        .route("/v1/reserve_gas", post(reservation::reserve_gas))
        .route("/v1/execute_tx", post(execution::execute_transaction))
        .route(
            "/v1/execute_transaction",
            post(execution::execute_transaction),
        )
        .with_state(Arc::new(RpcState {
            reservation,
            execution,
            token,
        }))
}
