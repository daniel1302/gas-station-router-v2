use axum::{Json, http::StatusCode};
use serde_json::{Value, json};

pub(super) async fn execute_transaction() -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({ "error": "not implemented" })),
    )
}
