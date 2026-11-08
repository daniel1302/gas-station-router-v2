use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use gas_station_router_v2::{
    api,
    backend::{health::BackgroundHealthChecker, registry::BackendRegistry},
    config::{BackendConfig, HealthChecksConfig, ReserveGasConfig, RetryConfig},
    routing::{BackendCandidate, BackendSelector, round_robin::RoundRobinSelector},
    services::reservation::{ReservationService, ReserveError, http_client},
    storage::{
        CachedTransaction, ExecutionClaim, ExecutionRecord, ExecutionToken, Reservation,
        ReservationId, Storage, StorageError, StorageResult, TransactionWriteOutcome,
        memory::MemoryStorage,
    },
};
use iota_sdk_types::{Address, ObjectDigest, ObjectId, TransactionDigest};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tower::ServiceExt;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, header, method, path},
};

fn backend(name: &str, rpc: String) -> BackendConfig {
    BackendConfig {
        name: name.into(),
        enabled: true,
        token: String::new(),
        rpc,
    }
}

fn success(id: u64) -> Value {
    json!({
        "result": {
            "sponsor_address": Address::new([1; 32]), "reservation_id": id,
            "gas_coins": [{ "objectId": ObjectId::new([2; 32]), "version": 9, "digest": ObjectDigest::new([3; 32]) }],
            "future_field": "preserved"
        },
        "error": null, "extra": true
    })
}

fn settings(retries: u32, timeout: Duration) -> ReserveGasConfig {
    ReserveGasConfig {
        timeout,
        retries: RetryConfig {
            max_retries: retries,
            backoff: Duration::ZERO,
        },
    }
}

fn app(
    backends: Vec<BackendConfig>,
    storage: Arc<dyn Storage>,
    config: ReserveGasConfig,
    token: &str,
) -> (Router, Arc<BackendRegistry>) {
    let registry = Arc::new(BackendRegistry::new(&backends).unwrap());
    for backend in &backends {
        registry.set_healthy(&backend.name, true);
    }
    let client = http_client(config.timeout).unwrap();
    let service = ReservationService::new(
        registry.clone(),
        Arc::new(RoundRobinSelector::new()),
        storage,
        client,
        config,
        Duration::from_secs(900),
    )
    .unwrap();
    (api::router(Arc::new(service), token.into()), registry)
}

async fn send(app: Router, body: Value, token: Option<&str>) -> (StatusCode, Value) {
    let mut request = Request::post("/v1/reserve_gas").header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

fn request() -> Value {
    json!({"gas_budget": 1_000_000, "reserve_duration_secs": 60})
}

async fn mock_reservation(server: &MockServer, template: ResponseTemplate, calls: u64) {
    Mock::given(method("POST"))
        .and(path("/v1/reserve_gas"))
        .respond_with(template)
        .expect(calls)
        .mount(server)
        .await;
}

#[tokio::test]
async fn forwards_backend_token_and_rewrites_only_reservation_id_after_storing() {
    let upstream = MockServer::start().await;
    let upstream_body = success(u64::MAX);
    Mock::given(method("POST"))
        .and(path("/v1/reserve_gas"))
        .and(header("authorization", "Bearer backend-token"))
        .and(body_json(request()))
        .respond_with(ResponseTemplate::new(200).set_body_json(&upstream_body))
        .expect(1)
        .mount(&upstream)
        .await;
    let mut config = backend("a", upstream.uri());
    config.token = "backend-token".into();
    let storage = Arc::new(MemoryStorage::new());
    let (router, registry) = app(
        vec![config],
        storage.clone(),
        settings(1, Duration::from_secs(1)),
        "client-token",
    );
    let (status, mut body) = send(router, request(), Some("client-token")).await;
    assert_eq!(status, StatusCode::OK);
    let id = body["result"]["reservation_id"].as_u64().unwrap();
    assert_ne!(id, u64::MAX);
    let mapping = storage.get_reservation(id).await.unwrap().unwrap();
    assert_eq!(mapping.backend, "a");
    assert_eq!(mapping.upstream_reservation_id, u64::MAX);
    body["result"]["reservation_id"] = json!(u64::MAX);
    assert_eq!(body, upstream_body);
    assert_eq!(registry.candidates(&HashSet::new())[0].inflight, 0);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(900)).await;
    assert!(storage.get_reservation(id).await.unwrap().is_none());
}

#[tokio::test]
async fn rotates_three_backends_in_round_robin_order() {
    let a = MockServer::start().await;
    let b = MockServer::start().await;
    let c = MockServer::start().await;
    for server in [&a, &b, &c] {
        mock_reservation(
            server,
            ResponseTemplate::new(200).set_body_json(success(100)),
            2,
        )
        .await;
    }
    let storage = Arc::new(MemoryStorage::new());
    let (router, _) = app(
        vec![
            backend("a", a.uri()),
            backend("b", b.uri()),
            backend("c", c.uri()),
        ],
        storage.clone(),
        settings(1, Duration::from_secs(1)),
        "",
    );
    for name in ["a", "b", "c", "a", "b", "c"] {
        let (status, body) = send(router.clone(), request(), None).await;
        assert_eq!(status, StatusCode::OK);
        let id = body["result"]["reservation_id"].as_u64().unwrap();
        assert_eq!(
            storage.get_reservation(id).await.unwrap().unwrap().backend,
            name
        );
    }
}

#[tokio::test]
async fn skips_unhealthy_disabled_and_request_excluded_backends() {
    let a = MockServer::start().await;
    let b = MockServer::start().await;
    let c = MockServer::start().await;
    mock_reservation(&a, ResponseTemplate::new(200).set_body_json(success(1)), 1).await;
    mock_reservation(&b, ResponseTemplate::new(200).set_body_json(success(2)), 0).await;
    mock_reservation(&c, ResponseTemplate::new(200).set_body_json(success(3)), 0).await;
    let mut disabled = backend("c", c.uri());
    disabled.enabled = false;
    let (router, registry) = app(
        vec![backend("a", a.uri()), backend("b", b.uri()), disabled],
        Arc::new(MemoryStorage::new()),
        settings(1, Duration::from_secs(1)),
        "",
    );
    registry.set_healthy("b", false);
    assert!(registry.candidates(&HashSet::from(["a".into()])).is_empty());
    assert_eq!(
        send(router.clone(), request(), None).await.0,
        StatusCode::OK
    );
    registry.set_healthy("a", false);
    assert_eq!(
        send(router, request(), None).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn retries_connect_error_on_b_without_skipping_to_c() {
    let b = MockServer::start().await;
    let c = MockServer::start().await;
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = closed.local_addr().unwrap();
    drop(closed);
    mock_reservation(
        &b,
        ResponseTemplate::new(200).set_body_json(success(200)),
        1,
    )
    .await;
    mock_reservation(
        &c,
        ResponseTemplate::new(200).set_body_json(success(300)),
        0,
    )
    .await;
    let storage = Arc::new(MemoryStorage::new());
    let mut retry = settings(1, Duration::from_secs(1));
    retry.retries.backoff = Duration::from_millis(30);
    let (router, registry) = app(
        vec![
            backend("a", format!("http://{address}")),
            backend("b", b.uri()),
            backend("c", c.uri()),
        ],
        storage.clone(),
        retry,
        "",
    );
    let started = std::time::Instant::now();
    let (status, body) = send(router, request(), None).await;
    assert!(started.elapsed() >= Duration::from_millis(30));
    assert_eq!(status, StatusCode::OK);
    let mapping = storage
        .get_reservation(body["result"]["reservation_id"].as_u64().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mapping.backend, "b");
    assert_eq!(mapping.upstream_reservation_id, 200);
    assert!(
        registry
            .candidates(&HashSet::new())
            .iter()
            .any(|candidate| candidate.name == "a")
    );
}

#[tokio::test]
async fn zero_retries_prevents_failover() {
    let b = MockServer::start().await;
    mock_reservation(&b, ResponseTemplate::new(200).set_body_json(success(2)), 0).await;
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = closed.local_addr().unwrap();
    drop(closed);
    let (router, _) = app(
        vec![
            backend("a", format!("http://{address}")),
            backend("b", b.uri()),
        ],
        Arc::new(MemoryStorage::new()),
        settings(0, Duration::from_secs(1)),
        "",
    );
    assert_eq!(
        send(router, request(), None).await.0,
        StatusCode::BAD_GATEWAY
    );
}

#[tokio::test]
async fn retry_budget_stops_before_a_third_backend() {
    let c = MockServer::start().await;
    mock_reservation(&c, ResponseTemplate::new(200).set_body_json(success(3)), 0).await;
    let a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a_url = format!("http://{}", a.local_addr().unwrap());
    let b_url = format!("http://{}", b.local_addr().unwrap());
    drop((a, b));
    let (router, _) = app(
        vec![
            backend("a", a_url),
            backend("b", b_url),
            backend("c", c.uri()),
        ],
        Arc::new(MemoryStorage::new()),
        settings(1, Duration::from_secs(1)),
        "",
    );
    assert_eq!(
        send(router, request(), None).await.0,
        StatusCode::BAD_GATEWAY
    );
}

#[test]
fn reserve_error_policy_retries_only_definite_failures() {
    assert!(ReserveError::ConnectionFailed.is_retryable());
    assert!(ReserveError::ReservationRejected.is_retryable());
    assert!(!ReserveError::UnknownOutcome.is_retryable());
    assert!(!ReserveError::Permanent.is_retryable());
}

#[tokio::test]
async fn explicit_reservation_rejections_fail_over_and_store_only_the_success() {
    for (status, error) in [
        (200, "reservation rejected"),
        (429, "rate limited"),
        (500, "Unable to reserve gas coins for the given budget."),
        (
            503,
            "Gas station is in maintenance mode. Please try again later.",
        ),
        (500, "Gas usage exceeds daily cap"),
    ] {
        let a = MockServer::start().await;
        let b = MockServer::start().await;
        mock_reservation(
            &a,
            ResponseTemplate::new(status).set_body_json(json!({"result": null, "error": error})),
            1,
        )
        .await;
        mock_reservation(&b, ResponseTemplate::new(200).set_body_json(success(20)), 1).await;
        let storage = Arc::new(MemoryStorage::new());
        let (router, registry) = app(
            vec![backend("a", a.uri()), backend("b", b.uri())],
            storage.clone(),
            settings(2, Duration::from_secs(1)),
            "",
        );
        let (status, body) = send(router, request(), None).await;
        assert_eq!(status, StatusCode::OK);
        let mapping = storage
            .get_reservation(body["result"]["reservation_id"].as_u64().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mapping.backend, "b");
        assert_eq!(mapping.upstream_reservation_id, 20);
        // Capacity rejection additionally excludes the backend during cooldown.
        let expected = if error == "Unable to reserve gas coins for the given budget." {
            1
        } else {
            2
        };
        assert_eq!(registry.candidates(&HashSet::new()).len(), expected);
    }
}

#[tokio::test]
async fn two_retries_allow_connection_failure_then_rejection_then_success() {
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unavailable = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let b = MockServer::start().await;
    let c = MockServer::start().await;
    mock_reservation(
        &b,
        ResponseTemplate::new(500).set_body_json(json!({
            "result": null, "error": "Unable to reserve gas coins for the given budget."
        })),
        1,
    )
    .await;
    mock_reservation(&c, ResponseTemplate::new(200).set_body_json(success(30)), 1).await;
    let storage = Arc::new(MemoryStorage::new());
    let (router, _) = app(
        vec![
            backend("a", unavailable),
            backend("b", b.uri()),
            backend("c", c.uri()),
        ],
        storage.clone(),
        settings(2, Duration::from_secs(1)),
        "",
    );
    let (status, body) = send(router, request(), None).await;
    assert_eq!(status, StatusCode::OK);
    let mapping = storage
        .get_reservation(body["result"]["reservation_id"].as_u64().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mapping.backend, "c");
    assert_eq!(mapping.upstream_reservation_id, 30);
}

#[tokio::test]
async fn rejection_retry_budget_returns_last_rejection_without_a_fourth_attempt() {
    let a = MockServer::start().await;
    let b = MockServer::start().await;
    let c = MockServer::start().await;
    let d = MockServer::start().await;
    let body = json!({"result": null, "error": "rate limited", "extra": "preserved"});
    for server in [&a, &b, &c] {
        mock_reservation(server, ResponseTemplate::new(429).set_body_json(&body), 1).await;
    }
    mock_reservation(&d, ResponseTemplate::new(200).set_body_json(success(40)), 0).await;
    let storage = Arc::new(UnavailableStorage::default());
    let (router, _) = app(
        vec![
            backend("a", a.uri()),
            backend("b", b.uri()),
            backend("c", c.uri()),
            backend("d", d.uri()),
        ],
        storage.clone(),
        settings(2, Duration::from_secs(1)),
        "",
    );
    let (status, actual) = send(router, request(), None).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(actual, body);
    assert_eq!(storage.writes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn no_remaining_candidate_returns_last_rejection_and_exclusions_are_per_request() {
    let a = MockServer::start().await;
    let body = json!({"result": null, "error": "reservation rejected"});
    mock_reservation(&a, ResponseTemplate::new(200).set_body_json(&body), 2).await;
    let storage = Arc::new(UnavailableStorage::default());
    let mut config = settings(100, Duration::from_secs(1));
    // Exhausting candidates should return without waiting for a nonexistent retry.
    config.retries.backoff = Duration::from_secs(10);
    let (router, _) = app(vec![backend("a", a.uri())], storage.clone(), config, "");
    for _ in 0..2 {
        let (status, actual) = tokio::time::timeout(
            Duration::from_secs(1),
            send(router.clone(), request(), None),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(actual, body);
    }
    assert_eq!(storage.writes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn permanent_and_unknown_backend_errors_do_not_retry_or_store() {
    for status in [400, 401, 403, 404, 422, 500, 502, 503] {
        let a = MockServer::start().await;
        let b = MockServer::start().await;
        let body = json!({"result": null, "error": "reservation rejected"});
        mock_reservation(&a, ResponseTemplate::new(status).set_body_json(&body), 1).await;
        mock_reservation(&b, ResponseTemplate::new(200).set_body_json(success(2)), 0).await;
        let storage = Arc::new(UnavailableStorage::default());
        let (router, _) = app(
            vec![backend("a", a.uri()), backend("b", b.uri())],
            storage.clone(),
            settings(3, Duration::from_secs(1)),
            "",
        );
        let (actual_status, actual_body) = send(router, request(), None).await;
        assert_eq!(actual_status.as_u16(), status);
        assert_eq!(actual_body, body);
        assert_eq!(storage.writes.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn timeout_does_not_retry_an_ambiguous_reservation() {
    let a = MockServer::start().await;
    let b = MockServer::start().await;
    mock_reservation(
        &a,
        ResponseTemplate::new(200)
            .set_body_json(success(1))
            .set_delay(Duration::from_millis(200)),
        1,
    )
    .await;
    mock_reservation(&b, ResponseTemplate::new(200).set_body_json(success(2)), 0).await;
    let storage = Arc::new(UnavailableStorage::default());
    let (router, _) = app(
        vec![backend("a", a.uri()), backend("b", b.uri())],
        storage.clone(),
        settings(3, Duration::from_millis(50)),
        "",
    );
    assert_eq!(
        send(router, request(), None).await.0,
        StatusCode::GATEWAY_TIMEOUT
    );
    assert_eq!(storage.writes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn malformed_success_response_does_not_retry_or_store() {
    let mut empty_coins = success(1);
    empty_coins["result"]["gas_coins"] = json!([]);
    let mut conflicting_envelope = success(1);
    conflicting_envelope["error"] = json!("also an error");
    let mut invalid_sponsor = success(1);
    invalid_sponsor["result"]["sponsor_address"] = json!("invalid");
    for body in [
        json!({}),
        json!({"error": "rejected but result field is missing"}),
        json!({"result": null, "error": ""}),
        json!({"result": null, "error": "  "}),
        empty_coins,
        conflicting_envelope,
        invalid_sponsor,
    ] {
        let a = MockServer::start().await;
        let b = MockServer::start().await;
        mock_reservation(&a, ResponseTemplate::new(200).set_body_json(body), 1).await;
        mock_reservation(&b, ResponseTemplate::new(200).set_body_json(success(2)), 0).await;
        let storage = Arc::new(UnavailableStorage::default());
        let (router, _) = app(
            vec![backend("a", a.uri()), backend("b", b.uri())],
            storage.clone(),
            settings(1, Duration::from_secs(1)),
            "",
        );
        assert_eq!(
            send(router, request(), None).await.0,
            StatusCode::BAD_GATEWAY
        );
        assert_eq!(storage.writes.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn failed_storage_write_never_retries_a_successful_upstream_reservation() {
    let a = MockServer::start().await;
    let b = MockServer::start().await;
    mock_reservation(&a, ResponseTemplate::new(200).set_body_json(success(1)), 1).await;
    mock_reservation(&b, ResponseTemplate::new(200).set_body_json(success(2)), 0).await;
    let storage = Arc::new(UnavailableStorage::default());
    let (router, _) = app(
        vec![backend("a", a.uri()), backend("b", b.uri())],
        storage.clone(),
        settings(3, Duration::from_secs(1)),
        "",
    );
    let (status, body) = send(router, request(), None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["result"], Value::Null);
    assert_eq!(storage.writes.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn redirects_are_not_followed() {
    let a = MockServer::start().await;
    let b = MockServer::start().await;
    mock_reservation(
        &a,
        ResponseTemplate::new(302)
            .insert_header("location", format!("{}/v1/reserve_gas", b.uri()))
            .set_body_json(json!({"error": "redirect"})),
        1,
    )
    .await;
    Mock::given(path("/v1/reserve_gas"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&b)
        .await;
    let (router, _) = app(
        vec![backend("a", a.uri()), backend("b", b.uri())],
        Arc::new(MemoryStorage::new()),
        settings(3, Duration::from_secs(1)),
        "",
    );
    assert_eq!(
        send(router, request(), None).await.0,
        StatusCode::BAD_GATEWAY
    );
}

#[tokio::test]
async fn upstream_success_resets_passive_failures_even_if_mapping_storage_fails() {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    Mock::given(method("POST"))
        .respond_with(move |_: &wiremock::Request| {
            if calls.fetch_add(1, Ordering::SeqCst) == 2 {
                ResponseTemplate::new(200).set_body_json(success(42))
            } else {
                ResponseTemplate::new(500)
                    .set_body_json(json!({"result": null, "error": "Redis unavailable"}))
            }
        })
        .expect(4)
        .mount(&server)
        .await;
    let storage = Arc::new(UnavailableStorage::default());
    let (router, registry) = app(
        vec![backend("a", server.uri())],
        storage.clone(),
        settings(0, Duration::from_secs(1)),
        "",
    );
    for expected in [
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::INTERNAL_SERVER_ERROR,
    ] {
        assert_eq!(send(router.clone(), request(), None).await.0, expected);
        assert_eq!(registry.candidates(&HashSet::new()).len(), 1);
    }
    assert_eq!(storage.writes.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn authentication_and_request_validation_happen_before_forwarding() {
    let a = MockServer::start().await;
    mock_reservation(&a, ResponseTemplate::new(200).set_body_json(success(1)), 0).await;
    let (router, _) = app(
        vec![backend("a", a.uri())],
        Arc::new(MemoryStorage::new()),
        settings(1, Duration::from_secs(1)),
        "client-token",
    );
    for token in [None, Some("wrong-token")] {
        assert_eq!(
            send(router.clone(), request(), token).await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    for body in [
        json!({}),
        json!({"gas_budget": 0, "reserve_duration_secs": 60}),
        json!({"gas_budget": 1, "reserve_duration_secs": 0}),
        json!({"gas_budget": -1, "reserve_duration_secs": 60}),
    ] {
        assert_eq!(
            send(router.clone(), body, Some("client-token")).await.0,
            StatusCode::BAD_REQUEST
        );
    }
}

#[tokio::test]
async fn health_probes_filter_failed_backends_and_allow_recovery() {
    let a = MockServer::start().await;
    let b = MockServer::start().await;
    let c = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)
        .mount(&a)
        .await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&b)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&c)
        .await;
    let mut disabled = backend("c", c.uri());
    disabled.enabled = false;
    let registry = Arc::new(
        BackendRegistry::new(&[backend("a", a.uri()), backend("b", b.uri()), disabled]).unwrap(),
    );
    assert_eq!(registry.candidates(&HashSet::new()).len(), 2);
    let checker = BackgroundHealthChecker::new(
        registry.clone(),
        HealthChecksConfig {
            unhealthy_threshold: 1,
            healthy_threshold: 1,
            ..HealthChecksConfig::default()
        },
    )
    .unwrap();
    checker.check_once().await.unwrap();
    assert_eq!(
        registry
            .candidates(&HashSet::new())
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        ["a"]
    );
    b.verify().await;
    b.reset().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&b)
        .await;
    checker.check_once().await.unwrap();
    assert_eq!(
        registry
            .candidates(&HashSet::new())
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
}

#[test]
fn selector_returns_candidate_indexes_and_does_not_skip_filtered_peers() {
    let selector = RoundRobinSelector::new();
    let candidates = ["a", "b", "c"].map(|name| BackendCandidate {
        name: name.into(),
        weight: 1,
        inflight: 0,
    });
    assert_eq!(selector.select(&[]), None);
    assert_eq!(selector.select(&candidates), Some(0));
    assert_eq!(selector.select(&candidates[1..]), Some(0));
    assert_eq!(selector.select(&candidates), Some(2));
    assert_eq!(selector.select(&candidates), Some(0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_requests_rotate_backends_and_preserve_each_mapping() {
    let a = MockServer::start().await;
    let b = MockServer::start().await;
    let c = MockServer::start().await;
    for (server, id) in [(&a, 101), (&b, 102), (&c, 103)] {
        mock_reservation(
            server,
            ResponseTemplate::new(200).set_body_json(success(id)),
            8,
        )
        .await;
    }
    let storage = Arc::new(MemoryStorage::new());
    let (router, _) = app(
        vec![
            backend("a", a.uri()),
            backend("b", b.uri()),
            backend("c", c.uri()),
        ],
        storage.clone(),
        settings(1, Duration::from_secs(2)),
        "",
    );
    let mut tasks = Vec::new();
    for _ in 0..24 {
        let router = router.clone();
        tasks.push(tokio::spawn(
            async move { send(router, request(), None).await },
        ));
    }
    let mut ids = HashSet::new();
    for task in tasks {
        let (status, body) = task.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        let id = body["result"]["reservation_id"].as_u64().unwrap();
        assert!(ids.insert(id));
        let mapping = storage.get_reservation(id).await.unwrap().unwrap();
        assert!(matches!(
            (mapping.backend.as_str(), mapping.upstream_reservation_id),
            ("a", 101) | ("b", 102) | ("c", 103)
        ));
    }
}

#[tokio::test]
async fn inflight_tracks_active_requests_and_is_released_on_cancellation() {
    let a = MockServer::start().await;
    mock_reservation(
        &a,
        ResponseTemplate::new(200)
            .set_body_json(success(1))
            .set_delay(Duration::from_secs(2)),
        1,
    )
    .await;
    let (router, registry) = app(
        vec![backend("a", a.uri())],
        Arc::new(MemoryStorage::new()),
        settings(1, Duration::from_secs(5)),
        "",
    );
    let task = tokio::spawn(async move { send(router, request(), None).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while a.received_requests().await.unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(registry.candidates(&HashSet::new())[0].inflight, 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(registry.candidates(&HashSet::new())[0].inflight, 0);
}

#[derive(Default)]
struct UnavailableStorage {
    writes: AtomicUsize,
}

fn unavailable<T>() -> StorageResult<T> {
    Err(StorageError::Backend(
        std::io::Error::other("unavailable test store").into(),
    ))
}

#[async_trait]
impl Storage for UnavailableStorage {
    async fn create_reservation(
        &self,
        _: Reservation,
        _: Duration,
    ) -> StorageResult<ReservationId> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        unavailable()
    }
    async fn get_reservation(&self, _: ReservationId) -> StorageResult<Option<Reservation>> {
        unavailable()
    }
    async fn put_transaction(
        &self,
        _: &TransactionDigest,
        _: CachedTransaction,
        _: Duration,
    ) -> StorageResult<TransactionWriteOutcome> {
        unavailable()
    }
    async fn get_transaction(
        &self,
        _: &TransactionDigest,
    ) -> StorageResult<Option<CachedTransaction>> {
        unavailable()
    }
    async fn health_check(&self) -> StorageResult<()> {
        unavailable()
    }
    async fn try_claim_execution(
        &self,
        _: ReservationId,
        _: &TransactionDigest,
        _: ExecutionToken,
        _: Duration,
        _: Duration,
    ) -> StorageResult<ExecutionClaim> {
        unavailable()
    }
    async fn get_execution(&self, _: ReservationId) -> StorageResult<Option<ExecutionRecord>> {
        unavailable()
    }
    async fn mark_execution_dispatching(
        &self,
        _: ReservationId,
        _: ExecutionToken,
    ) -> StorageResult<()> {
        unavailable()
    }
    async fn complete_execution(
        &self,
        _: ReservationId,
        _: ExecutionToken,
        _: CachedTransaction,
        _: Duration,
    ) -> StorageResult<TransactionWriteOutcome> {
        unavailable()
    }
    async fn cancel_execution(&self, _: ReservationId, _: ExecutionToken) -> StorageResult<()> {
        unavailable()
    }
}
