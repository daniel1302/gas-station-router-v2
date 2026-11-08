mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use gas_station_router_v2::{
    api,
    backend::{health::BackgroundHealthChecker, registry::BackendRegistry},
    config::{
        BackendConfig, HealthChecksConfig, PassiveHealthCheckConfig, ReserveGasConfig, RetryConfig,
    },
    metrics,
    routing::round_robin::RoundRobinSelector,
    services::reservation::{ReservationService, http_client},
    storage::memory::MemoryStorage,
};
use serde_json::{Value, json};
use tokio::{
    sync::Notify,
    time::{advance, pause, resume, timeout},
};
use tower::ServiceExt;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

fn backend(name: &str, url: &str) -> BackendConfig {
    BackendConfig {
        name: name.into(),
        rpc: url.into(),
        token: "PRIVATE_BACKEND_TOKEN".into(),
        enabled: true,
    }
}

fn setup(
    backends: &[BackendConfig],
    passive: PassiveHealthCheckConfig,
    token: &str,
) -> (Router, Arc<BackendRegistry>) {
    let registry = Arc::new(BackendRegistry::with_passive_health(backends, passive).unwrap());
    let store = Arc::new(MemoryStorage::new());
    let reservation = Arc::new(
        ReservationService::new(
            registry.clone(),
            Arc::new(RoundRobinSelector::new()),
            store.clone(),
            http_client(Duration::from_secs(1)).unwrap(),
            ReserveGasConfig {
                timeout: Duration::from_secs(1),
                retries: RetryConfig {
                    max_retries: 0,
                    backoff: Duration::ZERO,
                },
            },
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    (
        api::router(
            reservation,
            common::execution(registry.clone(), store),
            token.into(),
        ),
        registry,
    )
}

async fn get(router: &Router, authorization: Option<&str>) -> (StatusCode, Value) {
    let mut request = Request::get("/status");
    if let Some(authorization) = authorization {
        request = request.header("authorization", authorization);
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.headers()["content-type"], "application/json");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn status(router: &Router) -> Value {
    let (code, body) = get(router, None).await;
    assert_eq!(code, StatusCode::OK);
    body
}

fn reservation_request() -> Request<Body> {
    Request::post("/v1/reserve_gas")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"gas_budget":1,"reserve_duration_secs":5}).to_string(),
        ))
        .unwrap()
}

async fn reserve_failure(router: &Router) {
    let response = router.clone().oneshot(reservation_request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

async fn no_coins(server: &MockServer, calls: u64) {
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(
            json!({"result":null,"error":"Unable to reserve gas coins for the given budget."}),
        ))
        .expect(calls)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(server)
        .await;
}

#[tokio::test]
async fn rpc_status_is_public_even_when_rpc_authentication_is_configured() {
    let (router, _) = setup(
        &[],
        PassiveHealthCheckConfig::default(),
        "PRIVATE_ROUTER_TOKEN",
    );
    for authorization in [
        None,
        Some("Bearer wrong"),
        Some("Basic PRIVATE_ROUTER_TOKEN"),
        Some("Bearer PRIVATE_ROUTER_TOKEN"),
    ] {
        let (code, body) = get(&router, authorization).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(
            body,
            json!({"summary":{
            "total":0,"enabled":0,"disabled":0,"healthy":0,"degraded":0,"unhealthy":0,
            "eligible_for_reservations":0,"inflight_reservations":0
        },"gas_stations":[]})
        );
        assert!(!body.to_string().contains("PRIVATE_ROUTER_TOKEN"));
    }
    for path in ["/v1/reserve_gas", "/v1/execute_tx"] {
        let request = Request::post(path)
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        assert_eq!(
            router.clone().oneshot(request).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        router
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        metrics::router()
            .oneshot(Request::get("/status").body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let (public, _) = setup(&[], PassiveHealthCheckConfig::default(), "");
    assert_eq!(get(&public, None).await.0, StatusCode::OK);
}

#[tokio::test]
async fn reports_all_configured_stations_without_probes_or_private_configuration() {
    let server = MockServer::start().await;
    no_coins(&server, 1).await;
    let private_url = format!("{}/PRIVATE_URL_PATH", server.uri());
    let mut disabled = backend("disabled", &private_url);
    disabled.enabled = false;
    let (router, registry) = setup(
        &[
            backend("empty", &server.uri()),
            backend("available", &private_url),
            backend("unreachable", &private_url),
            disabled,
        ],
        PassiveHealthCheckConfig::default(),
        "PRIVATE_ROUTER_TOKEN",
    );
    let mut request = reservation_request();
    request.headers_mut().insert(
        "authorization",
        "Bearer PRIVATE_ROUTER_TOKEN".parse().unwrap(),
    );
    assert_eq!(
        router.clone().oneshot(request).await.unwrap().status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    registry.set_healthy("unreachable", false);
    pause();
    let body = status(&router).await;
    assert_eq!(
        body["summary"],
        json!({
            "total":4,"enabled":3,"disabled":1,"healthy":2,"degraded":1,"unhealthy":1,
            "eligible_for_reservations":1,"inflight_reservations":0
        })
    );
    let stations = body["gas_stations"].as_array().unwrap();
    assert_eq!(
        stations
            .iter()
            .map(|station| station["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["empty", "available", "unreachable", "disabled"]
    );
    assert_eq!(stations[0]["health"], "degraded");
    assert_eq!(stations[0]["cooldown"]["active"], true);
    assert_eq!(stations[0]["cooldown"]["duration_ms"], 15_000);
    let remaining = stations[0]["cooldown"]["remaining_ms"].as_u64().unwrap();
    assert!(remaining > 0 && remaining <= 15_000);
    assert_eq!(stations[0]["cooldown"]["next_duration_ms"], 30_000);
    assert_eq!(stations[1]["eligible_for_reservations"], true);
    assert!(stations[1]["cooldown"].is_null());
    for index in [0, 2, 3] {
        assert_eq!(stations[index]["eligible_for_reservations"], false);
    }
    let text = body.to_string();
    for private in [
        "PRIVATE_ROUTER_TOKEN",
        "PRIVATE_BACKEND_TOKEN",
        "PRIVATE_URL_PATH",
        &server.uri(),
    ] {
        assert!(!text.contains(private));
    }
    resume();
}

#[tokio::test]
async fn cooldown_expiry_backoff_and_unhealthy_precedence_are_visible_without_mutating_state() {
    let server = MockServer::start().await;
    no_coins(&server, 3).await;
    let (router, registry) = setup(
        &[backend("a", &server.uri())],
        PassiveHealthCheckConfig {
            max_cooldown: Duration::from_secs(30),
            ..PassiveHealthCheckConfig::default()
        },
        "",
    );
    for (index, duration) in [15, 30, 30].into_iter().enumerate() {
        reserve_failure(&router).await;
        pause();
        let before = status(&router).await;
        assert_eq!(
            before["gas_stations"][0]["cooldown"]["duration_ms"],
            duration * 1000
        );
        assert_eq!(
            before["gas_stations"][0]["cooldown"]["next_duration_ms"],
            30_000
        );
        if index == 0 {
            registry.set_healthy("a", false);
        }
        advance(Duration::from_secs(duration)).await;
        let after = status(&router).await;
        let station = &after["gas_stations"][0];
        assert_eq!(station["cooldown"]["active"], false);
        assert_eq!(station["cooldown"]["remaining_ms"], 0);
        assert_eq!(station["cooldown"]["duration_ms"], duration * 1000);
        assert_eq!(
            station["health"],
            if index == 0 { "unhealthy" } else { "healthy" }
        );
        assert_eq!(station["eligible_for_reservations"], index != 0);
        assert_eq!(
            after,
            status(&router).await,
            "Reading status must not reset cooldown backoff or counters"
        );
        registry.set_healthy("a", true);
        resume();
    }
}

#[tokio::test]
async fn active_and_passive_failure_counters_follow_real_outcomes() {
    let server = MockServer::start().await;
    let recovering = Arc::new(AtomicBool::new(false));
    let response = recovering.clone();
    Mock::given(method("GET"))
        .respond_with(move |_: &wiremock::Request| {
            ResponseTemplate::new(if response.load(Ordering::Relaxed) {
                200
            } else {
                503
            })
        })
        .expect(5)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(500)
                .set_body_json(json!({"result":null,"error":"unknown backend failure"})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let (router, registry) = setup(
        &[backend("a", &server.uri())],
        PassiveHealthCheckConfig::default(),
        "",
    );
    reserve_failure(&router).await;
    let checker = BackgroundHealthChecker::new(registry, HealthChecksConfig::default()).unwrap();
    for _ in 0..3 {
        checker.check_once().await.unwrap();
    }
    let failed = status(&router).await;
    assert_eq!(failed["gas_stations"][0]["health"], "unhealthy");
    assert_eq!(failed["gas_stations"][0]["active_failure_count"], 3);
    assert_eq!(failed["gas_stations"][0]["reservation_failure_count"], 1);
    recovering.store(true, Ordering::Relaxed);
    checker.check_once().await.unwrap();
    let recovering = status(&router).await;
    assert_eq!(recovering["gas_stations"][0]["health"], "unhealthy");
    assert_eq!(recovering["gas_stations"][0]["active_success_count"], 1);
    checker.check_once().await.unwrap();
    let healthy = status(&router).await;
    assert_eq!(healthy["gas_stations"][0]["health"], "healthy");
    assert_eq!(healthy["gas_stations"][0]["active_success_count"], 2);
    assert_eq!(healthy["gas_stations"][0]["active_failure_count"], 0);
    assert_eq!(healthy["gas_stations"][0]["reservation_failure_count"], 1);
}

#[tokio::test]
async fn inflight_reservations_are_visible_and_released_after_the_response() {
    let server = MockServer::start().await;
    let received = Arc::new(Notify::new());
    let signal = received.clone();
    Mock::given(method("POST"))
        .respond_with(move |_: &wiremock::Request| {
            signal.notify_one();
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(150))
                .set_body_json(json!({
                    "result":{"sponsor_address":"0x1","reservation_id":1,"gas_coins":[{
                        "objectId":"0x2","version":1,"digest":"11111111111111111111111111111111"
                    }]},"error":null
                }))
        })
        .expect(1)
        .mount(&server)
        .await;
    let (router, _) = setup(
        &[backend("a", &server.uri())],
        PassiveHealthCheckConfig::default(),
        "",
    );
    let request = tokio::spawn(router.clone().oneshot(reservation_request()));
    timeout(Duration::from_secs(1), received.notified())
        .await
        .unwrap();
    let busy = status(&router).await;
    assert_eq!(busy["summary"]["inflight_reservations"], 1);
    assert_eq!(busy["gas_stations"][0]["inflight_reservations"], 1);
    assert_eq!(request.await.unwrap().unwrap().status(), StatusCode::OK);
    assert_eq!(status(&router).await["summary"]["inflight_reservations"], 0);
}
