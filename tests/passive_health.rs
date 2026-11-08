use std::{
    collections::{HashSet, VecDeque},
    io::Write,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use gas_station_router_v2::{
    api,
    backend::{
        health::{BackendHealth, BackgroundHealthChecker},
        registry::BackendRegistry,
    },
    config::{
        BackendConfig, HealthChecksConfig, PassiveHealthCheckConfig, ReserveGasConfig, RetryConfig,
    },
    metrics,
    routing::round_robin::RoundRobinSelector,
    services::reservation::{ReservationService, http_client},
    storage::{Storage, memory::MemoryStorage},
};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tower::ServiceExt;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, method, path},
};

const NO_COINS: &str = "Unable to reserve gas coins for the given budget.";

mod common;

fn backend(name: &str, rpc: String) -> BackendConfig {
    BackendConfig {
        name: name.into(),
        rpc,
        token: String::new(),
        enabled: true,
    }
}

fn success(id: u64) -> Value {
    json!({"result": {"sponsor_address": "0x1", "reservation_id": id,
        "gas_coins": [{"objectId": "0x2", "version": 1, "digest": "11111111111111111111111111111111"}]},
        "error": null})
}

fn rejected(error: &str) -> Value {
    json!({"result": null, "error": error})
}

fn setup(
    backends: &[BackendConfig],
    passive: PassiveHealthCheckConfig,
    timeout: Duration,
    retries: u32,
) -> (Router, Arc<BackendRegistry>, Arc<MemoryStorage>) {
    let registry = Arc::new(BackendRegistry::with_passive_health(backends, passive).unwrap());
    let storage = Arc::new(MemoryStorage::new());
    let service = ReservationService::new(
        registry.clone(),
        Arc::new(RoundRobinSelector::new()),
        storage.clone(),
        http_client(timeout).unwrap(),
        ReserveGasConfig {
            timeout,
            retries: RetryConfig {
                max_retries: retries,
                backoff: Duration::ZERO,
            },
        },
        Duration::from_secs(900),
    )
    .unwrap();
    (
        api::router(
            Arc::new(service),
            common::execution(registry.clone(), storage.clone()),
            String::new(),
        ),
        registry,
        storage,
    )
}

async fn reserve(router: Router, budget: u64) -> (StatusCode, Value) {
    let request = Request::post("/v1/reserve_gas")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"gas_budget": budget, "reserve_duration_secs": 5}).to_string(),
        ))
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn mock(server: &MockServer, response: ResponseTemplate, calls: u64) {
    Mock::given(method("POST"))
        .and(path("/v1/reserve_gas"))
        .respond_with(response)
        .expect(calls)
        .mount(server)
        .await;
}

async fn expire_cooldown(registry: &BackendRegistry, name: &str, seconds: u64) {
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(seconds - 1)).await;
    assert_eq!(registry.health(name), Some(BackendHealth::Degraded));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(registry.health(name), Some(BackendHealth::Healthy));
    tokio::time::resume();
}

#[tokio::test]
async fn server_failures_reach_the_threshold_and_success_resets_the_counter() {
    let server = MockServer::start().await;
    let responses: Arc<Mutex<VecDeque<_>>> = Arc::new(Mutex::new(
        [500, 502, 400, 200, 503, 400, 504, 500]
            .into_iter()
            .map(|status| {
                ResponseTemplate::new(status).set_body_json(if status == 200 {
                    success(42)
                } else {
                    rejected("Redis unavailable")
                })
            })
            .collect(),
    ));
    Mock::given(method("POST"))
        .respond_with(move |_: &wiremock::Request| {
            responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra reservation")
        })
        .expect(8)
        .mount(&server)
        .await;
    let (router, registry, _) = setup(
        &[backend("a", server.uri())],
        PassiveHealthCheckConfig::default(),
        Duration::from_secs(1),
        2,
    );
    for index in 0..8 {
        let (status, _) = reserve(router.clone(), 1).await;
        assert_eq!(status.is_success(), index == 3);
        assert_eq!(
            registry.health("a"),
            Some(if index == 7 {
                BackendHealth::Unhealthy
            } else {
                BackendHealth::Healthy
            })
        );
    }
    assert_eq!(reserve(router, 1).await.0, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn refused_connections_count_toward_the_configured_threshold() {
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let (router, registry, _) = setup(
        &[backend("a", format!("http://{address}"))],
        PassiveHealthCheckConfig::default(),
        Duration::from_secs(1),
        0,
    );
    for count in 1..=3 {
        assert_eq!(reserve(router.clone(), 1).await.0, StatusCode::BAD_GATEWAY);
        assert_eq!(
            registry.health("a"),
            Some(if count == 3 {
                BackendHealth::Unhealthy
            } else {
                BackendHealth::Healthy
            })
        );
    }
    assert_eq!(reserve(router, 1).await.0, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn response_timeout_counts_as_failure_without_retrying_an_ambiguous_request() {
    let slow = MockServer::start().await;
    let fallback = MockServer::start().await;
    mock(
        &slow,
        ResponseTemplate::new(200)
            .set_body_json(success(1))
            .set_delay(Duration::from_secs(1)),
        1,
    )
    .await;
    mock(
        &fallback,
        ResponseTemplate::new(200).set_body_json(success(2)),
        0,
    )
    .await;
    let (router, registry, _) = setup(
        &[backend("a", slow.uri()), backend("b", fallback.uri())],
        PassiveHealthCheckConfig {
            failure_threshold: 1,
            ..PassiveHealthCheckConfig::default()
        },
        Duration::from_millis(50),
        2,
    );
    assert_eq!(reserve(router, 1).await.0, StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(registry.health("a"), Some(BackendHealth::Unhealthy));
    assert_eq!(registry.health("b"), Some(BackendHealth::Healthy));
}

#[tokio::test]
async fn malformed_5xx_bodies_still_count_once_and_do_not_trigger_retries() {
    for status in [500, 502, 503, 504] {
        let bad = MockServer::start().await;
        let fallback = MockServer::start().await;
        mock(
            &bad,
            ResponseTemplate::new(status).set_body_string("not JSON"),
            3,
        )
        .await;
        mock(
            &fallback,
            ResponseTemplate::new(200).set_body_json(success(2)),
            0,
        )
        .await;
        let (router, registry, _) = setup(
            &[backend("a", bad.uri()), backend("b", fallback.uri())],
            PassiveHealthCheckConfig::default(),
            Duration::from_secs(1),
            2,
        );
        assert_eq!(reserve(router.clone(), 1).await.0, StatusCode::BAD_GATEWAY);
        assert_eq!(registry.health("a"), Some(BackendHealth::Healthy));
        registry.set_healthy("b", false);
        assert_eq!(reserve(router.clone(), 1).await.0, StatusCode::BAD_GATEWAY);
        assert_eq!(registry.health("a"), Some(BackendHealth::Healthy));
        assert_eq!(reserve(router, 1).await.0, StatusCode::BAD_GATEWAY);
        assert_eq!(registry.health("a"), Some(BackendHealth::Unhealthy));
    }
}

#[tokio::test]
async fn bad_requests_and_known_business_rejections_do_not_mark_backends_unhealthy() {
    for (status, error) in [
        (400, NO_COINS),
        (400, "invalid budget"),
        (429, "rate limited"),
        (500, "Gas usage exceeds daily cap"),
        (
            503,
            "Gas station is in maintenance mode. Please try again later.",
        ),
    ] {
        let server = MockServer::start().await;
        mock(
            &server,
            ResponseTemplate::new(status).set_body_json(rejected(error)),
            3,
        )
        .await;
        let (router, registry, _) = setup(
            &[backend("a", server.uri())],
            PassiveHealthCheckConfig {
                failure_threshold: 1,
                ..PassiveHealthCheckConfig::default()
            },
            Duration::from_secs(1),
            0,
        );
        for _ in 0..3 {
            assert_eq!(reserve(router.clone(), 1).await.0.as_u16(), status);
            assert_eq!(registry.health("a"), Some(BackendHealth::Healthy));
        }
    }
    let server = MockServer::start().await;
    mock(
        &server,
        ResponseTemplate::new(400).set_body_string("not JSON"),
        3,
    )
    .await;
    let (router, registry, _) = setup(
        &[backend("a", server.uri())],
        PassiveHealthCheckConfig {
            failure_threshold: 1,
            ..PassiveHealthCheckConfig::default()
        },
        Duration::from_secs(1),
        0,
    );
    for _ in 0..3 {
        reserve(router.clone(), 1).await;
    }
    assert_eq!(registry.health("a"), Some(BackendHealth::Healthy));
}

#[derive(Clone)]
struct LogWriter(Arc<Mutex<Vec<u8>>>);
impl Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn unauthorized_backends_emit_alert_logs_and_metrics_even_with_malformed_bodies() {
    let logs = Arc::new(Mutex::new(Vec::new()));
    let writer = LogWriter(logs.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_max_level(tracing::Level::ERROR)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    for status in [401, 403] {
        for malformed in [false, true] {
            let server = MockServer::start().await;
            let name = format!("auth_{status}_{malformed}");
            let response = if malformed {
                ResponseTemplate::new(status).set_body_string("PRIVATE_ERROR_BODY")
            } else {
                ResponseTemplate::new(status).set_body_json(rejected("PRIVATE_ERROR_BODY"))
            };
            mock(&server, response, 1).await;
            let mut config = backend(&name, server.uri());
            config.token = "PRIVATE_BACKEND_TOKEN".into();
            let (router, registry, _) = setup(
                &[config],
                PassiveHealthCheckConfig {
                    enabled: !malformed,
                    failure_threshold: 1,
                    ..PassiveHealthCheckConfig::default()
                },
                Duration::from_secs(1),
                2,
            );
            reserve(router, 1).await;
            assert_eq!(registry.health(&name), Some(BackendHealth::Healthy));
            let metrics = metrics::render().unwrap();
            assert!(metrics.contains(&format!("gas_station_router_backend_authorization_errors_total{{backend=\"{name}\",status=\"{status}\"}} 1")));
        }
    }
    let logs = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
    assert_eq!(logs.matches("backend_reservation_authorization").count(), 4);
    assert!(!logs.contains("PRIVATE_ERROR_BODY"));
    assert!(!logs.contains("PRIVATE_BACKEND_TOKEN"));
}

#[tokio::test]
async fn no_coins_causes_cooldown_survives_active_probes_and_routes_to_another_backend() {
    for status in [200, 500] {
        let empty = MockServer::start().await;
        let fallback = MockServer::start().await;
        mock(
            &empty,
            ResponseTemplate::new(status).set_body_json(rejected(NO_COINS)),
            1,
        )
        .await;
        mock(
            &fallback,
            ResponseTemplate::new(200).set_body_json(success(22)),
            1,
        )
        .await;
        for server in [&empty, &fallback] {
            Mock::given(method("GET"))
                .and(path("/"))
                .respond_with(ResponseTemplate::new(200))
                .expect(2)
                .mount(server)
                .await;
        }
        let (router, registry, storage) = setup(
            &[backend("a", empty.uri()), backend("b", fallback.uri())],
            PassiveHealthCheckConfig::default(),
            Duration::from_secs(1),
            2,
        );
        let (code, body) = reserve(router, 1).await;
        assert_eq!(code, StatusCode::OK);
        let id = body["result"]["reservation_id"].as_u64().unwrap();
        assert_eq!(
            storage.get_reservation(id).await.unwrap().unwrap().backend,
            "b"
        );
        assert_eq!(registry.health("a"), Some(BackendHealth::Degraded));
        assert_eq!(registry.candidates(&HashSet::new()).len(), 1);
        let checker =
            BackgroundHealthChecker::new(registry.clone(), HealthChecksConfig::default()).unwrap();
        checker.check_once().await.unwrap();
        checker.check_once().await.unwrap();
        assert_eq!(registry.health("a"), Some(BackendHealth::Degraded));
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(14)).await;
        assert_eq!(registry.health("a"), Some(BackendHealth::Degraded));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(registry.health("a"), Some(BackendHealth::Healthy));
        assert_eq!(registry.candidates(&HashSet::new()).len(), 2);
        assert!(storage.get_reservation(id).await.unwrap().is_some());
        tokio::time::resume();
    }
}

#[tokio::test]
async fn repeated_capacity_rejections_back_off_to_the_cap_despite_successful_probes() {
    for (backoff, durations) in [
        (3, [2, 6, 10, 10]),
        (1, [2, 2, 2, 2]),
        (u32::MAX, [2, 10, 10, 10]),
    ] {
        let server = MockServer::start().await;
        mock(
            &server,
            ResponseTemplate::new(500).set_body_json(rejected(NO_COINS)),
            4,
        )
        .await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200))
            .expect(8)
            .mount(&server)
            .await;
        let (router, registry, _) = setup(
            &[backend("a", server.uri())],
            PassiveHealthCheckConfig {
                initial_cooldown: Duration::from_secs(2),
                backoff,
                max_cooldown: Duration::from_secs(10),
                ..PassiveHealthCheckConfig::default()
            },
            Duration::from_secs(1),
            0,
        );
        let checker =
            BackgroundHealthChecker::new(registry.clone(), HealthChecksConfig::default()).unwrap();
        for seconds in durations {
            assert_eq!(
                reserve(router.clone(), 1).await.0,
                StatusCode::INTERNAL_SERVER_ERROR
            );
            assert_eq!(registry.health("a"), Some(BackendHealth::Degraded));
            assert!(registry.candidates(&HashSet::new()).is_empty());
            assert_eq!(
                reserve(router.clone(), 1).await.0,
                StatusCode::SERVICE_UNAVAILABLE
            );
            checker.check_once().await.unwrap();
            checker.check_once().await.unwrap();
            expire_cooldown(&registry, "a", seconds).await;
            assert_eq!(registry.candidates(&HashSet::new()).len(), 1);
        }
    }
}

#[tokio::test]
async fn only_a_successful_reservation_after_cooldown_resets_backoff() {
    let server = MockServer::start().await;
    let responses = Arc::new(Mutex::new(VecDeque::from([
        ResponseTemplate::new(500).set_body_json(rejected(NO_COINS)),
        ResponseTemplate::new(400).set_body_json(rejected("Invalid request")),
        ResponseTemplate::new(500).set_body_json(rejected(NO_COINS)),
        ResponseTemplate::new(200).set_body_json(success(4)),
        ResponseTemplate::new(500).set_body_json(rejected(NO_COINS)),
    ])));
    Mock::given(method("POST"))
        .and(path("/v1/reserve_gas"))
        .respond_with(move |_: &wiremock::Request| responses.lock().unwrap().pop_front().unwrap())
        .expect(5)
        .mount(&server)
        .await;
    let (router, registry, _) = setup(
        &[backend("a", server.uri())],
        PassiveHealthCheckConfig {
            initial_cooldown: Duration::from_secs(2),
            backoff: 3,
            max_cooldown: Duration::from_secs(10),
            ..PassiveHealthCheckConfig::default()
        },
        Duration::from_secs(1),
        0,
    );
    assert_eq!(
        reserve(router.clone(), 1).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    expire_cooldown(&registry, "a", 2).await;
    assert_eq!(reserve(router.clone(), 1).await.0, StatusCode::BAD_REQUEST);
    assert_eq!(registry.health("a"), Some(BackendHealth::Healthy));
    assert_eq!(
        reserve(router.clone(), 1).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    expire_cooldown(&registry, "a", 6).await;
    assert_eq!(reserve(router.clone(), 1).await.0, StatusCode::OK);
    assert_eq!(
        reserve(router, 1).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    expire_cooldown(&registry, "a", 2).await;
}

#[tokio::test]
async fn cooldown_backoff_is_independent_for_each_backend() {
    let a = MockServer::start().await;
    let b = MockServer::start().await;
    mock(
        &a,
        ResponseTemplate::new(500).set_body_json(rejected(NO_COINS)),
        2,
    )
    .await;
    for (budget, status, body) in [(2, 200, success(2)), (4, 500, rejected(NO_COINS))] {
        Mock::given(method("POST"))
            .and(body_json(
                json!({"gas_budget": budget, "reserve_duration_secs": 5}),
            ))
            .respond_with(ResponseTemplate::new(status).set_body_json(body))
            .expect(1)
            .mount(&b)
            .await;
    }
    let (router, registry, _) = setup(
        &[backend("a", a.uri()), backend("b", b.uri())],
        PassiveHealthCheckConfig {
            initial_cooldown: Duration::from_secs(2),
            backoff: 3,
            max_cooldown: Duration::from_secs(10),
            ..PassiveHealthCheckConfig::default()
        },
        Duration::from_secs(1),
        0,
    );
    assert_eq!(
        reserve(router.clone(), 1).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(reserve(router.clone(), 2).await.0, StatusCode::OK);
    expire_cooldown(&registry, "a", 2).await;
    assert_eq!(
        reserve(router.clone(), 3).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        reserve(router, 4).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    expire_cooldown(&registry, "b", 2).await;
    assert_eq!(registry.health("a"), Some(BackendHealth::Degraded));
    let candidates = registry.candidates(&HashSet::new());
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].name, "b");
}

#[tokio::test]
async fn an_older_inflight_success_does_not_clear_a_newer_cooldown() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_json(
            json!({"gas_budget": 1, "reserve_duration_secs": 5}),
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(success(1))
                .set_delay(Duration::from_millis(100)),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_json(
            json!({"gas_budget": 2, "reserve_duration_secs": 5}),
        ))
        .respond_with(ResponseTemplate::new(500).set_body_json(rejected(NO_COINS)))
        .expect(2)
        .mount(&server)
        .await;
    let (router, registry, _) = setup(
        &[backend("a", server.uri())],
        PassiveHealthCheckConfig::default(),
        Duration::from_secs(1),
        0,
    );
    let older = tokio::spawn(reserve(router.clone(), 1));
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.received_requests().await.unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    reserve(router.clone(), 2).await;
    assert_eq!(registry.health("a"), Some(BackendHealth::Degraded));
    assert_eq!(older.await.unwrap().0, StatusCode::OK);
    assert_eq!(registry.health("a"), Some(BackendHealth::Degraded));
    expire_cooldown(&registry, "a", 15).await;
    reserve(router, 2).await;
    expire_cooldown(&registry, "a", 30).await;
}

#[tokio::test]
async fn passive_failures_recover_through_active_checks_but_only_reservations_reset_the_counter() {
    let server = MockServer::start().await;
    mock(
        &server,
        ResponseTemplate::new(500).set_body_json(rejected("Redis unavailable")),
        3,
    )
    .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)
        .mount(&server)
        .await;
    let (router, registry, _) = setup(
        &[backend("a", server.uri())],
        PassiveHealthCheckConfig {
            failure_threshold: 2,
            ..PassiveHealthCheckConfig::default()
        },
        Duration::from_secs(1),
        0,
    );
    reserve(router.clone(), 1).await;
    reserve(router.clone(), 1).await;
    assert_eq!(registry.health("a"), Some(BackendHealth::Unhealthy));
    let checker =
        BackgroundHealthChecker::new(registry.clone(), HealthChecksConfig::default()).unwrap();
    checker.check_once().await.unwrap();
    assert_eq!(registry.health("a"), Some(BackendHealth::Unhealthy));
    checker.check_once().await.unwrap();
    assert_eq!(registry.health("a"), Some(BackendHealth::Healthy));
    reserve(router, 1).await;
    assert_eq!(registry.health("a"), Some(BackendHealth::Unhealthy));
}

#[tokio::test]
async fn disabled_passive_checks_do_not_exclude_backends() {
    for error in [NO_COINS, "Redis unavailable"] {
        let server = MockServer::start().await;
        mock(
            &server,
            ResponseTemplate::new(500).set_body_json(rejected(error)),
            4,
        )
        .await;
        let (router, registry, _) = setup(
            &[backend("a", server.uri())],
            PassiveHealthCheckConfig {
                enabled: false,
                failure_threshold: 0,
                initial_cooldown: Duration::ZERO,
                backoff: 0,
                max_cooldown: Duration::ZERO,
            },
            Duration::from_secs(1),
            0,
        );
        for _ in 0..4 {
            reserve(router.clone(), 1).await;
        }
        assert_eq!(registry.health("a"), Some(BackendHealth::Healthy));
    }
}

#[test]
fn enabled_passive_checks_reject_invalid_cooldown_settings() {
    for (field, config) in [
        (
            "failure_threshold",
            PassiveHealthCheckConfig {
                failure_threshold: 0,
                ..PassiveHealthCheckConfig::default()
            },
        ),
        (
            "initial_cooldown",
            PassiveHealthCheckConfig {
                initial_cooldown: Duration::ZERO,
                ..PassiveHealthCheckConfig::default()
            },
        ),
        (
            "backoff",
            PassiveHealthCheckConfig {
                backoff: 0,
                ..PassiveHealthCheckConfig::default()
            },
        ),
        (
            "max_cooldown",
            PassiveHealthCheckConfig {
                max_cooldown: Duration::ZERO,
                ..PassiveHealthCheckConfig::default()
            },
        ),
        (
            "max_cooldown",
            PassiveHealthCheckConfig {
                max_cooldown: Duration::from_secs(14),
                ..PassiveHealthCheckConfig::default()
            },
        ),
    ] {
        let error = BackendRegistry::with_passive_health(&[], config)
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains(&format!("health_checks.passive.{field}"))
        );
    }
}

#[tokio::test]
async fn client_authorization_failures_do_not_alert_on_the_backend() {
    let server = MockServer::start().await;
    mock(
        &server,
        ResponseTemplate::new(200).set_body_json(success(1)),
        0,
    )
    .await;
    let registry = Arc::new(BackendRegistry::new(&[backend("client_auth", server.uri())]).unwrap());
    let service = ReservationService::new(
        registry.clone(),
        Arc::new(RoundRobinSelector::new()),
        Arc::new(MemoryStorage::new()),
        http_client(Duration::from_secs(1)).unwrap(),
        ReserveGasConfig {
            timeout: Duration::from_secs(1),
            retries: RetryConfig {
                max_retries: 0,
                backoff: Duration::ZERO,
            },
        },
        Duration::from_secs(900),
    )
    .unwrap();
    let router = api::router(
        Arc::new(service),
        common::execution(registry.clone(), Arc::new(MemoryStorage::new())),
        "router-token".into(),
    );
    assert_eq!(reserve(router, 1).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(registry.health("client_auth"), Some(BackendHealth::Healthy));
    let metrics = metrics::render().unwrap();
    for status in ["401", "403"] {
        assert!(metrics.contains(&format!("gas_station_router_backend_authorization_errors_total{{backend=\"client_auth\",status=\"{status}\"}} 0")));
    }
}

#[tokio::test]
async fn only_an_explicit_negative_capacity_envelope_starts_cooldown() {
    for body in [
        rejected(&format!("{NO_COINS} unknown additional failure")),
        json!({"error": NO_COINS}),
        json!({"result": success(1)["result"], "error": NO_COINS}),
    ] {
        let server = MockServer::start().await;
        let fallback = MockServer::start().await;
        mock(&server, ResponseTemplate::new(500).set_body_json(body), 1).await;
        mock(
            &fallback,
            ResponseTemplate::new(200).set_body_json(success(2)),
            0,
        )
        .await;
        let (router, registry, _) = setup(
            &[backend("a", server.uri()), backend("b", fallback.uri())],
            PassiveHealthCheckConfig {
                failure_threshold: 1,
                ..PassiveHealthCheckConfig::default()
            },
            Duration::from_secs(1),
            2,
        );
        assert_eq!(
            reserve(router, 1).await.0,
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(registry.health("a"), Some(BackendHealth::Unhealthy));
    }
}

#[tokio::test]
async fn cooldown_expiry_does_not_readmit_a_backend_that_fails_active_checks() {
    let server = MockServer::start().await;
    mock(
        &server,
        ResponseTemplate::new(500).set_body_json(rejected(NO_COINS)),
        1,
    )
    .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .expect(3)
        .mount(&server)
        .await;
    let (router, registry, _) = setup(
        &[backend("a", server.uri())],
        PassiveHealthCheckConfig::default(),
        Duration::from_secs(1),
        0,
    );
    reserve(router, 1).await;
    assert_eq!(registry.health("a"), Some(BackendHealth::Degraded));
    let checker =
        BackgroundHealthChecker::new(registry.clone(), HealthChecksConfig::default()).unwrap();
    for _ in 0..3 {
        checker.check_once().await.unwrap();
    }
    assert_eq!(registry.health("a"), Some(BackendHealth::Unhealthy));
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(15)).await;
    assert_eq!(registry.health("a"), Some(BackendHealth::Unhealthy));
    assert!(registry.candidates(&HashSet::new()).is_empty());
}
