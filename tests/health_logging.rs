use std::{
    collections::{HashSet, VecDeque},
    io::Write,
    sync::{Arc, Mutex},
    time::Duration,
};

use gas_station_router_v2::{
    backend::{
        health::{BackendHealth, BackgroundHealthChecker},
        registry::BackendRegistry,
    },
    config::{
        BackendConfig, HealthChecksConfig, PassiveHealthCheckConfig, ReserveGasConfig, RetryConfig,
    },
    routing::round_robin::RoundRobinSelector,
    services::reservation::{ReservationService, ReserveGasRequest, http_client},
    storage::memory::MemoryStorage,
};
use serde_json::json;
use tokio::net::TcpListener;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

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

fn registry(name: &str, url: String, passive: PassiveHealthCheckConfig) -> Arc<BackendRegistry> {
    Arc::new(
        BackendRegistry::with_passive_health(
            &[BackendConfig {
                name: name.into(),
                rpc: url,
                token: "PRIVATE_BACKEND_TOKEN".into(),
                enabled: true,
            }],
            passive,
        )
        .unwrap(),
    )
}

fn service(registry: Arc<BackendRegistry>, timeout: Duration) -> ReservationService {
    ReservationService::new(
        registry,
        Arc::new(RoundRobinSelector::new()),
        Arc::new(MemoryStorage::new()),
        http_client(timeout).unwrap(),
        ReserveGasConfig {
            timeout,
            retries: RetryConfig {
                max_retries: 0,
                backoff: Duration::ZERO,
            },
        },
        Duration::from_secs(60),
    )
    .unwrap()
}

async fn reserve(service: &ReservationService) {
    service
        .reserve_with_retry(&ReserveGasRequest {
            gas_budget: 1,
            reserve_duration_secs: 5,
        })
        .await;
}

fn field(line: &str, key: &str, value: &str) -> bool {
    line.contains(&format!("{key}={value}")) || line.contains(&format!("{key}=\"{value}\""))
}

#[tokio::test]
async fn logs_explain_transitions_without_logging_unchanged_probes_or_secrets() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer = LogWriter(output.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_max_level(tracing::Level::INFO)
        .with_writer(move || writer.clone())
        .finish();
    // A global subscriber also captures the background checker's spawned workers.
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503).set_body_string("PRIVATE_RESPONSE_BODY"))
        .expect(3)
        .mount(&server)
        .await;
    let active = registry(
        "active_http",
        format!("{}/PRIVATE_URL_PATH", server.uri()),
        PassiveHealthCheckConfig::default(),
    );
    let checker =
        BackgroundHealthChecker::new(active.clone(), HealthChecksConfig::default()).unwrap();
    for _ in 0..3 {
        checker.check_once().await.unwrap();
    }
    assert_eq!(active.health("active_http"), Some(BackendHealth::Unhealthy));
    server.reset().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(3)
        .mount(&server)
        .await;
    for _ in 0..3 {
        checker.check_once().await.unwrap();
    }
    assert_eq!(active.health("active_http"), Some(BackendHealth::Healthy));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let refused = registry(
        "active_refused",
        url.clone(),
        PassiveHealthCheckConfig::default(),
    );
    let checker =
        BackgroundHealthChecker::new(refused.clone(), HealthChecksConfig::default()).unwrap();
    for _ in 0..3 {
        checker.check_once().await.unwrap();
    }
    assert_eq!(
        refused.health("active_refused"),
        Some(BackendHealth::Unhealthy)
    );
    let passive_refused = registry(
        "passive_refused",
        url,
        PassiveHealthCheckConfig {
            failure_threshold: 1,
            ..Default::default()
        },
    );
    reserve(&service(passive_refused, Duration::from_secs(1))).await;

    let slow = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(100)))
        .expect(1)
        .mount(&slow)
        .await;
    let active_timeout = registry(
        "active_timeout",
        slow.uri(),
        PassiveHealthCheckConfig::default(),
    );
    BackgroundHealthChecker::new(
        active_timeout,
        HealthChecksConfig {
            timeout: Duration::from_millis(10),
            unhealthy_threshold: 1,
            ..Default::default()
        },
    )
    .unwrap()
    .check_once()
    .await
    .unwrap();
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(100)))
        .expect(1)
        .mount(&slow)
        .await;
    let passive_timeout = registry(
        "passive_timeout",
        slow.uri(),
        PassiveHealthCheckConfig {
            failure_threshold: 1,
            ..Default::default()
        },
    );
    reserve(&service(passive_timeout, Duration::from_millis(10))).await;

    let failed = MockServer::start().await;
    let statuses = Mutex::new(VecDeque::from([500, 502, 504]));
    Mock::given(method("POST"))
        .respond_with(move |_: &wiremock::Request| {
            ResponseTemplate::new(statuses.lock().unwrap().pop_front().unwrap())
                .set_body_json(json!({"result":null,"error":"PRIVATE_RESPONSE_BODY"}))
        })
        .expect(3)
        .mount(&failed)
        .await;
    let passive = registry(
        "passive_http",
        failed.uri(),
        PassiveHealthCheckConfig::default(),
    );
    let reservation = service(passive, Duration::from_secs(1));
    for _ in 0..3 {
        reserve(&reservation).await;
    }

    let empty = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(
            json!({"result":null,"error":"Unable to reserve gas coins for the given budget."}),
        ))
        .expect(3)
        .mount(&empty)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&empty)
        .await;
    let capacity = registry(
        "capacity",
        empty.uri(),
        PassiveHealthCheckConfig {
            initial_cooldown: Duration::from_secs(2),
            backoff: 2,
            max_cooldown: Duration::from_secs(8),
            ..Default::default()
        },
    );
    let reservation = service(capacity.clone(), Duration::from_secs(1));
    for duration in [2, 4, 8] {
        reserve(&reservation).await;
        for _ in 0..3 {
            assert_eq!(capacity.health("capacity"), Some(BackendHealth::Degraded));
        }
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(duration)).await;
        if duration == 8 {
            tokio::time::resume();
            // Expiry must be attributed to the cooldown before applying HTTP 200.
            BackgroundHealthChecker::new(capacity.clone(), HealthChecksConfig::default())
                .unwrap()
                .check_once()
                .await
                .unwrap();
        } else {
            assert_eq!(capacity.candidates(&HashSet::new()).len(), 1);
            tokio::time::resume();
        }
        for _ in 0..3 {
            assert_eq!(capacity.health("capacity"), Some(BackendHealth::Healthy));
        }
    }

    let manual = registry("manual", server.uri(), PassiveHealthCheckConfig::default());
    for healthy in [false, false, true, true] {
        assert!(manual.set_healthy("manual", healthy));
    }

    let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    let transitions: Vec<_> = logs
        .lines()
        .filter(|line| line.contains("Backend health changed"))
        .collect();
    let for_backend = |name: &str| {
        transitions
            .iter()
            .copied()
            .filter(|line| field(line, "backend", name))
            .collect::<Vec<_>>()
    };
    let active = for_backend("active_http");
    assert_eq!(active.len(), 2, "{logs}");
    for (key, value) in [
        ("previous_health", "Healthy"),
        ("health", "Unhealthy"),
        ("source", "background"),
        ("reason", "http_error"),
        ("http_status", "503"),
        ("active_failure_count", "3"),
        ("threshold", "3"),
    ] {
        assert!(
            field(active[0], key, value),
            "missing {key}={value}: {}",
            active[0]
        );
    }
    for (key, value) in [
        ("previous_health", "Unhealthy"),
        ("health", "Healthy"),
        ("reason", "http_ok"),
        ("http_status", "200"),
        ("active_success_count", "2"),
        ("threshold", "2"),
    ] {
        assert!(
            field(active[1], key, value),
            "missing {key}={value}: {}",
            active[1]
        );
    }
    for (name, source, reason) in [
        ("active_refused", "background", "connection_refused"),
        ("passive_refused", "passive", "connection_refused"),
        ("active_timeout", "background", "request_timeout"),
        ("passive_timeout", "passive", "request_timeout"),
    ] {
        let lines = for_backend(name);
        assert_eq!(lines.len(), 1, "{logs}");
        assert!(field(lines[0], "source", source));
        assert!(field(lines[0], "reason", reason));
    }
    let passive = for_backend("passive_http");
    assert_eq!(passive.len(), 1);
    for (key, value) in [
        ("source", "passive"),
        ("reason", "http_error"),
        ("http_status", "504"),
        ("reservation_failure_count", "3"),
        ("threshold", "3"),
    ] {
        assert!(
            field(passive[0], key, value),
            "missing {key}={value}: {}",
            passive[0]
        );
    }
    let capacity = for_backend("capacity");
    assert_eq!(capacity.len(), 6, "{logs}");
    for (index, milliseconds) in [(0, "2000"), (2, "4000"), (4, "8000")] {
        assert!(field(capacity[index], "reason", "no_gas_coins"));
        assert!(field(capacity[index], "health", "Degraded"));
        assert!(field(capacity[index], "cooldown_ms", milliseconds));
        assert!(field(capacity[index + 1], "source", "cooldown"));
        assert!(field(capacity[index + 1], "reason", "cooldown_expired"));
        assert!(field(capacity[index + 1], "previous_health", "Degraded"));
        assert!(field(capacity[index + 1], "health", "Healthy"));
        assert!(field(capacity[index + 1], "cooldown_remaining_ms", "0"));
    }
    let manual = for_backend("manual");
    assert_eq!(manual.len(), 2);
    assert!(manual.iter().all(|line|field(line,"source","manual") && field(line,"reason","manual_override")));
    for private in [
        "PRIVATE_BACKEND_TOKEN",
        "PRIVATE_RESPONSE_BODY",
        "PRIVATE_URL_PATH",
    ] {
        assert!(!logs.contains(private));
    }
}
