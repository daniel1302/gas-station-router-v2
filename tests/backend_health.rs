use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU16, Ordering},
    },
    time::Duration,
};

use gas_station_router_v2::{
    backend::{health::BackgroundHealthChecker, registry::BackendRegistry},
    config::{BackendConfig, HealthChecksConfig},
};
use tokio::{
    net::TcpListener,
    sync::{Barrier, watch},
    time::timeout,
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

fn backend(name: &str, rpc: String) -> BackendConfig {
    BackendConfig {
        name: name.into(),
        rpc,
        token: String::new(),
        enabled: true,
    }
}

fn eligible(registry: &BackendRegistry) -> Vec<String> {
    registry
        .candidates(&HashSet::new())
        .into_iter()
        .map(|node| node.name)
        .collect()
}

async fn changing_server(status: Arc<AtomicU16>) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(move |_: &wiremock::Request| {
            ResponseTemplate::new(status.load(Ordering::SeqCst))
        })
        .mount(&server)
        .await;
    server
}

async fn wait_until(mut ready: impl AsyncFnMut() -> bool) {
    timeout(Duration::from_secs(2), async {
        while !ready().await {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("condition did not become true");
}

#[tokio::test]
async fn thresholds_require_consecutive_outcomes_and_reset_the_opposite_counter() {
    let status = Arc::new(AtomicU16::new(503));
    let server = changing_server(status.clone()).await;
    let registry = Arc::new(BackendRegistry::new(&[backend("a", server.uri())]).unwrap());
    let checker =
        BackgroundHealthChecker::new(registry.clone(), HealthChecksConfig::default()).unwrap();
    for _ in 0..2 {
        checker.check_once().await.unwrap();
        assert_eq!(eligible(&registry), ["a"]);
    }
    // A success interrupts the failure streak.
    status.store(200, Ordering::SeqCst);
    checker.check_once().await.unwrap();
    status.store(503, Ordering::SeqCst);
    for _ in 0..2 {
        checker.check_once().await.unwrap();
        assert_eq!(eligible(&registry), ["a"]);
    }
    checker.check_once().await.unwrap();
    assert!(eligible(&registry).is_empty());

    status.store(200, Ordering::SeqCst);
    checker.check_once().await.unwrap();
    assert!(eligible(&registry).is_empty());
    // A failure interrupts the recovery streak.
    status.store(503, Ordering::SeqCst);
    checker.check_once().await.unwrap();
    status.store(200, Ordering::SeqCst);
    checker.check_once().await.unwrap();
    assert!(eligible(&registry).is_empty());
    checker.check_once().await.unwrap();
    assert_eq!(eligible(&registry), ["a"]);
}

#[tokio::test]
async fn reservation_health_changes_reset_probe_streaks() {
    let server = changing_server(Arc::new(AtomicU16::new(200))).await;
    let registry = Arc::new(BackendRegistry::new(&[backend("a", server.uri())]).unwrap());
    let checker =
        BackgroundHealthChecker::new(registry.clone(), HealthChecksConfig::default()).unwrap();
    for _ in 0..3 {
        checker.check_once().await.unwrap();
    }
    registry.set_healthy("a", false);
    checker.check_once().await.unwrap();
    assert!(eligible(&registry).is_empty());
    // Another reservation connection failure resets even a partially recovered node.
    registry.set_healthy("a", false);
    checker.check_once().await.unwrap();
    assert!(eligible(&registry).is_empty());
    checker.check_once().await.unwrap();
    assert_eq!(eligible(&registry), ["a"]);
}

#[tokio::test]
async fn all_backends_are_probed_in_parallel() {
    let barrier = Arc::new(Barrier::new(3));
    let mut servers = Vec::new();
    let mut backends = Vec::new();
    for name in ["a", "b", "c"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        backends.push(backend(
            name,
            format!("http://{}", listener.local_addr().unwrap()),
        ));
        let barrier = barrier.clone();
        let app = axum::Router::new().route(
            "/",
            axum::routing::get(move || async move {
                // None of the requests can complete before all three arrive.
                barrier.wait().await;
                axum::http::StatusCode::OK
            }),
        );
        servers.push(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap()
        }));
    }
    let registry = Arc::new(BackendRegistry::new(&backends).unwrap());
    let checker = BackgroundHealthChecker::new(
        registry.clone(),
        HealthChecksConfig {
            timeout: Duration::from_millis(500),
            unhealthy_threshold: 1,
            ..HealthChecksConfig::default()
        },
    )
    .unwrap();
    timeout(Duration::from_secs(2), checker.check_once())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(eligible(&registry), ["a", "b", "c"]);
    for server in servers {
        server.abort();
    }
}

#[tokio::test]
async fn a_slow_backend_does_not_delay_other_backends_next_probes() {
    let slow = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(60)))
        .mount(&slow)
        .await;
    let fast = changing_server(Arc::new(AtomicU16::new(503))).await;
    let registry = Arc::new(
        BackendRegistry::new(&[backend("slow", slow.uri()), backend("fast", fast.uri())]).unwrap(),
    );
    let checker = BackgroundHealthChecker::new(
        registry.clone(),
        HealthChecksConfig {
            timeout: Duration::from_secs(30),
            interval: Duration::from_millis(20),
            unhealthy_threshold: 3,
            ..HealthChecksConfig::default()
        },
    )
    .unwrap();
    let (shutdown, receiver) = watch::channel(());
    let task = tokio::spawn(checker.run(receiver));
    wait_until(async || eligible(&registry) == ["slow"]).await;
    assert!(fast.received_requests().await.unwrap().len() >= 3);
    assert_eq!(slow.received_requests().await.unwrap().len(), 1);
    assert!(!task.is_finished());
    shutdown.send(()).unwrap();
    timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn probes_use_backend_auth_skip_disabled_nodes_and_require_http_200() {
    let ok = MockServer::start().await;
    let no_content = MockServer::start().await;
    let redirect = MockServer::start().await;
    let disabled = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .and(header("authorization", "Bearer node-token"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&ok)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&no_content)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(302).insert_header("location", ok.uri()))
        .expect(1)
        .mount(&redirect)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&disabled)
        .await;
    let mut authenticated = backend("ok", ok.uri());
    authenticated.token = "node-token".into();
    let mut excluded = backend("disabled", disabled.uri());
    excluded.enabled = false;
    let registry = Arc::new(
        BackendRegistry::new(&[
            authenticated,
            backend("204", no_content.uri()),
            backend("redirect", redirect.uri()),
            excluded,
        ])
        .unwrap(),
    );
    BackgroundHealthChecker::new(
        registry.clone(),
        HealthChecksConfig {
            unhealthy_threshold: 1,
            ..HealthChecksConfig::default()
        },
    )
    .unwrap()
    .check_once()
    .await
    .unwrap();
    assert_eq!(eligible(&registry), ["ok"]);
    assert_eq!(registry.candidates(&HashSet::new())[0].inflight, 0);
}

#[tokio::test]
async fn timeouts_and_connection_errors_count_as_failures() {
    let slow = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(1)))
        .expect(1)
        .mount(&slow)
        .await;
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unavailable = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let registry = Arc::new(
        BackendRegistry::new(&[
            backend("timeout", slow.uri()),
            backend("connection", unavailable),
        ])
        .unwrap(),
    );
    let checker = BackgroundHealthChecker::new(
        registry.clone(),
        HealthChecksConfig {
            timeout: Duration::from_millis(50),
            unhealthy_threshold: 1,
            ..HealthChecksConfig::default()
        },
    )
    .unwrap();
    timeout(Duration::from_secs(1), checker.check_once())
        .await
        .unwrap()
        .unwrap();
    assert!(eligible(&registry).is_empty());
}

#[tokio::test]
async fn disabling_checks_sends_no_probes_and_preserves_health() {
    let server = changing_server(Arc::new(AtomicU16::new(503))).await;
    let registry = Arc::new(BackendRegistry::new(&[backend("a", server.uri())]).unwrap());
    let checker = BackgroundHealthChecker::new(
        registry.clone(),
        HealthChecksConfig {
            enabled: false,
            interval: Duration::ZERO,
            timeout: Duration::ZERO,
            unhealthy_threshold: 0,
            healthy_threshold: 0,
            ..HealthChecksConfig::default()
        },
    )
    .unwrap();
    checker.check_once().await.unwrap();
    let (_shutdown, receiver) = watch::channel(());
    checker.run(receiver).await.unwrap();
    assert_eq!(eligible(&registry), ["a"]);
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[test]
fn enabled_checks_reject_zero_timers_and_thresholds() {
    for (field, config) in [
        (
            "interval",
            HealthChecksConfig {
                interval: Duration::ZERO,
                ..HealthChecksConfig::default()
            },
        ),
        (
            "timeout",
            HealthChecksConfig {
                timeout: Duration::ZERO,
                ..HealthChecksConfig::default()
            },
        ),
        (
            "unhealthy_threshold",
            HealthChecksConfig {
                unhealthy_threshold: 0,
                ..HealthChecksConfig::default()
            },
        ),
        (
            "healthy_threshold",
            HealthChecksConfig {
                healthy_threshold: 0,
                ..HealthChecksConfig::default()
            },
        ),
    ] {
        let registry = Arc::new(BackendRegistry::new(&[]).unwrap());
        let error = BackgroundHealthChecker::new(registry, config)
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains(&format!("health_checks.{field}"))
        );
    }
}

#[tokio::test]
async fn background_task_uses_the_interval_and_recovers_without_reservation_traffic() {
    let status = Arc::new(AtomicU16::new(503));
    let server = changing_server(status.clone()).await;
    let registry = Arc::new(BackendRegistry::new(&[backend("a", server.uri())]).unwrap());
    let checker = BackgroundHealthChecker::new(
        registry.clone(),
        HealthChecksConfig {
            interval: Duration::from_millis(40),
            unhealthy_threshold: 2,
            ..HealthChecksConfig::default()
        },
    )
    .unwrap();
    let (shutdown, receiver) = watch::channel(());
    let start = tokio::time::Instant::now();
    let task = tokio::spawn(checker.run(receiver));
    wait_until(async || eligible(&registry).is_empty()).await;
    assert!(start.elapsed() >= Duration::from_millis(40));
    assert!(server.received_requests().await.unwrap().len() >= 2);
    status.store(200, Ordering::SeqCst);
    wait_until(async || eligible(&registry) == ["a"]).await;
    assert!(server.received_requests().await.unwrap().len() >= 4);
    shutdown.send(()).unwrap();
    timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn shutdown_cancels_pending_probes_without_counting_a_failure() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(60)))
        .expect(1)
        .mount(&server)
        .await;
    let registry = Arc::new(BackendRegistry::new(&[backend("a", server.uri())]).unwrap());
    let checker = BackgroundHealthChecker::new(
        registry.clone(),
        HealthChecksConfig {
            timeout: Duration::from_secs(30),
            unhealthy_threshold: 1,
            ..HealthChecksConfig::default()
        },
    )
    .unwrap();
    let (shutdown, receiver) = watch::channel(());
    let task = tokio::spawn(checker.run(receiver));
    wait_until(async || !server.received_requests().await.unwrap().is_empty()).await;
    shutdown.send(()).unwrap();
    timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(eligible(&registry), ["a"]);
}
