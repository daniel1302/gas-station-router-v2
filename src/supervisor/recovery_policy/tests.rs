use super::*;

fn policy(automatic: bool) -> RecoveryPolicy {
    let mut config = RecoveryConfig {
        refresh_url: Some("http://localhost/refresh".into()),
        ..Default::default()
    };
    config.auto_full_rescan = automatic;
    config.require_manual_approval = !automatic;
    RecoveryPolicy::new(
        config,
        Duration::from_secs(5),
        Duration::from_secs(30),
        false,
    )
}

fn sample(divergence: f64) -> PoolSnapshot {
    PoolSnapshot {
        balance_divergence: Some(divergence),
        coin_count_divergence: Some(divergence),
        active_reservations: Some(0.0),
        pending_executions: Some(0.0),
        chain_verified: true,
        ..Default::default()
    }
}

fn divergent(policy: &mut RecoveryPolicy) {
    for _ in 0..3 {
        policy.observe(sample(50.0));
    }
}

async fn after_refresh(policy: &mut RecoveryPolicy) {
    assert!(matches!(policy.decide(false), Some(Action::Refresh)));
    policy.refreshed();
    tokio::time::advance(Duration::from_secs(60)).await;
    divergent(policy);
}

#[tokio::test(start_paused = true)]
async fn tries_refresh_before_rescan_and_requires_approval_by_default() {
    let mut policy = policy(false);
    divergent(&mut policy);
    after_refresh(&mut policy).await;
    assert!(policy.decide(false).is_none());
    assert!(policy.pending.is_some());
    assert!(matches!(
        policy.decide(true),
        Some(Action::FullRescan {
            ignore_locks: false
        })
    ));
    policy.rescanned();
    assert!(policy.pending.is_none());
    assert!(policy.decide(false).is_none());
}

#[tokio::test(start_paused = true)]
async fn automatic_rescan_needs_fresh_network_and_explicit_idle_metrics() {
    let mut policy = policy(true);
    divergent(&mut policy);
    after_refresh(&mut policy).await;
    policy.observe(PoolSnapshot::default());
    assert!(policy.decide(false).is_none());
    divergent(&mut policy);
    tokio::time::advance(Duration::from_secs(31)).await;
    assert!(policy.decide(false).is_none());
    divergent(&mut policy);
    assert!(matches!(
        policy.decide(false),
        Some(Action::FullRescan {
            ignore_locks: false
        })
    ));
}

#[tokio::test(start_paused = true)]
async fn network_change_and_inflight_work_block_even_manual_approval() {
    let mut policy = policy(true);
    policy.signal(Signal::ReinitializationRequired);
    policy.observe(PoolSnapshot {
        network_mismatch: true,
        ..sample(50.0)
    });
    policy.missing_sample();
    assert!(policy.decide(true).is_none());
    policy.observe(PoolSnapshot {
        active_reservations: Some(1.0),
        ..sample(50.0)
    });
    policy.missing_sample();
    assert!(policy.decide(true).is_none());
    policy.observe(sample(50.0));
    assert!(matches!(
        policy.decide(true),
        Some(Action::FullRescan { .. })
    ));
}

#[tokio::test(start_paused = true)]
async fn unreleased_coins_never_request_a_rebuild() {
    let mut policy = policy(true);
    for _ in 0..10 {
        policy.observe(PoolSnapshot {
            unreleased_growth: true,
            ..Default::default()
        });
    }
    assert!(policy.decide(false).is_none());
    assert!(policy.pending.is_none());
}

#[tokio::test(start_paused = true)]
async fn failed_refresh_is_rate_limited_and_does_not_escalate() {
    let mut policy = policy(true);
    divergent(&mut policy);
    assert!(matches!(policy.decide(false), Some(Action::Refresh)));
    policy.refresh_failed();
    assert!(policy.decide(false).is_none());
    assert_eq!(policy.refreshes, 0);
    tokio::time::advance(Duration::from_secs(60)).await;
    divergent(&mut policy);
    assert!(matches!(policy.decide(false), Some(Action::Refresh)));
}

#[tokio::test(start_paused = true)]
async fn locks_are_respected_until_explicit_escalation_and_rescan_budget_is_bounded() {
    let mut policy = policy(true);
    policy.config.aggression.ignore_locks_after = 2;
    policy.config.aggression.max_full_rescans = 1;
    policy.signal(Signal::ReinitializationRequired);
    policy.signal(Signal::LockBusy);
    assert!(matches!(
        policy.decide(true),
        Some(Action::FullRescan {
            ignore_locks: false
        })
    ));
    policy.signal(Signal::LockBusy);
    assert!(matches!(
        policy.decide(true),
        Some(Action::FullRescan { ignore_locks: true })
    ));
    policy.rescanned();
    tokio::time::advance(Duration::from_secs(60)).await;
    policy.signal(Signal::ReinitializationRequired);
    assert!(policy.decide(true).is_none());
}

#[tokio::test(start_paused = true)]
async fn successful_reconciliation_resets_the_refresh_budget_for_the_next_incident() {
    let mut policy = policy(true);
    divergent(&mut policy);
    after_refresh(&mut policy).await;
    for _ in 0..3 {
        policy.observe(sample(0.0));
    }
    divergent(&mut policy);
    assert!(matches!(policy.decide(false), Some(Action::Refresh)));
}

#[tokio::test(start_paused = true)]
async fn native_refresh_waits_for_the_actual_station_refresh_interval() {
    let mut policy = policy(false);
    policy.refresh_wait = Duration::from_secs(86460);
    divergent(&mut policy);
    assert!(matches!(policy.decide(false), Some(Action::Refresh)));
    policy.refreshed();
    tokio::time::advance(Duration::from_secs(60)).await;
    divergent(&mut policy);
    assert!(policy.decide(true).is_none());
}

#[tokio::test(start_paused = true)]
async fn a_new_incident_starts_with_locks_after_confirmed_recovery() {
    let mut policy = policy(true);
    policy.config.aggression.ignore_locks_after = 1;
    policy.signal(Signal::ReinitializationRequired);
    policy.signal(Signal::LockBusy);
    assert!(matches!(
        policy.decide(true),
        Some(Action::FullRescan { ignore_locks: true })
    ));
    policy.rescanned();
    tokio::time::advance(Duration::from_secs(60)).await;
    for _ in 0..3 {
        policy.observe(sample(0.0));
    }
    policy.signal(Signal::ReinitializationRequired);
    assert!(matches!(
        policy.decide(true),
        Some(Action::FullRescan {
            ignore_locks: false
        })
    ));
}
