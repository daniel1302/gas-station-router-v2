use std::path::Path;

use serde_json::Value;

use super::redis_audit::AuditReport;

pub(super) fn print(report: &Value, audit: &AuditReport, directory: &Path) {
    let passed = report["status"] == "passed";
    println!(
        "\nFull E2E summary: {}",
        if passed { "PASS" } else { "FAIL" }
    );
    println!(
        "Total time including startup and cleanup: {}s; TTL drain: {}s",
        report["total_duration_seconds"], audit.ttl_wait_seconds
    );
    if let Some(traffic) = report.get("traffic") {
        println!(
            "Scenario: {}s; finalized transactions: {}; cache reads: {}; sponsors: {}",
            report["duration_seconds"],
            traffic["transactions"],
            traffic["cache_reads"],
            traffic["sponsors"].as_array().map_or(0, Vec::len)
        );
    }
    if let Some(routers) = report["routers"].as_array() {
        for (index, router) in routers.iter().enumerate() {
            println!(
                "Router {}: reservations={}, executions={} (including cache), dispatches={}, Redis errors={}, healthy backends={}/3",
                index + 1,
                router["reservations"],
                router["executions"],
                router["dispatches"],
                router["storage_errors"],
                router["healthy_backends"]
            );
        }
    }
    print_ttls(audit);
    if let Some(checks) = report["checks"].as_object() {
        println!("\nScenario checks:");
        for (name, result) in checks {
            println!(
                "  {}: {}",
                name.replace('_', " "),
                if result == true { "PASS" } else { "FAIL" }
            );
        }
    }
    println!(
        "Cleanup: {}",
        if report["cleanup_succeeded"] == true {
            "PASS"
        } else {
            "FAIL"
        }
    );
    for field in ["error", "cleanup_error"] {
        if let Some(error) = report[field].as_str() {
            println!("{field}: {error}");
        }
    }
    println!("Report: {}", directory.join("report.json").display());
    if report["setup"].is_object() {
        println!("Setup: {}", directory.join("setup.json").display());
    }
}

fn print_ttls(audit: &AuditReport) {
    println!("\nRouter Redis TTLs (this test's namespace only):");
    println!(
        "  {:<20} {:>5} {:>6} {:>8} {:>6} {:>10} {:>16}  Result",
        "Type", "TTL", "Seen", "Expired", "Left", "TTL checks", "Min/max TTL ms"
    );
    for (kind, stats) in &audit.by_type {
        let range = match (stats.min_observed_ttl_ms, stats.max_observed_ttl_ms) {
            (Some(min), Some(max)) => format!("{min}/{max}"),
            _ => "not sampled".into(),
        };
        println!(
            "  {:<20} {:>4}s {:>6} {:>8} {:>6} {:>10} {:>16}  {}",
            kind.label(),
            stats.expected_ttl_ms / 1000,
            stats.observed_keys,
            stats.expired_keys,
            stats.remaining_keys,
            stats.ttl_checks,
            range,
            if stats.expiry_verified() {
                "PASS"
            } else if stats.ttl_checks == 0 {
                "NOT TESTED"
            } else {
                "FAIL"
            }
        );
    }
    let sum = |value: fn(&super::redis_audit::KeyReport) -> usize| {
        audit.by_type.values().map(value).sum::<usize>()
    };
    println!(
        "  TTL violations: missing={}, excessive={}, renewed={}",
        sum(|stats| stats.missing_ttl_checks),
        sum(|stats| stats.excessive_ttl_checks),
        sum(|stats| stats.renewed_ttl_checks)
    );
    println!(
        "  Persistent ID counter: observed={}, removed after routers stopped={}",
        audit.persistent_counter_observed, audit.persistent_counter_removed
    );
    println!(
        "  Namespace empty after cleanup: {}",
        audit.namespace_empty_after_cleanup
    );
    println!(
        "  Redis restarts: {}; evicted keys: {}; audit snapshots: {}",
        audit.redis_restarts, audit.evicted_keys, audit.snapshots
    );
}
