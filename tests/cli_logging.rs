use std::process::{Command, Output};

use serde_json::Value;
use uuid::Uuid;

fn command(format: Option<&str>, filter: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_gas-station-router-v2"));
    command.env_remove("LOG_FORMAT").env("RUST_LOG", filter);
    if let Some(format) = format {
        command.env("LOG_FORMAT", format);
    }
    command
}

fn failure(format: Option<&str>, filter: &str) -> Output {
    command(format, filter)
        .args(["start", "--config-file"])
        .arg(std::env::temp_dir().join(format!("missing-router-config-{}.yaml", Uuid::new_v4())))
        .output()
        .unwrap()
}

#[test]
fn json_logs_are_structured_on_stderr_without_ansi() {
    let output = failure(Some("json"), "info");
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stderr.contains('\u{1b}'));
    let records: Vec<Value> = stderr
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["level"], "ERROR");
    assert_eq!(records[0]["fields"]["message"], "Command failed");
    assert!(
        records[0]["fields"]["error"]
            .as_str()
            .unwrap()
            .contains("failed to read config file")
    );
    assert!(records[0]["timestamp"].is_string());
    assert!(records[0]["target"].is_string());
}

#[test]
fn text_is_the_default_and_filtering_works_in_both_formats() {
    for format in [None, Some("text")] {
        let output = failure(format, "info");
        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains("ERROR"));
        assert!(stderr.contains("Command failed"));
        assert!(serde_json::from_str::<Value>(&stderr).is_err());
    }
    for format in [Some("json"), Some("text")] {
        let output = failure(format, "off");
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(output.stderr.is_empty());
    }
}

#[test]
fn json_logging_preserves_yaml_init_output() {
    let output = command(Some("json"), "info").arg("init").output().unwrap();
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let yaml: serde_yaml_ng::Value = serde_yaml_ng::from_slice(&output.stdout).unwrap();
    assert!(yaml.get("rpc").is_some());
    assert!(yaml.get("health_checks").is_some());
}

#[test]
fn unsupported_log_formats_fail_without_echoing_the_value() {
    let output = failure(Some("UNSUPPORTED_FORMAT"), "info");
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("LOG_FORMAT must be 'text' or 'json'"));
    assert!(!stderr.contains("UNSUPPORTED_FORMAT"));
}
