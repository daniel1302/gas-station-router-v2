use super::*;
use std::{fs, os::unix::fs::PermissionsExt};

struct Fixture(std::path::PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn recovery_flags_are_one_shot_and_shutdown_reaps_the_child() {
    let directory = std::env::current_dir()
        .unwrap()
        .join("target")
        .join(format!("supervisor-process-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&directory).unwrap();
    let fixture = Fixture(directory);
    let binary = fixture.0.join("mock station");
    fs::write(&binary, "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$(dirname \"$0\")/args\"\ntrap 'exit 0' TERM\nwhile :; do sleep 1; done\n").unwrap();
    fs::set_permissions(&binary, fs::Permissions::from_mode(0o700)).unwrap();
    let config = ProcessConfig {
        binary,
        config: fixture.0.join("station.yaml"),
        ..Default::default()
    };
    fs::write(&config.config, "").unwrap();
    let (sender, _) = mpsc::channel(4);
    let mut process = ProcessController::new();
    for recovery in [Some(false), Some(true), None] {
        let _ = fs::remove_file(fixture.0.join("args"));
        process.start(&config, recovery, sender.clone()).unwrap();
        let arguments = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(text) = fs::read_to_string(fixture.0.join("args"))
                    && text.contains("--config-path")
                {
                    break text;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            arguments.contains("--delete-coin-registry"),
            recovery.is_some()
        );
        assert_eq!(arguments.contains("--allow-reinit"), recovery.is_some());
        assert_eq!(arguments.contains("--ignore-locks"), recovery == Some(true));
        assert!(arguments.contains(config.config.to_str().unwrap()));
        process.stop(Duration::from_secs(2)).await.unwrap();
        assert!(process.pid().is_none());
    }
}
