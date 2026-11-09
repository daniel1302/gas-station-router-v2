use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use tokio::time::sleep;

pub const ROOT: &str = env!("CARGO_MANIFEST_DIR");
pub const TOKEN: &str = "local-e2e-auth";

pub fn directory(name: &str) -> Result<PathBuf> {
    let path = Path::new(ROOT)
        .join("docker/.runtime")
        .join(format!("{name}-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&path)?;
    println!("[setup] Logs and report: {}", path.display());
    Ok(path)
}

pub fn ports(count: usize) -> Result<Vec<u16>> {
    let listeners = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0"))
        .collect::<std::io::Result<Vec<_>>>()?;
    Ok(listeners
        .iter()
        .map(|listener| listener.local_addr().map(|address| address.port()))
        .collect::<std::io::Result<_>>()?)
}

pub fn binary(name: &str) -> Result<PathBuf> {
    // Examples and the main binary share the same profile directory.
    let current = std::env::current_exe()?;
    let examples = current.parent().context("Missing examples directory")?;
    Ok(if name == "gas-station-router-v2" {
        examples
            .parent()
            .context("Missing target directory")?
            .join(name)
    } else {
        examples.join(name)
    })
}

pub struct Process {
    child: Child,
    pub pid: u32,
}

impl Process {
    pub fn start(binary: &str, args: &[&str], log: &Path) -> Result<Self> {
        use std::os::unix::process::CommandExt;
        let child = Command::new(self::binary(binary)?)
            .args(args)
            .current_dir(ROOT)
            .env("LOG_FORMAT", "json")
            .env("RUST_LOG", "info")
            .stdout(Stdio::null())
            .stderr(fs::File::create(log)?)
            .process_group(0)
            .spawn()?;
        let pid = child.id();
        Ok(Self { child, pid })
    }

    pub fn stop(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            unsafe {
                libc::kill(-(self.pid as i32), libc::SIGTERM);
            }
            let deadline = Instant::now() + Duration::from_secs(8);
            while self.child.try_wait()?.is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
            if self.child.try_wait()?.is_none() {
                unsafe {
                    libc::kill(-(self.pid as i32), libc::SIGKILL);
                }
                self.child.wait()?;
                bail!("Owned process did not stop gracefully");
            }
        }
        Ok(())
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

pub struct Station {
    pub directory: PathBuf,
    pub config: PathBuf,
    pub url: String,
    pub metrics_url: String,
    pub sponsor: String,
}

impl Station {
    pub fn new(root: &Path, name: &str, sponsor: u8) -> Result<Self> {
        let directory = root.join(name);
        fs::create_dir_all(&directory)?;
        let ports = ports(2)?;
        let sponsor = iota_sdk_types::Address::new([sponsor; 32]).to_string();
        let config = directory.join("station.yaml");
        let yaml = json!({"rpc-host-ip":"127.0.0.1", "rpc-port":ports[0], "metrics-port":ports[1],
            "coin-init-config":{"refresh-interval-sec":1},
            "mock":{"state_dir":directory,"sponsor_address":sponsor,"token":TOKEN}});
        fs::write(&config, serde_yaml_ng::to_string(&yaml)?)?;
        let station = Self {
            directory,
            config,
            sponsor,
            url: format!("http://127.0.0.1:{}", ports[0]),
            metrics_url: format!("http://127.0.0.1:{}", ports[1]),
        };
        station.set(json!({}))?;
        Ok(station)
    }

    pub fn start(&self) -> Result<Process> {
        Process::start(
            "mock-gas-station",
            &[
                "--config-path",
                self.config.to_str().context("Station path")?,
            ],
            &self.directory.join("station.log"),
        )
    }

    pub fn set(&self, patch: Value) -> Result<()> {
        let path = self.directory.join("control.json");
        let mut control: Value = if path.exists() {
            serde_json::from_slice(&fs::read(&path)?)?
        } else {
            json!({})
        };
        for (key, value) in patch
            .as_object()
            .context("Control patch must be an object")?
        {
            control[key] = value.clone();
        }
        let temporary = self.directory.join("control.next.json");
        fs::write(&temporary, serde_json::to_vec_pretty(&control)?)?;
        fs::rename(temporary, path)?;
        Ok(())
    }

    pub fn events(&self) -> Result<Vec<Value>> {
        let path = self.directory.join("events.jsonl");
        if !path.exists() {
            return Ok(Vec::new());
        }
        fs::read_to_string(path)?
            .lines()
            .map(|line| serde_json::from_str(line).map_err(Into::into))
            .collect()
    }

    pub fn event_count(&self, event: &str) -> Result<usize> {
        Ok(self
            .events()?
            .iter()
            .filter(|entry| entry["event"] == event)
            .count())
    }
}

pub fn client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?)
}

pub async fn wait_health(url: &str) -> Result<()> {
    let client = client()?;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if client
            .get(format!("{url}/"))
            .send()
            .await
            .is_ok_and(|reply| reply.status().is_success())
        {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "Service did not become healthy: {url}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

pub async fn wait_for(description: &str, mut check: impl FnMut() -> Result<bool>) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if check()? {
            println!("[ok] {description}");
            return Ok(());
        }
        ensure!(Instant::now() < deadline, "Timed out: {description}");
        sleep(Duration::from_millis(50)).await;
    }
}

pub fn report(directory: &Path, name: &str, details: Value) -> Result<()> {
    fs::write(
        directory.join("report.json"),
        serde_json::to_vec_pretty(&json!({"suite":name,"status":"pass","details":details}))?,
    )?;
    println!("PASS {name}");
    Ok(())
}
