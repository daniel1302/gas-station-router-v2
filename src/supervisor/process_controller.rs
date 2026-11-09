use std::{path::Path, process::Stdio, time::Duration};

use anyhow::{Context, Result};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    sync::mpsc,
    task::JoinHandle,
};

use super::{config::ProcessConfig, recovery_policy::Signal};

#[cfg(test)]
mod tests;

pub(super) struct ProcessController {
    child: Option<Child>,
    readers: Vec<JoinHandle<()>>,
    pub generation: u64,
}

impl ProcessController {
    pub fn new() -> Self {
        Self {
            child: None,
            readers: Vec::new(),
            generation: 0,
        }
    }

    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(Child::id)
    }

    pub fn start(
        &mut self,
        config: &ProcessConfig,
        recovery: Option<bool>,
        events: mpsc::Sender<(u64, Signal)>,
    ) -> Result<()> {
        let mut command = Command::new(&config.binary);
        command.arg("--config-path").arg(&config.config);
        if let Some(ignore_locks) = recovery {
            command.args(["--allow-reinit", "--delete-coin-registry"]);
            if ignore_locks {
                command.arg("--ignore-locks");
            }
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .process_group(0);
        let mut child = command
            .spawn()
            .context("Cannot start Gas Station process")?;
        self.generation += 1;
        let generation = self.generation;
        self.readers.push(tokio::spawn(read_signals(
            child.stdout.take().expect("piped stdout"),
            generation,
            events.clone(),
        )));
        self.readers.push(tokio::spawn(read_signals(
            child.stderr.take().expect("piped stderr"),
            generation,
            events,
        )));
        tracing::info!(
            pid = child.id(),
            generation,
            full_rescan = recovery.is_some(),
            "Gas Station started"
        );
        self.child = Some(child);
        Ok(())
    }

    pub fn exited(&mut self) -> Result<Option<std::process::ExitStatus>> {
        let Some(child) = &mut self.child else {
            return Ok(None);
        };
        let pid = child.id();
        let result = child
            .try_wait()
            .context("Cannot inspect Gas Station process")?;
        if result.is_some() {
            if let Some(pid) = pid {
                signal_group(pid, libc::SIGKILL);
            }
            self.child = None;
            self.clear_readers();
        }
        Ok(result)
    }

    pub async fn stop(&mut self, timeout: Duration) -> Result<()> {
        // Keep ownership in self while awaiting shutdown. If this future is
        // cancelled, the outer shutdown handler can still stop the process group.
        if let Some(child) = &mut self.child {
            let pid = child.id();
            if let Some(pid) = pid {
                signal_group(pid, libc::SIGTERM);
            }
            match tokio::time::timeout(timeout, child.wait()).await {
                Ok(result) => {
                    result.context("Cannot reap Gas Station process")?;
                }
                Err(_) => {
                    tracing::warn!("Gas Station shutdown timed out; killing owned process group");
                    if let Some(pid) = pid {
                        signal_group(pid, libc::SIGKILL);
                    }
                    child
                        .wait()
                        .await
                        .context("Cannot reap Gas Station process")?;
                }
            }
            if let Some(pid) = pid {
                signal_group(pid, libc::SIGKILL);
            }
        }
        self.child = None;
        self.clear_readers();
        Ok(())
    }

    fn clear_readers(&mut self) {
        for task in self.readers.drain(..) {
            task.abort();
        }
    }
}

impl Drop for ProcessController {
    fn drop(&mut self) {
        if let Some(pid) = self.pid() {
            signal_group(pid, libc::SIGKILL);
        }
        self.clear_readers();
    }
}

fn signal_group(pid: u32, signal: i32) {
    // Each child starts its own process group. Never signal unrelated processes.
    unsafe {
        libc::kill(-(pid as i32), signal);
    }
}

async fn read_signals(
    mut reader: impl AsyncRead + Unpin,
    generation: u64,
    events: mpsc::Sender<(u64, Signal)>,
) {
    let mut buffer = [0; 4096];
    let mut overlap = String::new();
    while let Ok(count) = reader.read(&mut buffer).await {
        if count == 0 {
            break;
        }
        overlap.push_str(&String::from_utf8_lossy(&buffer[..count]));
        if let Some(signal) = Signal::from_output(&overlap) {
            let _ = events.try_send((generation, signal));
            overlap.clear();
        } else if overlap.len() > 1024 {
            let mut start = overlap.len() - 1024;
            while !overlap.is_char_boundary(start) {
                start += 1;
            }
            overlap.drain(..start);
        }
    }
}

pub(super) fn absolute(path: &Path) -> Result<std::path::PathBuf> {
    path.canonicalize()
        .context("Cannot resolve Gas Station binary or config path")
}
