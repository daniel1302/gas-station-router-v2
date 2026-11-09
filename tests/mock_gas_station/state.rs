use std::{
    fs,
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Control {
    pub healthy: bool,
    pub ready: bool,
    pub valid_metrics: bool,
    pub include_activity: bool,
    pub active_reservations: u64,
    pub pending_executions: u64,
    pub available_coins: u64,
    pub available_balance: u64,
    pub network_coins: u64,
    pub network_balance: u64,
    pub unreleased: u64,
    pub invariant_violations: u64,
    pub oversized: u64,
    pub chain_id: String,
    pub refresh_heals: bool,
    pub locked: bool,
    pub requires_reinitialization: bool,
    pub next_reservation: u64,
}

impl Default for Control {
    fn default() -> Self {
        Self {
            healthy: true,
            ready: true,
            valid_metrics: true,
            include_activity: true,
            active_reservations: 0,
            pending_executions: 0,
            available_coins: 100,
            available_balance: 1000,
            network_coins: 100,
            network_balance: 1000,
            unreleased: 0,
            invariant_violations: 0,
            oversized: 0,
            chain_id: "mock-testnet".into(),
            refresh_heals: true,
            locked: false,
            requires_reinitialization: false,
            next_reservation: 0,
        }
    }
}

impl Control {
    pub fn reconcile(&mut self) {
        self.available_coins = self.network_coins;
        self.available_balance = self.network_balance;
        self.requires_reinitialization = false;
    }
}

#[derive(Clone)]
pub struct State {
    pub directory: PathBuf,
    pub sponsor: String,
    pub token: String,
    mutex: Arc<Mutex<()>>,
}

impl State {
    pub fn new(directory: PathBuf, sponsor: String, token: String) -> Result<Self> {
        fs::create_dir_all(&directory)?;
        let state = Self {
            directory,
            sponsor,
            token,
            mutex: Arc::default(),
        };
        if !state.directory.join("control.json").exists() {
            state.update(|_| ())?;
        }
        Ok(state)
    }

    pub fn read(&self) -> Result<Control> {
        let path = self.directory.join("control.json");
        if !path.exists() {
            return Ok(Control::default());
        }
        Ok(serde_json::from_slice(&fs::read(path)?)?)
    }

    pub fn update<T>(&self, update: impl FnOnce(&mut Control) -> T) -> Result<T> {
        let _lock = self.mutex.lock().expect("mock state lock");
        let mut control = self.read()?;
        let result = update(&mut control);
        let temporary = self.directory.join("control.json.tmp");
        fs::write(&temporary, serde_json::to_vec_pretty(&control)?)?;
        fs::rename(temporary, self.directory.join("control.json"))?;
        Ok(result)
    }

    pub fn event(&self, event: &str, details: Value) -> Result<()> {
        let _lock = self.mutex.lock().expect("mock event lock");
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("events.jsonl"))?;
        writeln!(
            file,
            "{}",
            json!({"event": event, "pid": std::process::id(), "details": details})
        )?;
        Ok(())
    }
}
