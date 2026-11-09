//! A deterministic process fixture. It never contacts IOTA or uses sponsor keys.
mod api;
mod state;

use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

use anyhow::{Result, ensure};
use clap::Parser;
use serde::Deserialize;
use serde_json::json;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    config_path: PathBuf,
    #[arg(long)]
    allow_reinit: bool,
    #[arg(long)]
    delete_coin_registry: bool,
    #[arg(long)]
    ignore_locks: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Config {
    rpc_host_ip: IpAddr,
    rpc_port: u16,
    metrics_port: u16,
    mock: MockConfig,
}

#[derive(Deserialize)]
struct MockConfig {
    state_dir: PathBuf,
    sponsor_address: String,
    token: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();
    // Match the real Gas Station's environment-backed recovery flags.
    args.allow_reinit |= recovery_flag("ALLOW_REINIT")?;
    args.delete_coin_registry |= recovery_flag("DELETE_COIN_REGISTRY")?;
    args.ignore_locks |= recovery_flag("IGNORE_LOCKS")?;
    let config: Config = serde_yaml_ng::from_slice(&std::fs::read(&args.config_path)?)?;
    let state = state::State::new(
        config.mock.state_dir,
        config.mock.sponsor_address,
        config.mock.token,
    )?;
    state.event("start", json!({"allow_reinit":args.allow_reinit,"delete_coin_registry":args.delete_coin_registry,"ignore_locks":args.ignore_locks}))?;
    if !initialize(&args, &state)? {
        return Ok(());
    }
    let rpc =
        tokio::net::TcpListener::bind(SocketAddr::new(config.rpc_host_ip, config.rpc_port)).await?;
    let metrics =
        tokio::net::TcpListener::bind(SocketAddr::new(config.rpc_host_ip, config.metrics_port))
            .await?;
    tokio::try_join!(
        axum::serve(rpc, api::router(state.clone())).into_future(),
        axum::serve(metrics, api::router(state)).into_future()
    )?;
    Ok(())
}

fn recovery_flag(name: &str) -> Result<bool> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid boolean recovery flag")),
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(_) => anyhow::bail!("Invalid recovery flag encoding"),
    }
}

fn initialize(args: &Args, state: &state::State) -> Result<bool> {
    ensure!(
        !args.delete_coin_registry || args.allow_reinit,
        "Registry deletion requires --allow-reinit"
    );
    let control = state.read()?;
    if args.delete_coin_registry && control.locked && !args.ignore_locks {
        state.event("lock_blocked", json!({}))?;
        eprintln!("Another instance is already performing maintenance");
        // Let the parent's bounded output reader consume the signal before exit.
        std::thread::sleep(std::time::Duration::from_millis(100));
        return Ok(false);
    }
    if control.requires_reinitialization && !args.allow_reinit {
        eprintln!("automatic reinitialization is not allowed");
        std::thread::sleep(std::time::Duration::from_millis(100));
        return Ok(false);
    }
    if args.delete_coin_registry {
        state.update(|control| control.reconcile())?;
        state.event("rescan", json!({"ignore_locks":args.ignore_locks}))?;
    }
    Ok(true)
}
