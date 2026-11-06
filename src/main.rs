use std::{
    fs::OpenOptions,
    io::{self, Write},
    path::PathBuf,
};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use gas_station_router_v2::{
    config::{Config, EXAMPLE_CONFIG},
    server,
};

#[derive(Parser)]
#[command(version, about = "Simple Gas Station router")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the RPC and metrics HTTP servers.
    Start {
        #[arg(long)]
        config_file: PathBuf,
    },
    /// Print an example YAML config, or write it to a new file.
    Init {
        #[arg(long, alias = "output", value_name = "PATH")]
        config_file: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Start { config_file } => server::start(Config::load(&config_file)?).await,
        Command::Init { config_file } => {
            if let Some(path) = config_file {
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .with_context(|| format!("failed to create config file {}", path.display()))?;
                file.write_all(EXAMPLE_CONFIG.as_bytes())?;
            } else {
                io::stdout().lock().write_all(EXAMPLE_CONFIG.as_bytes())?;
            }
            Ok(())
        }
    }
}
