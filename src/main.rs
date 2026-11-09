use std::{
    fs::OpenOptions,
    io::{self, IsTerminal, Write},
    path::PathBuf,
    process::ExitCode,
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use gas_station_router_v2::router::{
    config::{Config, EXAMPLE_CONFIG},
    server,
};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version, about = "Gas Station router and supervisor")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start and supervise one Gas Station process.
    Supervisor {
        #[arg(long)]
        config_file: Option<PathBuf>,
        #[arg(long)]
        gas_station_binary: Option<PathBuf>,
        #[arg(long)]
        gas_station_config: Option<PathBuf>,
        /// Authorize one full rescan after verifying network and in-flight work.
        #[arg(long)]
        approve_recovery: bool,
    },
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
async fn main() -> ExitCode {
    if let Err(error) = init_logging() {
        eprintln!("{error}");
        return ExitCode::FAILURE;
    }

    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = %format!("{error:#}"), "Command failed");
            ExitCode::FAILURE
        }
    }
}

fn init_logging() -> Result<()> {
    let json = match std::env::var("LOG_FORMAT").as_deref() {
        Ok("json") => true,
        Ok("text") | Err(std::env::VarError::NotPresent) => false,
        _ => bail!("LOG_FORMAT must be 'text' or 'json'"),
    };
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_writer(io::stderr);
    if json {
        subscriber.json().with_ansi(false).init();
    } else {
        subscriber.with_ansi(io::stderr().is_terminal()).init();
    }
    Ok(())
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Supervisor {
            config_file,
            gas_station_binary,
            gas_station_config,
            approve_recovery,
        } => {
            use gas_station_router_v2::supervisor;
            let mut config = config_file
                .map(|path| supervisor::config::Config::load(&path))
                .transpose()?
                .unwrap_or_default();
            if let Some(binary) = gas_station_binary {
                config.process.binary = binary;
            }
            if let Some(path) = gas_station_config {
                config.process.config = path;
            }
            supervisor::run(config, approve_recovery).await
        }
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
