//! Process-level E2E tests without chain access or sponsor keys.
mod common;
mod router;
mod supervisor;

use anyhow::Result;
use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, ValueEnum)]
enum Suite {
    Router,
    Supervisor,
    All,
}

#[derive(Parser)]
struct Args {
    #[arg(value_enum, default_value = "all")]
    suite: Suite,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if matches!(args.suite, Suite::Router | Suite::All) {
        router::run().await?;
    }
    if matches!(args.suite, Suite::Supervisor | Suite::All) {
        supervisor::run().await?;
    }
    Ok(())
}
