//! Opt-in live runner. Regular `cargo test` never sends transactions.
use std::{collections::HashSet, fs::OpenOptions, io::Write, path::PathBuf, time::Duration};

use anyhow::{Context, Result, anyhow, bail, ensure};
use base64ct::{Base64, Encoding};
use clap::{Parser, Subcommand};
use gas_station_router_v2::services::reservation::ReserveGasResult;
use iota_sdk_crypto::{IotaSigner, ed25519::Ed25519PrivateKey};
use iota_sdk_types::{
    Argument, Command, GasPayment, Input, MoveCall, ObjectReference, ProgrammableTransaction,
    Transaction, TransactionExpiration, TransactionKind, TransactionV1,
};
use rand::{Rng, RngCore, rngs::OsRng};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

const DEFAULT_RPC: &str = "https://api.testnet.iota.cafe";
const DEFAULT_ROUTER: &str = "http://127.0.0.1:9257";

mod cluster;
mod full;
mod setup;

#[derive(Parser)]
#[command(about = "Incremental Gas Station Router e2e tests (Testnet)")]
struct Cli {
    #[arg(long, global = true, default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/docker/.env"))]
    env_file: PathBuf,
    #[command(subcommand)]
    command: Action,
}

#[derive(Subcommand)]
enum Action {
    /// Generate three sponsor keys and a token in .env; never overwrite it.
    Init,
    /// Print only the public sponsor addresses to fund.
    Sponsors,
    /// Test real gas reservations and round-robin coverage (works today).
    Reserve(Options),
    /// Sign and submit random transactions, then verify their finalized effects.
    Transactions(Options),
    /// Run two router binaries behind the local Nginx proxy.
    Routers,
    /// Run the Testnet, Redis and Gas Station failure scenario, including TTL drain.
    #[command(alias = "full_e2e", alias = "ful_e2e")]
    FullE2e,
}

#[derive(clap::Args)]
struct Options {
    #[arg(long, default_value_t = 9)]
    count: usize,
    /// Required distinct sponsors; set to 1 when testing a single backend.
    #[arg(long, default_value_t = 3)]
    min_sponsors: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if matches!(cli.command, Action::Init) {
        initialize(&cli.env_file)?;
    }
    if cli.env_file.exists() {
        // Do not include dotenv's error: malformed lines can contain private keys.
        dotenvy::from_path(&cli.env_file)
            .map_err(|_| anyhow!("Cannot load {}; check its syntax", cli.env_file.display()))?;
    }
    match cli.command {
        Action::Init | Action::Sponsors => print_sponsors(),
        Action::Reserve(options) => run(options, false).await,
        Action::Transactions(options) => run(options, true).await,
        Action::Routers => cluster::run().await,
        Action::FullE2e => full::run(&cli.env_file).await,
    }
}

fn initialize(path: &std::path::Path) -> Result<()> {
    let mut text = String::from("# Local Testnet keys. Do not commit this file.\n");
    for index in 1..=4 {
        let mut flagged_key = [0u8; 33];
        OsRng.fill_bytes(&mut flagged_key[1..]);
        text.push_str(&format!(
            "GAS_STATION_{index}_PRIVATE_KEY={}\n",
            Base64::encode_string(&flagged_key)
        ));
    }
    let mut token = [0u8; 32];
    OsRng.fill_bytes(&mut token);
    text.push_str(&format!(
        "GS_AUTH={}\nIOTA_RPC_URL={DEFAULT_RPC}\nIOTA_GRPC_URL=https://grpc.testnet.iota.cafe\n",
        Base64::encode_string(&token)
    ));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).with_context(|| {
        format!(
            "Cannot create {} (an existing file is never overwritten)",
            path.display()
        )
    })?;
    file.write_all(text.as_bytes())?;
    println!("Created {}", path.display());
    Ok(())
}

fn print_sponsors() -> Result<()> {
    for index in 1..=3 {
        let name = format!("GAS_STATION_{index}_PRIVATE_KEY");
        let encoded = std::env::var(&name).with_context(|| format!("Missing {name}; run init"))?;
        let bytes =
            Base64::decode_vec(&encoded).map_err(|_| anyhow!("Invalid base64 in {name}"))?;
        ensure!(
            bytes.len() == 33 && bytes[0] == 0,
            "{name} must be a flagged Ed25519 key"
        );
        let key = Ed25519PrivateKey::new(bytes[1..].try_into()?);
        println!("gas-station-{index}: {}", key.public_key().derive_address());
    }
    Ok(())
}

fn check_envelope(body: &Value) -> Result<()> {
    ensure!(
        body.get("error").is_none_or(Value::is_null),
        "API error: {}",
        body["error"]
    );
    Ok(())
}

async fn node_read(client: &Client, rpc: &str, method: &str, params: Value) -> Result<Value> {
    let body: Value = client
        .post(rpc)
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    check_envelope(&body)?;
    body.get("result")
        .filter(|value| !value.is_null())
        .cloned()
        .context("Fullnode returned no result")
}

async fn run(options: Options, execute: bool) -> Result<()> {
    ensure!(
        options.min_sponsors > 0 && options.count >= options.min_sponsors,
        "count must be >= min-sponsors, and min-sponsors must be positive"
    );
    let router = std::env::var("ROUTER_URL").unwrap_or_else(|_| DEFAULT_ROUTER.into());
    let router = router.trim_end_matches('/');
    let rpc = std::env::var("IOTA_RPC_URL").unwrap_or_else(|_| DEFAULT_RPC.into());
    let token = std::env::var("GS_AUTH").unwrap_or_default();
    setup::print_incremental(router, &rpc, &token);
    let mut auth = reqwest::header::HeaderMap::new();
    if !token.is_empty() {
        let mut header = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))?;
        header.set_sensitive(true);
        auth.insert(reqwest::header::AUTHORIZATION, header);
    }
    let router_client = Client::builder()
        .default_headers(auth)
        .timeout(Duration::from_secs(90))
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?;
    // A separate client prevents sending the router's auth token to the fullnode.
    let node_client = Client::builder().timeout(Duration::from_secs(15)).build()?;
    router_client
        .get(format!("{router}/health"))
        .send()
        .await?
        .error_for_status()
        .context("Router health check failed")?;
    if execute {
        // Check that execution rejects an invalid body before reserving any coins.
        let response = router_client
            .post(format!("{router}/v1/execute_transaction"))
            .json(&json!({}))
            .send()
            .await?;
        match response.status() {
            StatusCode::NOT_IMPLEMENTED => bail!(
                "Router execution is not implemented (HTTP 501). Use: cargo run --example e2e -- reserve"
            ),
            StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {}
            status => bail!("Execution preflight expected HTTP 400/422; got {status}"),
        }
    }
    let gas_price = if execute {
        let price = node_read(&node_client, &rpc, "iotax_getReferenceGasPrice", json!([])).await?;
        price
            .as_str()
            .context("Invalid reference gas price")?
            .parse::<u64>()?
    } else {
        0
    };
    let mut sponsors = HashSet::new();
    let mut reservations = HashSet::new();
    for index in 1..=options.count {
        let budget = rand::thread_rng().gen_range(5_000_000..=10_000_000u64);
        let body: Value = router_client
            .post(format!("{router}/v1/reserve_gas"))
            .json(
                &json!({"gas_budget": budget, "reserve_duration_secs": if execute {120} else {5}}),
            )
            .send()
            .await
            .context("Reservation request failed; its outcome may be unknown")?
            .error_for_status()
            .context("Reservation failed; check sponsor funding and station logs")?
            .json()
            .await?;
        check_envelope(&body)?;
        let reservation: ReserveGasResult = serde_json::from_value(body["result"].clone())
            .context("Invalid reservation response")?;
        ensure!(
            !reservation.gas_coins.is_empty(),
            "Reservation contains no gas coins"
        );
        ensure!(
            reservations.insert(reservation.reservation_id),
            "Duplicate router reservation ID"
        );
        sponsors.insert(reservation.sponsor_address);
        println!(
            "{index}/{} reserve OK sponsor={} reservation={}",
            options.count, reservation.sponsor_address, reservation.reservation_id
        );
        if execute {
            execute_random(
                &router_client,
                &node_client,
                router,
                &rpc,
                reservation,
                gas_price,
                budget,
            )
            .await?;
        }
    }
    ensure!(
        sponsors.len() >= options.min_sponsors,
        "Expected at least {} distinct sponsors, saw {}",
        options.min_sponsors,
        sponsors.len()
    );
    println!(
        "PASS: {} {}, {} distinct sponsors",
        options.count,
        if execute {
            "finalized transactions"
        } else {
            "reservations"
        },
        sponsors.len()
    );
    Ok(())
}

fn random_transaction(
    reservation: &ReserveGasResult,
    gas_price: u64,
    budget: u64,
) -> Result<(Transaction, Ed25519PrivateKey)> {
    // Read the shared clock 1..5 times. A fresh sender and random budget make
    // each transaction unique; the sender needs no coins or faucet funding.
    let key = Ed25519PrivateKey::generate(OsRng);
    let commands = (0..rand::thread_rng().gen_range(1..=5))
        .map(|_| {
            Ok(Command::MoveCall(MoveCall {
                package: "0x2".parse()?,
                module: "clock".parse()?,
                function: "timestamp_ms".parse()?,
                type_arguments: vec![],
                arguments: vec![Argument::Input(0)],
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let tx = TransactionV1 {
        kind: TransactionKind::new_programmable(ProgrammableTransaction {
            inputs: vec![Input::CLOCK_IMMUTABLE],
            commands,
        }),
        sender: key.public_key().derive_address(),
        gas_payment: GasPayment {
            objects: reservation
                .gas_coins
                .iter()
                .map(|coin| ObjectReference::new(coin.object_id, coin.version.into(), coin.digest))
                .collect(),
            owner: reservation.sponsor_address,
            price: gas_price,
            budget,
        },
        expiration: TransactionExpiration::None,
    };
    Ok((tx.into(), key))
}

async fn execute_random(
    client: &Client,
    node: &Client,
    router: &str,
    rpc: &str,
    reservation: ReserveGasResult,
    gas_price: u64,
    budget: u64,
) -> Result<()> {
    let (tx, key) = random_transaction(&reservation, gas_price, budget)?;
    let digest = tx.digest().to_string();
    let response = client
        .post(format!("{router}/v1/execute_transaction"))
        .json(&json!({
            "reservation_id": reservation.reservation_id, "tx_bytes": tx.to_base64(),
            "user_sig": key.sign_transaction(&tx)?.to_base64(),
            "request_type": "waitForLocalExecution",
        }))
        .send()
        .await
        .with_context(|| {
            format!("Unknown execution outcome for {digest}; check the fullnode before retrying")
        })?;
    ensure!(
        response.status() != StatusCode::NOT_IMPLEMENTED,
        "Router execution is not implemented (HTTP 501); use the reserve command"
    );
    let body: Value = response
        .error_for_status()
        .with_context(|| {
            format!("Execution HTTP error for {digest}; check the fullnode before retrying")
        })?
        .json()
        .await?;
    check_envelope(&body)?;
    ensure!(
        body["effects"]["transactionDigest"] == digest,
        "Missing or mismatched effects digest"
    );
    ensure!(
        body["effects"]["status"]["status"] == "success",
        "Transaction failed: {}",
        body["effects"]["status"]
    );
    // A successful HTTP response alone does not establish on-chain finality.
    for _ in 0..30 {
        if let Ok(result) = node_read(
            node,
            rpc,
            "iota_getTransactionBlock",
            json!([digest, {"showEffects": true}]),
        )
        .await
        {
            ensure!(
                result["digest"] == digest,
                "Fullnode returned another digest"
            );
            ensure!(
                result["effects"]["status"]["status"] == "success",
                "On-chain execution failed"
            );
            println!("  finalized OK digest={digest}");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    bail!("Could not confirm {digest} on the fullnode; do not blindly resubmit")
}

#[cfg(test)]
mod tests {
    use super::*;
    use iota_sdk_crypto::IotaVerifier;

    #[test]
    fn api_errors_are_not_successes_even_with_http_200() {
        assert!(check_envelope(&json!({"result": null, "error": "no gas"})).is_err());
        assert!(check_envelope(&json!({"error": null})).is_ok());
    }

    #[test]
    fn random_transaction_keeps_reserved_sponsor_coins_and_has_valid_sender_signature() {
        let reservation: ReserveGasResult = serde_json::from_value(json!({
            "reservation_id": 42, "sponsor_address": "0x123",
            "gas_coins": [{"objectId": "0x456", "version": 7,
                "digest": "11111111111111111111111111111111"}]
        }))
        .unwrap();
        let (tx, key) = random_transaction(&reservation, 1000, 10_000_000).unwrap();
        let decoded = Transaction::from_base64(&tx.to_base64()).unwrap();
        assert_eq!(tx, decoded);
        let data = tx.as_v1();
        assert_eq!(data.gas_payment.owner, reservation.sponsor_address);
        assert_eq!(
            data.gas_payment.objects[0].object_id(),
            &reservation.gas_coins[0].object_id
        );
        assert_eq!(data.sender, key.public_key().derive_address());
        key.verifying_key()
            .verify_transaction(&tx, &key.sign_transaction(&tx).unwrap())
            .unwrap();
    }
}
