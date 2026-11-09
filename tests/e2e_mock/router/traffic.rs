use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, ensure};
use gas_station_router_v2::services::reservation::ReserveGasResult;
use iota_sdk_crypto::{IotaSigner, ed25519::Ed25519PrivateKey};
use iota_sdk_types::{
    GasPayment, ObjectReference, ProgrammableTransaction, Transaction, TransactionExpiration,
    TransactionKind, TransactionV1,
};
use rand::{Rng, rngs::OsRng};
use serde_json::{Value, json};

use crate::common;

pub(super) struct Traffic {
    pub proxy: String,
    pub client: reqwest::Client,
    pub transactions: usize,
    pub sponsors: HashSet<String>,
    pub upstreams: HashMap<String, usize>,
    ids: HashSet<u64>,
    pub last: Option<Value>,
}

impl Traffic {
    pub fn new(proxy: String) -> Result<Self> {
        Ok(Self {
            proxy,
            client: common::client()?,
            transactions: 0,
            sponsors: HashSet::new(),
            upstreams: HashMap::new(),
            ids: HashSet::new(),
            last: None,
        })
    }

    async fn post(&mut self, path: &str, body: &Value) -> Result<Value> {
        let response = self
            .client
            .post(format!("{}{path}", self.proxy))
            .bearer_auth(common::TOKEN)
            .json(body)
            .send()
            .await?;
        let upstream = response
            .headers()
            .get("x-router-upstream")
            .context("Missing Nginx upstream")?
            .to_str()?
            .to_owned();
        *self.upstreams.entry(upstream).or_default() += 1;
        let response = response.error_for_status()?.json::<Value>().await?;
        ensure!(response["error"].is_null(), "Router returned an API error");
        Ok(response)
    }

    pub async fn transaction(&mut self) -> Result<String> {
        let budget = rand::thread_rng().gen_range(1_000_000..=10_000_000u64);
        let reply = self
            .post(
                "/v1/reserve_gas",
                &json!({"gas_budget":budget,"reserve_duration_secs":5}),
            )
            .await?;
        let reservation: ReserveGasResult = serde_json::from_value(reply["result"].clone())?;
        ensure!(
            self.ids.insert(reservation.reservation_id),
            "Reservation ID was reused"
        );
        let sponsor = reservation.sponsor_address.to_string();
        self.sponsors.insert(sponsor.clone());
        let request = transaction(&reservation, budget)?;
        let original = self.post("/v1/execute_tx", &request).await?;
        let replay = self.post("/v1/execute_tx", &request).await?;
        ensure!(
            original == replay && original["effects"]["status"]["status"] == "success",
            "Shared execution cache changed the result"
        );
        self.last = Some(request);
        self.transactions += 1;
        Ok(sponsor)
    }

    pub async fn replay(&mut self) -> Result<()> {
        let request = self.last.clone().context("No cached transaction")?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
        let reply = loop {
            match self.post("/v1/execute_tx", &request).await {
                Ok(reply) => break reply,
                Err(error) => {
                    let retryable = error
                        .downcast_ref::<reqwest::Error>()
                        .and_then(reqwest::Error::status)
                        .is_some_and(|status| {
                            status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                                || status == reqwest::StatusCode::GATEWAY_TIMEOUT
                        });
                    if !retryable || tokio::time::Instant::now() >= deadline {
                        return Err(error);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            }
        };
        ensure!(
            reply["effects"]["status"]["status"] == "success",
            "Cached execution was lost"
        );
        Ok(())
    }
}

fn transaction(reservation: &ReserveGasResult, budget: u64) -> Result<Value> {
    let key = Ed25519PrivateKey::generate(OsRng);
    let transaction: Transaction = TransactionV1 {
        kind: TransactionKind::new_programmable(ProgrammableTransaction {
            inputs: vec![],
            commands: vec![],
        }),
        sender: key.public_key().derive_address(),
        gas_payment: GasPayment {
            owner: reservation.sponsor_address,
            price: 1000,
            budget,
            objects: reservation
                .gas_coins
                .iter()
                .map(|coin| ObjectReference::new(coin.object_id, coin.version.into(), coin.digest))
                .collect(),
        },
        expiration: TransactionExpiration::None,
    }
    .into();
    Ok(
        json!({"reservation_id":reservation.reservation_id,"tx_bytes":transaction.to_base64(),"user_sig":key.sign_transaction(&transaction)?.to_base64()}),
    )
}
