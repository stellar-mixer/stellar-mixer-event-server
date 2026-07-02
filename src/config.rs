use anyhow::{bail, Context, Result};
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub stellar_rpc_url: String,
    pub mixer_contract_id: String,
    pub start_ledger: Option<u64>,
    pub db_path: PathBuf,
    pub poll_interval: Duration,
    pub batch_ledgers: u64,
    pub events_limit: u32,
    pub event_finality_lag: u64,
    pub catchup_sleep: Duration,
}

impl ServerConfig {
    pub fn from_env() -> Result<Self> {
        let bind_addr = read_env("MIXER_ARCHIVE_BIND_ADDR", "TREEPIR_BIND_ADDR")
            .unwrap_or_else(|| "0.0.0.0:3001".to_string())
            .parse()
            .context("invalid MIXER_ARCHIVE_BIND_ADDR")?;

        let stellar_rpc_url = read_env("MIXER_ARCHIVE_STELLAR_RPC_URL", "TREEPIR_STELLAR_RPC_URL")
            .unwrap_or_else(|| "https://soroban-testnet.stellar.org".to_string());

        let mixer_contract_id = read_env(
            "MIXER_ARCHIVE_MIXER_CONTRACT_ID",
            "TREEPIR_MIXER_CONTRACT_ID",
        )
        .context("missing MIXER_ARCHIVE_MIXER_CONTRACT_ID")?;

        if mixer_contract_id.trim().is_empty() {
            bail!("MIXER_ARCHIVE_MIXER_CONTRACT_ID is empty");
        }

        let start_ledger = match read_env("MIXER_ARCHIVE_START_LEDGER", "TREEPIR_START_LEDGER") {
            Some(value) if !value.trim().is_empty() => Some(
                value
                    .parse::<u64>()
                    .context("invalid MIXER_ARCHIVE_START_LEDGER")?,
            ),
            _ => None,
        };

        let db_path = read_env("MIXER_ARCHIVE_DB_PATH", "TREEPIR_DB_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./mixer-archive-state.rocksdb"));

        let poll_interval_ms =
            parse_optional_u64("MIXER_ARCHIVE_POLL_INTERVAL_MS", "TREEPIR_POLL_INTERVAL_MS")?
                .unwrap_or(2000);

        let batch_ledgers =
            parse_optional_u64("MIXER_ARCHIVE_BATCH_LEDGERS", "TREEPIR_BATCH_LEDGERS")?
                .unwrap_or(10_000)
                .clamp(1, 10_000);

        let events_limit = parse_optional_u64("MIXER_ARCHIVE_EVENTS_LIMIT", "TREEPIR_EVENTS_LIMIT")?
            .unwrap_or(10_000)
            .clamp(1, 10_000) as u32;

        let event_finality_lag = parse_optional_u64(
            "MIXER_ARCHIVE_EVENT_FINALITY_LAG",
            "TREEPIR_EVENT_FINALITY_LAG",
        )?
        .unwrap_or(8);

        let catchup_sleep_ms =
            parse_optional_u64("MIXER_ARCHIVE_CATCHUP_SLEEP_MS", "TREEPIR_CATCHUP_SLEEP_MS")?
                .unwrap_or(300);

        Ok(Self {
            bind_addr,
            stellar_rpc_url,
            mixer_contract_id,
            start_ledger,
            db_path,
            poll_interval: Duration::from_millis(poll_interval_ms),
            batch_ledgers,
            events_limit,
            event_finality_lag,
            catchup_sleep: Duration::from_millis(catchup_sleep_ms),
        })
    }
}

fn read_env(primary: &str, fallback: &str) -> Option<String> {
    env::var(primary)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            env::var(fallback)
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
}

fn parse_optional_u64(primary: &str, fallback: &str) -> Result<Option<u64>> {
    read_env(primary, fallback)
        .map(|value| {
            value
                .parse::<u64>()
                .with_context(|| format!("invalid {primary}"))
        })
        .transpose()
}
