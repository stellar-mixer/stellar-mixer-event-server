use crate::config::ServerConfig;
use mixer_archive_server::state_store::PersistentArchiveStore;

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

use stellar_rpc_client::{
    Client as StellarRpcClient, Event as StellarEvent, EventStart, EventType,
};
use stellar_xdr::{
    ContractEvent, ContractEventBody, LedgerCloseMeta, Limits, ReadXdr, ScVal, TransactionMeta,
};

#[derive(Debug)]
pub struct StellarMixerArchiveIndexer {
    config: ServerConfig,
    rpc: StellarEventClient,
    store: Arc<RwLock<PersistentArchiveStore>>,
}

#[derive(Debug, Clone)]
struct StellarEventClient {
    client: StellarRpcClient,
    http: reqwest::Client,
    rpc_url: String,
    contract_id: String,
    contract_id_raw: [u8; 32],
    events_limit: usize,
    ledgers_limit: usize,
}

#[derive(Debug, Clone)]
struct LocalLedgerArchiveEvent {
    id: String,
    ledger: u32,
    parsed: Vec<MixerArchiveEvent>,
}

#[derive(Debug, Clone)]
enum MixerArchiveEvent {
    EncryptedNote {
        index: u64,
        leaf: [u8; 32],
        encrypted_note: Vec<u8>,
    },
    Nullifiers {
        nullifiers: Vec<[u8; 32]>,
        source: &'static str,
    },
}

impl StellarMixerArchiveIndexer {
    pub fn new(config: ServerConfig, store: Arc<RwLock<PersistentArchiveStore>>) -> Self {
        let contract_id_raw = config
            .mixer_contract_id
            .parse::<stellar_strkey::Contract>()
            .expect("invalid MIXER_ARCHIVE_MIXER_CONTRACT_ID strkey")
            .0;

        let rpc = StellarEventClient {
            client: StellarRpcClient::new(&config.stellar_rpc_url)
                .expect("invalid MIXER_ARCHIVE_STELLAR_RPC_URL"),
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .pool_idle_timeout(Duration::from_secs(30))
                .build()
                .expect("failed to build reqwest client"),
            rpc_url: config.stellar_rpc_url.clone(),
            contract_id: config.mixer_contract_id.clone(),
            contract_id_raw,
            events_limit: config.events_limit as usize,
            ledgers_limit: (config.events_limit as usize).clamp(1, 200),
        };

        Self { config, rpc, store }
    }

    pub async fn catch_up_once(&mut self) -> Result<()> {
        let latest = self.rpc.latest_ledger().await?;
        let target = latest.saturating_sub(self.config.event_finality_lag);
        self.catch_up_to(target).await
    }

    pub async fn run_forever(mut self) -> Result<()> {
        let mut consecutive_transient_failures = 0u32;

        loop {
            match self.catch_up_live_once().await {
                Ok(()) => {
                    consecutive_transient_failures = 0;
                    sleep(self.config.poll_interval).await;
                }
                Err(error) if is_probably_transient_rpc_error(&error) => {
                    consecutive_transient_failures =
                        consecutive_transient_failures.saturating_add(1);

                    let backoff = transient_rpc_backoff(consecutive_transient_failures);
                    let store = self.store.read().await;

                    warn!(
                        %error,
                        consecutive_transient_failures,
                        backoff_ms = backoff.as_millis(),
                        last_indexed_ledger = store.last_indexed_ledger(),
                        encrypted_note_count = store.encrypted_note_count(),
                        nullifier_count = store.nullifier_count(),
                        "transient Stellar RPC error; retrying mixer archive indexer"
                    );

                    drop(store);
                    sleep(backoff).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn catch_up_live_once(&mut self) -> Result<()> {
        let latest = self.rpc.latest_ledger().await?;
        self.catch_up_live_ledgers_to(latest).await
    }

    async fn catch_up_to(&mut self, latest_ledger: u64) -> Result<()> {
        let mut from = {
            let store = self.store.read().await;
            store.last_indexed_ledger().saturating_add(1)
        };

        if from > latest_ledger {
            return Ok(());
        }

        while from <= latest_ledger {
            let end_exclusive = from
                .saturating_add(self.config.batch_ledgers)
                .min(latest_ledger.saturating_add(1));

            let events = self.rpc.events_for_range(from, end_exclusive).await?;

            if !events.is_empty() {
                info!(
                    from,
                    end_exclusive,
                    events = events.len(),
                    "startup fetched Stellar mixer archive events through getEvents"
                );
            }

            for event in events {
                self.apply_rpc_event(event).await?;
            }

            {
                let mut store = self.store.write().await;
                store.set_last_indexed_ledger(end_exclusive.saturating_sub(1));
                store.save()?;

                info!(
                    from,
                    end_exclusive,
                    last_indexed_ledger = store.last_indexed_ledger(),
                    encrypted_note_count = store.encrypted_note_count(),
                    nullifier_count = store.nullifier_count(),
                    "startup indexed Stellar mixer archive ledger range"
                );
            }

            from = end_exclusive;

            if from <= latest_ledger && !self.config.catchup_sleep.is_zero() {
                sleep(self.config.catchup_sleep).await;
            }
        }

        Ok(())
    }

    async fn catch_up_live_ledgers_to(&mut self, latest_ledger: u64) -> Result<()> {
        let mut from = {
            let store = self.store.read().await;
            store.last_indexed_ledger().saturating_add(1)
        };

        if from > latest_ledger {
            return Ok(());
        }

        while from <= latest_ledger {
            let end_exclusive = from
                .saturating_add(self.config.batch_ledgers)
                .min(latest_ledger.saturating_add(1));

            let events = self
                .rpc
                .events_from_ledger_close_meta_range(from, end_exclusive)
                .await?;

            if !events.is_empty() {
                info!(
                    from,
                    end_exclusive,
                    events = events.len(),
                    "live fetched Stellar mixer archive events from LedgerCloseMeta"
                );
            }

            for event in events {
                self.apply_local_ledger_event(event).await?;
            }

            {
                let mut store = self.store.write().await;
                store.set_last_indexed_ledger(end_exclusive.saturating_sub(1));
                store.save()?;

                info!(
                    from,
                    end_exclusive,
                    last_indexed_ledger = store.last_indexed_ledger(),
                    encrypted_note_count = store.encrypted_note_count(),
                    nullifier_count = store.nullifier_count(),
                    "live indexed Stellar mixer archive ledger range from LedgerCloseMeta"
                );
            }

            from = end_exclusive;

            if from <= latest_ledger && !self.config.catchup_sleep.is_zero() {
                sleep(self.config.catchup_sleep).await;
            }
        }

        Ok(())
    }

    async fn apply_rpc_event(&mut self, event: StellarEvent) -> Result<()> {
        if event.contract_id != self.config.mixer_contract_id {
            return Ok(());
        }

        let parsed = parse_mixer_archive_events(&event)?;

        if parsed.is_empty() {
            return Ok(());
        }

        self.apply_parsed_archive_events(&event.id, u64::from(event.ledger), parsed)
            .await
    }

    async fn apply_local_ledger_event(&mut self, event: LocalLedgerArchiveEvent) -> Result<()> {
        if event.parsed.is_empty() {
            return Ok(());
        }

        self.apply_parsed_archive_events(&event.id, u64::from(event.ledger), event.parsed)
            .await
    }

    async fn apply_parsed_archive_events(
        &mut self,
        event_id: &str,
        event_ledger: u64,
        parsed_events: Vec<MixerArchiveEvent>,
    ) -> Result<()> {
        let mut store = self.store.write().await;

        let mut encrypted_note_offset = 0usize;
        let mut nullifier_offset = 0usize;
        let mut changed = false;

        for parsed in parsed_events {
            match parsed {
                MixerArchiveEvent::EncryptedNote {
                    index,
                    leaf,
                    encrypted_note,
                } => {
                    let child_event_id =
                        format!("{event_id}#encrypted_note#{encrypted_note_offset}");
                    encrypted_note_offset += 1;

                    if store.has_event_id(&child_event_id) {
                        continue;
                    }

                    let current_count = store.encrypted_note_count();

                    if index < current_count {
                        warn!(
                            event_id = child_event_id,
                            index, current_count, "skipping already-indexed encrypted note"
                        );
                        continue;
                    }

                    store.append_encrypted_note_record(
                        index,
                        leaf,
                        encrypted_note,
                        &child_event_id,
                        event_ledger,
                    )?;
                    changed = true;
                }
                MixerArchiveEvent::Nullifiers { nullifiers, source } => {
                    for nullifier in nullifiers {
                        let child_event_id = format!("{event_id}#nullifier#{nullifier_offset}");
                        nullifier_offset += 1;

                        if store.has_event_id(&child_event_id) {
                            continue;
                        }

                        store.append_nullifier_record(
                            nullifier,
                            &child_event_id,
                            event_ledger,
                            source,
                        )?;
                        changed = true;
                    }
                }
            }
        }

        if changed {
            store.save()?;
        }

        Ok(())
    }
}

impl StellarEventClient {
    async fn latest_ledger(&self) -> Result<u64> {
        let latest = self.client.get_latest_ledger().await?;
        Ok(u64::from(latest.sequence))
    }

    async fn events_for_range(
        &self,
        start_ledger: u64,
        end_ledger: u64,
    ) -> Result<Vec<StellarEvent>> {
        if start_ledger >= end_ledger {
            return Ok(Vec::new());
        }

        let start = u32::try_from(start_ledger).context("start ledger does not fit u32")?;
        let end_inclusive =
            u32::try_from(end_ledger.saturating_sub(1)).context("end ledger does not fit u32")?;

        let mut out = Vec::new();
        let mut start_at = EventStart::ledger_range(start, end_inclusive)
            .map_err(|error| anyhow::anyhow!("invalid Stellar event ledger range: {error}"))?;

        loop {
            let page = self
                .client
                .get_events(
                    start_at.clone(),
                    Some(EventType::Contract),
                    &[self.contract_id.clone()],
                    &[],
                    Some(self.events_limit),
                )
                .await
                .with_context(|| {
                    format!("getEvents failed for ledger range [{start_ledger}, {end_ledger})")
                })?;

            if u64::from(page.latest_ledger) < start_ledger {
                warn!(
                    latest_ledger = page.latest_ledger,
                    start_ledger, "RPC latest ledger is behind requested getEvents range"
                );
            }

            let page_count = page.events.len();
            let reached_end = page
                .events
                .iter()
                .any(|event| u64::from(event.ledger) >= end_ledger);

            out.extend(
                page.events
                    .into_iter()
                    .filter(|event| u64::from(event.ledger) < end_ledger),
            );

            if reached_end || page_count < self.events_limit || page.cursor.is_empty() {
                break;
            }

            start_at = EventStart::Cursor(page.cursor);
        }

        out.sort_by(|a, b| (a.ledger, a.id.as_str()).cmp(&(b.ledger, b.id.as_str())));

        Ok(out)
    }

    async fn events_from_ledger_close_meta_range(
        &self,
        start_ledger: u64,
        end_ledger: u64,
    ) -> Result<Vec<LocalLedgerArchiveEvent>> {
        if start_ledger >= end_ledger {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let params = if let Some(cursor) = cursor.as_ref() {
                serde_json::json!({
                    "pagination": {
                        "cursor": cursor,
                        "limit": self.ledgers_limit
                    },
                    "xdrFormat": "base64"
                })
            } else {
                serde_json::json!({
                    "startLedger": start_ledger,
                    "pagination": {
                        "limit": self.ledgers_limit
                    },
                    "xdrFormat": "base64"
                })
            };

            let request = serde_json::json!({
                "jsonrpc": "2.0",
                "id": "mixer-archive-get-ledgers",
                "method": "getLedgers",
                "params": params
            });

            let response = self
                .http
                .post(&self.rpc_url)
                .json(&request)
                .send()
                .await
                .with_context(|| {
                    format!(
                        "getLedgers request failed for ledger range [{start_ledger}, {end_ledger})"
                    )
                })?
                .error_for_status()
                .with_context(|| {
                    format!(
                        "getLedgers HTTP error for ledger range [{start_ledger}, {end_ledger})"
                    )
                })?
                .json::<JsonRpcResponse<GetLedgersResult>>()
                .await
                .with_context(|| {
                    format!(
                        "getLedgers response decode failed for ledger range [{start_ledger}, {end_ledger})"
                    )
                })?;

            if let Some(error) = response.error {
                bail!(
                    "getLedgers RPC error for ledger range [{}, {}): code={}, message={}",
                    start_ledger,
                    end_ledger,
                    error.code,
                    error.message
                );
            }

            let result = response
                .result
                .context("getLedgers response missing result")?;

            let page_count = result.ledgers.len();
            let mut reached_end = false;

            for ledger in result.ledgers {
                let ledger_seq = u64::from(ledger.sequence);

                if ledger_seq >= end_ledger {
                    reached_end = true;
                    continue;
                }

                if ledger_seq < start_ledger {
                    continue;
                }

                let meta = LedgerCloseMeta::from_xdr_base64(&ledger.metadata_xdr, Limits::none())
                    .with_context(|| {
                    format!(
                        "failed to decode LedgerCloseMeta for ledger {}",
                        ledger.sequence
                    )
                })?;

                let before = out.len();

                collect_mixer_archive_events_from_ledger_close_meta(
                    ledger.sequence,
                    &meta,
                    &self.contract_id_raw,
                    &mut out,
                )
                .with_context(|| {
                    format!(
                        "failed to extract mixer archive events from LedgerCloseMeta for ledger {}",
                        ledger.sequence
                    )
                })?;

                let found = out.len().saturating_sub(before);
                if found > 0 {
                    info!(
                        ledger = ledger.sequence,
                        events = found,
                        "extracted mixer archive events from typed LedgerCloseMeta"
                    );
                }
            }

            let next_cursor = result.cursor.unwrap_or_default();

            if reached_end || page_count < self.ledgers_limit || next_cursor.is_empty() {
                break;
            }

            cursor = Some(next_cursor);
        }

        out.sort_by(|a, b| (a.ledger, a.id.as_str()).cmp(&(b.ledger, b.id.as_str())));

        Ok(out)
    }
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetLedgersResult {
    ledgers: Vec<RpcLedger>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcLedger {
    sequence: u32,
    metadata_xdr: String,
}

fn collect_mixer_archive_events_from_ledger_close_meta(
    ledger: u32,
    meta: &LedgerCloseMeta,
    mixer_contract_id: &[u8; 32],
    out: &mut Vec<LocalLedgerArchiveEvent>,
) -> Result<()> {
    match meta {
        LedgerCloseMeta::V0(v0) => {
            for (tx_index, tx_meta) in v0.tx_processing.iter().enumerate() {
                collect_mixer_archive_events_from_transaction_meta(
                    ledger,
                    tx_index,
                    &tx_meta.tx_apply_processing,
                    mixer_contract_id,
                    out,
                )?;
            }
        }
        LedgerCloseMeta::V1(v1) => {
            for (tx_index, tx_meta) in v1.tx_processing.iter().enumerate() {
                collect_mixer_archive_events_from_transaction_meta(
                    ledger,
                    tx_index,
                    &tx_meta.tx_apply_processing,
                    mixer_contract_id,
                    out,
                )?;
            }
        }
        LedgerCloseMeta::V2(v2) => {
            for (tx_index, tx_meta) in v2.tx_processing.iter().enumerate() {
                collect_mixer_archive_events_from_transaction_meta(
                    ledger,
                    tx_index,
                    &tx_meta.tx_apply_processing,
                    mixer_contract_id,
                    out,
                )?;
            }
        }
    }

    Ok(())
}

fn collect_mixer_archive_events_from_transaction_meta(
    ledger: u32,
    tx_index: usize,
    tx_meta: &TransactionMeta,
    mixer_contract_id: &[u8; 32],
    out: &mut Vec<LocalLedgerArchiveEvent>,
) -> Result<()> {
    match tx_meta {
        TransactionMeta::V3(v3) => {
            if let Some(soroban_meta) = v3.soroban_meta.as_ref() {
                for (event_index, event) in soroban_meta.events.iter().enumerate() {
                    maybe_push_contract_event(
                        ledger,
                        format!("ledger-meta:{ledger}:tx:{tx_index}:soroban:{event_index}"),
                        event,
                        mixer_contract_id,
                        out,
                    )?;
                }
            }
        }
        TransactionMeta::V4(v4) => {
            for (op_index, op_meta) in v4.operations.iter().enumerate() {
                for (event_index, event) in op_meta.events.iter().enumerate() {
                    maybe_push_contract_event(
                        ledger,
                        format!(
                            "ledger-meta:{ledger}:tx:{tx_index}:op:{op_index}:event:{event_index}"
                        ),
                        event,
                        mixer_contract_id,
                        out,
                    )?;
                }
            }
        }
        TransactionMeta::V0(_) | TransactionMeta::V1(_) | TransactionMeta::V2(_) => {}
    }

    Ok(())
}

fn maybe_push_contract_event(
    ledger: u32,
    event_id: String,
    event: &ContractEvent,
    mixer_contract_id: &[u8; 32],
    out: &mut Vec<LocalLedgerArchiveEvent>,
) -> Result<()> {
    if !contract_event_matches_contract(event, mixer_contract_id) {
        return Ok(());
    }

    let parsed = parse_contract_event_fast(event)?;

    if parsed.is_empty() {
        return Ok(());
    }

    out.push(LocalLedgerArchiveEvent {
        id: event_id,
        ledger,
        parsed,
    });

    Ok(())
}

fn contract_event_matches_contract(event: &ContractEvent, mixer_contract_id: &[u8; 32]) -> bool {
    let Some(contract_id) = event.contract_id.as_ref() else {
        return false;
    };

    let hash: &stellar_xdr::Hash = contract_id.as_ref();
    hash.0 == *mixer_contract_id
}

fn parse_contract_event_fast(event: &ContractEvent) -> Result<Vec<MixerArchiveEvent>> {
    let ContractEventBody::V0(v0) = &event.body;

    if !topics_contain_symbol(&v0.topics, "mixer") {
        return Ok(Vec::new());
    }

    let symbols = topic_symbols_from_scvals(&v0.topics);
    let value = serde_json::to_value(&v0.data).context("failed to convert ScVal data to JSON")?;

    parse_mixer_archive_events_from_xdr_json(&symbols, &value)
}

fn topics_contain_symbol(topics: &[ScVal], expected: &str) -> bool {
    topics.iter().any(|topic| scval_symbol_eq(topic, expected))
}

fn topic_symbols_from_scvals(topics: &[ScVal]) -> Vec<String> {
    let mut out = Vec::new();

    for topic in topics {
        match topic {
            ScVal::Symbol(symbol) => {
                if let Ok(s) = std::str::from_utf8(symbol.as_slice()) {
                    out.push(s.to_string());
                }
            }
            ScVal::String(string) => {
                if let Ok(s) = std::str::from_utf8(string.as_slice()) {
                    out.push(s.to_string());
                }
            }
            _ => {
                if let Ok(value) = serde_json::to_value(topic) {
                    collect_symbols(&value, &mut out);
                }
            }
        }
    }

    out
}

fn scval_symbol_eq(value: &ScVal, expected: &str) -> bool {
    match value {
        ScVal::Symbol(symbol) => symbol.as_slice() == expected.as_bytes(),
        ScVal::String(string) => string.as_slice() == expected.as_bytes(),
        _ => false,
    }
}

fn parse_mixer_archive_events(event: &StellarEvent) -> Result<Vec<MixerArchiveEvent>> {
    let symbols = topic_symbols(&event.topic)?;
    let value = scval_json_from_base64(&event.value)
        .with_context(|| format!("failed to decode event value XDR for {}", event.id))?;

    parse_mixer_archive_events_from_xdr_json(&symbols, &value)
}

fn parse_mixer_archive_events_from_xdr_json(
    symbols: &[String],
    value: &Value,
) -> Result<Vec<MixerArchiveEvent>> {
    let has_symbol = |needle: &str| symbols.iter().any(|symbol| symbol == needle);

    let mut out = Vec::new();

    if has_symbol("encrypted_note") {
        if let Some(event) = try_parse_single_encrypted_note(
            value,
            &["index"],
            &["leaf"],
            &["encrypted_note", "ciphertext", "note"],
        )? {
            out.push(event);
        }

        return Ok(out);
    }

    if has_symbol("deposit") {
        if let Some(event) = try_parse_single_encrypted_note(
            value,
            &["index"],
            &["leaf"],
            &["encrypted_note", "ciphertext", "note"],
        )? {
            out.push(event);
        }
    }

    if has_symbol("withdraw") {
        if let Ok(nullifiers) = named_hash_vec(value, &["nullifiers"]) {
            out.push(MixerArchiveEvent::Nullifiers {
                nullifiers,
                source: "withdraw",
            });
        } else if let Ok(nullifier) = named_hash(value, &["nullifier"]) {
            out.push(MixerArchiveEvent::Nullifiers {
                nullifiers: vec![nullifier],
                source: "withdraw",
            });
        }

        if let Some(event) = try_parse_single_encrypted_note(
            value,
            &["index", "change_index"],
            &["output_leaf", "change_leaf", "leaf"],
            &["encrypted_note", "ciphertext", "note"],
        )? {
            out.push(event);
        }
    }

    if has_symbol("transfer") {
        if let Ok(nullifiers) = named_hash_vec(value, &["nullifiers"]) {
            out.push(MixerArchiveEvent::Nullifiers {
                nullifiers,
                source: "transfer",
            });
        }

        if let Ok(encrypted_notes) = named_bytes_vec(value, &["encrypted_notes"]) {
            let start_index = named_u64(value, &["start_index"])?;
            let output_leaves = named_hash_vec(value, &["output_leaves"])?;

            if encrypted_notes.len() != output_leaves.len() {
                bail!(
                    "transfer encrypted_notes/output_leaves length mismatch: {} != {}",
                    encrypted_notes.len(),
                    output_leaves.len()
                );
            }

            for (offset, (encrypted_note, leaf)) in encrypted_notes
                .into_iter()
                .zip(output_leaves.into_iter())
                .enumerate()
            {
                out.push(MixerArchiveEvent::EncryptedNote {
                    index: start_index + offset as u64,
                    leaf,
                    encrypted_note,
                });
            }
        } else if let Some(event) = try_parse_single_encrypted_note(
            value,
            &["index", "start_index"],
            &["leaf", "output_leaf"],
            &["encrypted_note", "ciphertext", "note"],
        )? {
            out.push(event);
        }
    }

    Ok(out)
}

fn try_parse_single_encrypted_note(
    value: &Value,
    index_names: &[&str],
    leaf_names: &[&str],
    encrypted_note_names: &[&str],
) -> Result<Option<MixerArchiveEvent>> {
    let Some(encrypted_note_value) = find_named_value(value, encrypted_note_names) else {
        return Ok(None);
    };

    let index = named_u64(value, index_names)?;
    let leaf = named_hash(value, leaf_names)?;
    let encrypted_note = json_to_bytes(encrypted_note_value)?;

    Ok(Some(MixerArchiveEvent::EncryptedNote {
        index,
        leaf,
        encrypted_note,
    }))
}

fn topic_symbols(topics: &[String]) -> Result<Vec<String>> {
    let mut out = Vec::new();

    for topic in topics {
        let value = scval_json_from_base64(topic).context("failed to decode event topic XDR")?;
        collect_symbols(&value, &mut out);
    }

    Ok(out)
}

fn scval_json_from_base64(value: &str) -> Result<Value> {
    let scval = ScVal::from_xdr_base64(value, Limits::none())
        .context("failed to decode Stellar ScVal base64 XDR")?;

    serde_json::to_value(scval).context("failed to convert ScVal to JSON")
}

fn collect_symbols(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(s) => {
            if matches!(
                s.as_str(),
                "mixer" | "init" | "deposit" | "withdraw" | "transfer" | "encrypted_note"
            ) {
                out.push(s.clone());
            }
        }
        Value::Object(map) => {
            for key in ["symbol", "sym"] {
                if let Some(symbol) = map.get(key).and_then(Value::as_str) {
                    out.push(symbol.to_string());
                }
            }

            for val in map.values() {
                collect_symbols(val, out);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_symbols(value, out);
            }
        }
        _ => {}
    }
}

fn named_u64(value: &Value, names: &[&str]) -> Result<u64> {
    let found =
        find_named_value(value, names).with_context(|| format!("missing u64 field {names:?}"))?;

    json_to_u64(found).with_context(|| format!("invalid u64 field {names:?}"))
}

fn named_hash(value: &Value, names: &[&str]) -> Result<[u8; 32]> {
    let bytes = named_bytes(value, names)?;

    if bytes.len() != 32 {
        bail!(
            "expected 32-byte hash for {names:?}, got {} bytes",
            bytes.len()
        );
    }

    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn named_hash_vec(value: &Value, names: &[&str]) -> Result<Vec<[u8; 32]>> {
    let found = find_named_value(value, names)
        .with_context(|| format!("missing hash vec field {names:?}"))?;

    json_to_hash_vec(found).with_context(|| format!("invalid hash vec field {names:?}"))
}

fn named_bytes(value: &Value, names: &[&str]) -> Result<Vec<u8>> {
    let found =
        find_named_value(value, names).with_context(|| format!("missing bytes field {names:?}"))?;

    json_to_bytes(found).with_context(|| format!("invalid bytes field {names:?}"))
}

fn named_bytes_vec(value: &Value, names: &[&str]) -> Result<Vec<Vec<u8>>> {
    let found = find_named_value(value, names)
        .with_context(|| format!("missing bytes vec field {names:?}"))?;

    json_to_bytes_vec(found).with_context(|| format!("invalid bytes vec field {names:?}"))
}

fn find_named_value<'a>(value: &'a Value, names: &[&str]) -> Option<&'a Value> {
    match value {
        Value::Object(map) => {
            for name in names {
                if let Some(v) = map.get(*name) {
                    return Some(v);
                }
            }

            if let (Some(key), Some(val)) =
                (map.get("key"), map.get("val").or_else(|| map.get("value")))
            {
                if json_key_matches(key, names) {
                    return Some(val);
                }
            }

            if let Some(entries) = map.get("map").and_then(Value::as_array) {
                for entry in entries {
                    if let Value::Object(entry_map) = entry {
                        if let (Some(key), Some(val)) = (
                            entry_map.get("key"),
                            entry_map.get("val").or_else(|| entry_map.get("value")),
                        ) {
                            if json_key_matches(key, names) {
                                return Some(val);
                            }
                        }
                    }
                }
            }

            for val in map.values() {
                if let Some(found) = find_named_value(val, names) {
                    return Some(found);
                }
            }

            None
        }
        Value::Array(values) => {
            for value in values {
                if let Some(found) = find_named_value(value, names) {
                    return Some(found);
                }
            }

            None
        }
        _ => None,
    }
}

fn json_key_matches(value: &Value, names: &[&str]) -> bool {
    match value {
        Value::String(s) => names.iter().any(|name| s == name),
        Value::Object(map) => {
            for key in ["symbol", "sym"] {
                if let Some(symbol) = map.get(key).and_then(Value::as_str) {
                    if names.iter().any(|name| symbol == *name) {
                        return true;
                    }
                }
            }

            map.values().any(|value| json_key_matches(value, names))
        }
        Value::Array(values) => values.iter().any(|value| json_key_matches(value, names)),
        _ => false,
    }
}

fn json_to_u64(value: &Value) -> Result<u64> {
    match value {
        Value::Number(n) => n.as_u64().context("number is not u64"),
        Value::String(s) => Ok(s.parse()?),
        Value::Object(map) => {
            for key in ["u64", "u32", "i64", "i32"] {
                if let Some(inner) = map.get(key) {
                    return json_to_u64(inner);
                }
            }

            if map.len() == 1 {
                if let Some(inner) = map.values().next() {
                    return json_to_u64(inner);
                }
            }

            for inner in map.values() {
                if let Ok(value) = json_to_u64(inner) {
                    return Ok(value);
                }
            }

            bail!("object is not u64-compatible")
        }
        _ => bail!("not u64-compatible"),
    }
}

fn json_to_hash_vec(value: &Value) -> Result<Vec<[u8; 32]>> {
    match value {
        Value::Array(values) => values.iter().map(json_to_hash).collect(),
        Value::Object(map) => {
            for key in ["vec", "Vec", "values"] {
                if let Some(Value::Array(values)) = map.get(key) {
                    return values.iter().map(json_to_hash).collect();
                }
            }

            if map.len() == 1 {
                if let Some(inner) = map.values().next() {
                    return json_to_hash_vec(inner);
                }
            }

            for inner in map.values() {
                if let Ok(values) = json_to_hash_vec(inner) {
                    return Ok(values);
                }
            }

            bail!("object is not hash vec")
        }
        _ => bail!("not hash vec"),
    }
}

fn json_to_hash(value: &Value) -> Result<[u8; 32]> {
    let bytes = json_to_bytes(value)?;

    if bytes.len() != 32 {
        bail!("expected 32 bytes, got {}", bytes.len());
    }

    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn json_to_bytes_vec(value: &Value) -> Result<Vec<Vec<u8>>> {
    match value {
        Value::Array(values) => values.iter().map(json_to_bytes).collect(),
        Value::Object(map) => {
            for key in ["vec", "Vec", "values"] {
                if let Some(Value::Array(values)) = map.get(key) {
                    return values.iter().map(json_to_bytes).collect();
                }
            }

            if map.len() == 1 {
                if let Some(inner) = map.values().next() {
                    return json_to_bytes_vec(inner);
                }
            }

            for inner in map.values() {
                if let Ok(values) = json_to_bytes_vec(inner) {
                    return Ok(values);
                }
            }

            bail!("object is not bytes vec")
        }
        _ => bail!("not bytes vec"),
    }
}

fn json_to_bytes(value: &Value) -> Result<Vec<u8>> {
    match value {
        Value::String(s) => decode_bytes_string(s),
        Value::Array(values) => {
            let mut out = Vec::with_capacity(values.len());

            for value in values {
                let byte = value
                    .as_u64()
                    .with_context(|| format!("array byte is not u64: {value}"))?;

                if byte > 255 {
                    bail!("array byte out of range: {byte}");
                }

                out.push(byte as u8);
            }

            Ok(out)
        }
        Value::Object(map) => {
            for key in ["bytes", "Bytes", "bytes_n", "bytesN", "value"] {
                if let Some(inner) = map.get(key) {
                    if let Ok(bytes) = json_to_bytes(inner) {
                        return Ok(bytes);
                    }
                }
            }

            if map.len() == 1 {
                if let Some(inner) = map.values().next() {
                    return json_to_bytes(inner);
                }
            }

            for inner in map.values() {
                if let Ok(bytes) = json_to_bytes(inner) {
                    return Ok(bytes);
                }
            }

            bail!("object is not bytes-compatible")
        }
        _ => bail!("not bytes-compatible"),
    }
}

fn decode_bytes_string(value: &str) -> Result<Vec<u8>> {
    let trimmed = value.trim();
    let hex_value = trimmed.strip_prefix("0x").unwrap_or(trimmed);

    if hex_value.len() % 2 == 0 && hex_value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(hex::decode(hex_value)?);
    }

    BASE64
        .decode(trimmed.as_bytes())
        .context("string is neither hex nor base64 bytes")
}

fn is_probably_transient_rpc_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();

    message.contains("sendrequest")
        || message.contains("error sending request")
        || message.contains("request failed")
        || message.contains("connection")
        || message.contains("connection reset")
        || message.contains("connection closed")
        || message.contains("unexpected eof")
        || message.contains("broken pipe")
        || message.contains("timeout")
        || message.contains("timed out")
        || message.contains("dns")
        || message.contains("tls")
        || message.contains("hyper")
        || message.contains("http error")
        || message.contains("429")
        || message.contains("502")
        || message.contains("503")
        || message.contains("504")
}

fn transient_rpc_backoff(failures: u32) -> Duration {
    match failures {
        0 | 1 => Duration::from_secs(2),
        2 => Duration::from_secs(4),
        3 => Duration::from_secs(8),
        4 => Duration::from_secs(16),
        _ => Duration::from_secs(30),
    }
}
