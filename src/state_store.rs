use anyhow::{bail, Context, Result};
use rocksdb::{Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

const META_KEY: &[u8] = b"meta";
const VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveMetadata {
    pub version: u32,
    pub contract_id: String,
    pub start_ledger: u64,
    pub last_indexed_ledger: u64,
    pub encrypted_note_count: u64,
    pub nullifier_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEncryptedNote {
    pub index: u64,
    pub leaf_hex: String,
    pub encrypted_note_base64: String,
    pub event_id: String,
    pub ledger: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredNullifier {
    pub index: u64,
    pub nullifier_hex: String,
    pub event_id: String,
    pub ledger: u64,
    pub source: String,
}

pub struct PersistentArchiveStore {
    path: PathBuf,
    db: DB,
    metadata: ArchiveMetadata,
}

impl std::fmt::Debug for PersistentArchiveStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistentArchiveStore")
            .field("path", &self.path)
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl PersistentArchiveStore {
    pub fn load_or_create(
        path: impl Into<PathBuf>,
        contract_id: String,
        start_ledger: Option<u64>,
    ) -> Result<Self> {
        let path = path.into();

        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_max_open_files(256);
        options.set_keep_log_file_num(8);

        let db = DB::open(&options, &path)
            .with_context(|| format!("failed to open RocksDB at {}", path.display()))?;

        if let Some(bytes) = db.get(META_KEY)? {
            let metadata: ArchiveMetadata = serde_json::from_slice(&bytes).with_context(|| {
                format!("failed to parse RocksDB metadata at {}", path.display())
            })?;

            if metadata.version != VERSION {
                bail!("unsupported state version {}", metadata.version);
            }

            if metadata.contract_id != contract_id {
                bail!(
                    "state DB belongs to contract {}, config points to {}",
                    metadata.contract_id,
                    contract_id
                );
            }

            return Ok(Self { path, db, metadata });
        }

        let start_ledger = start_ledger
            .context("MIXER_ARCHIVE_START_LEDGER is required when state DB does not exist")?;

        let metadata = ArchiveMetadata {
            version: VERSION,
            contract_id,
            start_ledger,
            last_indexed_ledger: start_ledger.saturating_sub(1),
            encrypted_note_count: 0,
            nullifier_count: 0,
        };

        let store = Self { path, db, metadata };
        store.save()?;

        Ok(store)
    }

    pub fn metadata(&self) -> &ArchiveMetadata {
        &self.metadata
    }

    pub fn last_indexed_ledger(&self) -> u64 {
        self.metadata.last_indexed_ledger
    }

    pub fn set_last_indexed_ledger(&mut self, ledger: u64) {
        self.metadata.last_indexed_ledger = ledger;
    }

    pub fn encrypted_note_count(&self) -> u64 {
        self.metadata.encrypted_note_count
    }

    pub fn nullifier_count(&self) -> u64 {
        self.metadata.nullifier_count
    }

    pub fn has_event_id(&self, event_id: &str) -> bool {
        self.db
            .get(event_key(event_id))
            .map(|value| value.is_some())
            .unwrap_or(false)
    }

    pub fn append_encrypted_note_record(
        &mut self,
        note_index: u64,
        leaf: [u8; 32],
        encrypted_note: Vec<u8>,
        event_id: &str,
        ledger: u64,
    ) -> Result<()> {
        if self.has_event_id(event_id) {
            return Ok(());
        }

        if note_index != self.metadata.encrypted_note_count {
            bail!(
                "encrypted note index mismatch: next={}, event_index={}",
                self.metadata.encrypted_note_count,
                note_index
            );
        }

        let stored = StoredEncryptedNote {
            index: note_index,
            leaf_hex: hex::encode(leaf),
            encrypted_note_base64: base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                encrypted_note,
            ),
            event_id: event_id.to_string(),
            ledger,
        };

        let mut next_metadata = self.metadata.clone();
        next_metadata.encrypted_note_count = next_metadata.encrypted_note_count.saturating_add(1);

        let mut batch = WriteBatch::default();
        batch.put(encrypted_note_key(note_index), serde_json::to_vec(&stored)?);
        batch.put(event_key(event_id), b"1");
        batch.put(META_KEY, serde_json::to_vec(&next_metadata)?);

        self.db.write(batch)?;
        self.metadata = next_metadata;

        Ok(())
    }

    pub fn append_nullifier_record(
        &mut self,
        nullifier: [u8; 32],
        event_id: &str,
        ledger: u64,
        source: &str,
    ) -> Result<()> {
        if self.has_event_id(event_id) {
            return Ok(());
        }

        let index = self.metadata.nullifier_count;

        let stored = StoredNullifier {
            index,
            nullifier_hex: hex::encode(nullifier),
            event_id: event_id.to_string(),
            ledger,
            source: source.to_string(),
        };

        let mut next_metadata = self.metadata.clone();
        next_metadata.nullifier_count = next_metadata.nullifier_count.saturating_add(1);

        let mut batch = WriteBatch::default();
        batch.put(nullifier_key(index), serde_json::to_vec(&stored)?);
        batch.put(event_key(event_id), b"1");
        batch.put(META_KEY, serde_json::to_vec(&next_metadata)?);

        self.db.write(batch)?;
        self.metadata = next_metadata;

        Ok(())
    }

    pub fn encrypted_notes_range(
        &self,
        start: u64,
        limit: u64,
    ) -> Result<Vec<StoredEncryptedNote>> {
        let end = start
            .saturating_add(limit)
            .min(self.metadata.encrypted_note_count);

        let mut out = Vec::with_capacity(end.saturating_sub(start) as usize);

        let mut index = start;
        while index < end {
            let Some(bytes) = self.db.get(encrypted_note_key(index))? else {
                bail!("missing encrypted note at index {index}");
            };

            let stored: StoredEncryptedNote = serde_json::from_slice(&bytes)
                .with_context(|| format!("failed to parse encrypted note at index {index}"))?;

            if stored.index != index {
                bail!(
                    "encrypted note key/index mismatch: key={}, value={}",
                    index,
                    stored.index
                );
            }

            out.push(stored);
            index += 1;
        }

        Ok(out)
    }

    pub fn nullifiers_range(&self, start: u64, limit: u64) -> Result<Vec<StoredNullifier>> {
        let end = start
            .saturating_add(limit)
            .min(self.metadata.nullifier_count);

        let mut out = Vec::with_capacity(end.saturating_sub(start) as usize);

        let mut index = start;
        while index < end {
            let Some(bytes) = self.db.get(nullifier_key(index))? else {
                bail!("missing nullifier at index {index}");
            };

            let stored: StoredNullifier = serde_json::from_slice(&bytes)
                .with_context(|| format!("failed to parse nullifier at index {index}"))?;

            if stored.index != index {
                bail!(
                    "nullifier key/index mismatch: key={}, value={}",
                    index,
                    stored.index
                );
            }

            out.push(stored);
            index += 1;
        }

        Ok(out)
    }

    pub fn save(&self) -> Result<()> {
        self.db.put(META_KEY, serde_json::to_vec(&self.metadata)?)?;
        self.db.flush()?;
        Ok(())
    }
}

fn encrypted_note_key(index: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(23);
    key.extend_from_slice(b"encrypted_note/");
    key.extend_from_slice(&index.to_be_bytes());
    key
}

fn nullifier_key(index: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(18);
    key.extend_from_slice(b"nullifier/");
    key.extend_from_slice(&index.to_be_bytes());
    key
}

fn event_key(event_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(6 + event_id.len());
    key.extend_from_slice(b"event/");
    key.extend_from_slice(event_id.as_bytes());
    key
}
