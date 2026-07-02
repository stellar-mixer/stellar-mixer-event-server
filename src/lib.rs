use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use thiserror::Error;
use tokio::sync::RwLock;

pub mod state_store;

use state_store::{ArchiveMetadata, PersistentArchiveStore, StoredEncryptedNote, StoredNullifier};

pub const ENCRYPTED_NOTES_BATCH_SIZE: u64 = 100_000;
pub const NULLIFIERS_BATCH_SIZE: u64 = 1_000_000;

pub type SharedStore = Arc<RwLock<PersistentArchiveStore>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub service: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateResponse {
    pub contract_id: String,
    pub start_ledger: u64,
    pub last_indexed_ledger: u64,
    pub encrypted_note_count: u64,
    pub nullifier_count: u64,
    pub encrypted_notes_batch_size: u64,
    pub nullifiers_batch_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchQuery {
    /// First array index to return.
    ///
    /// The server returns the fixed-size batch:
    ///
    /// encrypted-notes: [index, index + 100_000)
    /// nullifiers:     [index, index + 1_000_000)
    ///
    /// `start` is accepted as a backward-compatible alias.
    pub index: Option<u64>,
    pub start: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedNotesResponse {
    pub index: u64,
    pub batch_size: u64,
    pub end_exclusive: u64,
    pub next_index: u64,
    pub total: u64,
    pub returned: u64,
    pub has_more: bool,
    pub items: Vec<StoredEncryptedNote>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NullifiersResponse {
    pub index: u64,
    pub batch_size: u64,
    pub end_exclusive: u64,
    pub next_index: u64,
    pub total: u64,
    pub returned: u64,
    pub has_more: bool,
    pub items: Vec<StoredNullifier>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("{0}")]
    Store(String),

    #[error("invalid batch query: {0}")]
    InvalidBatchQuery(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Store(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidBatchQuery(_) => StatusCode::BAD_REQUEST,
        };

        (
            status,
            Json(ErrorResponse {
                error: self.to_string(),
            }),
        )
            .into_response()
    }
}

pub fn app(store: SharedStore) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(state))
        .route("/v1/state", get(state))
        .route("/v1/encrypted-notes", get(encrypted_notes))
        .route("/v1/nullifiers", get(nullifiers))
        .with_state(store)
}

pub async fn run(
    addr: SocketAddr,
    store: SharedStore,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app(store)).await?;
    Ok(())
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        service: "mixer-archive-server",
    })
}

async fn state(State(store): State<SharedStore>) -> Json<StateResponse> {
    let store = store.read().await;
    Json(StateResponse::from_metadata(store.metadata()))
}

async fn encrypted_notes(
    State(store): State<SharedStore>,
    Query(query): Query<BatchQuery>,
) -> Result<Json<EncryptedNotesResponse>, ApiError> {
    let index = query_index(query)?;

    let store = store.read().await;
    let total = store.encrypted_note_count();

    let end_exclusive = index.saturating_add(ENCRYPTED_NOTES_BATCH_SIZE).min(total);

    let items = store
        .encrypted_notes_range(index, ENCRYPTED_NOTES_BATCH_SIZE)
        .map_err(|error| ApiError::Store(error.to_string()))?;

    let returned = items.len() as u64;
    let next_index = end_exclusive;
    let has_more = next_index < total;

    Ok(Json(EncryptedNotesResponse {
        index,
        batch_size: ENCRYPTED_NOTES_BATCH_SIZE,
        end_exclusive,
        next_index,
        total,
        returned,
        has_more,
        items,
    }))
}

async fn nullifiers(
    State(store): State<SharedStore>,
    Query(query): Query<BatchQuery>,
) -> Result<Json<NullifiersResponse>, ApiError> {
    let index = query_index(query)?;

    let store = store.read().await;
    let total = store.nullifier_count();

    let end_exclusive = index.saturating_add(NULLIFIERS_BATCH_SIZE).min(total);

    let items = store
        .nullifiers_range(index, NULLIFIERS_BATCH_SIZE)
        .map_err(|error| ApiError::Store(error.to_string()))?;

    let returned = items.len() as u64;
    let next_index = end_exclusive;
    let has_more = next_index < total;

    Ok(Json(NullifiersResponse {
        index,
        batch_size: NULLIFIERS_BATCH_SIZE,
        end_exclusive,
        next_index,
        total,
        returned,
        has_more,
        items,
    }))
}

fn query_index(query: BatchQuery) -> Result<u64, ApiError> {
    match (query.index, query.start) {
        (Some(index), None) => Ok(index),
        (None, Some(start)) => Ok(start),
        (None, None) => Ok(0),
        (Some(index), Some(start)) if index == start => Ok(index),
        (Some(_), Some(_)) => Err(ApiError::InvalidBatchQuery(
            "use either index or start, not both with different values".to_string(),
        )),
    }
}

impl StateResponse {
    pub fn from_metadata(metadata: &ArchiveMetadata) -> Self {
        Self {
            contract_id: metadata.contract_id.clone(),
            start_ledger: metadata.start_ledger,
            last_indexed_ledger: metadata.last_indexed_ledger,
            encrypted_note_count: metadata.encrypted_note_count,
            nullifier_count: metadata.nullifier_count,
            encrypted_notes_batch_size: ENCRYPTED_NOTES_BATCH_SIZE,
            nullifiers_batch_size: NULLIFIERS_BATCH_SIZE,
        }
    }
}
