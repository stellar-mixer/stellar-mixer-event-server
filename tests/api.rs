use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use mixer_archive_server::state_store::PersistentArchiveStore;
use mixer_archive_server::{app, EncryptedNotesResponse, NullifiersResponse, StateResponse};
use rocksdb::{Options, DB};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tower::ServiceExt;

fn temp_state_path(name: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    std::env::temp_dir().join(format!(
        "mixer-archive-server-{name}-{}-{nonce}",
        std::process::id()
    ))
}

#[tokio::test]
async fn api_returns_state_and_fixed_batches() {
    let path = temp_state_path("api");
    let contract = "CONTRACT".to_string();

    let mut store =
        PersistentArchiveStore::load_or_create(path.clone(), contract.clone(), Some(123)).unwrap();

    store
        .append_encrypted_note_record(0, [1u8; 32], vec![1, 2, 3], "event-note-0", 123)
        .unwrap();
    store
        .append_encrypted_note_record(1, [2u8; 32], vec![4, 5, 6], "event-note-1", 124)
        .unwrap();
    store
        .append_nullifier_record([3u8; 32], "event-nullifier-0", 124, "withdraw")
        .unwrap();

    store.set_last_indexed_ledger(124);
    store.save().unwrap();

    let app = app(Arc::new(RwLock::new(store)));

    let state: StateResponse = get_json(app.clone(), "/v1/state").await;
    assert_eq!(state.encrypted_note_count, 2);
    assert_eq!(state.nullifier_count, 1);
    assert_eq!(state.last_indexed_ledger, 124);
    assert_eq!(state.encrypted_notes_batch_size, 100_000);
    assert_eq!(state.nullifiers_batch_size, 1_000_000);

    let notes: EncryptedNotesResponse = get_json(app.clone(), "/v1/encrypted-notes?index=0").await;

    assert_eq!(notes.index, 0);
    assert_eq!(notes.batch_size, 100_000);
    assert_eq!(notes.total, 2);
    assert_eq!(notes.returned, 2);
    assert_eq!(notes.end_exclusive, 2);
    assert_eq!(notes.next_index, 2);
    assert!(!notes.has_more);
    assert_eq!(notes.items.len(), 2);
    assert_eq!(notes.items[0].index, 0);
    assert_eq!(notes.items[1].index, 1);

    let empty_notes: EncryptedNotesResponse =
        get_json(app.clone(), "/v1/encrypted-notes?index=2").await;

    assert_eq!(empty_notes.index, 2);
    assert_eq!(empty_notes.total, 2);
    assert_eq!(empty_notes.returned, 0);
    assert_eq!(empty_notes.end_exclusive, 2);
    assert_eq!(empty_notes.next_index, 2);
    assert!(!empty_notes.has_more);
    assert!(empty_notes.items.is_empty());

    let nullifiers: NullifiersResponse = get_json(app.clone(), "/v1/nullifiers?index=0").await;

    assert_eq!(nullifiers.index, 0);
    assert_eq!(nullifiers.batch_size, 1_000_000);
    assert_eq!(nullifiers.total, 1);
    assert_eq!(nullifiers.returned, 1);
    assert_eq!(nullifiers.end_exclusive, 1);
    assert_eq!(nullifiers.next_index, 1);
    assert!(!nullifiers.has_more);
    assert_eq!(nullifiers.items.len(), 1);
    assert_eq!(nullifiers.items[0].index, 0);
    assert_eq!(nullifiers.items[0].source, "withdraw");

    drop(app);
    let _ = DB::destroy(&Options::default(), path);
}

#[tokio::test]
async fn start_alias_still_works_for_backward_compatibility() {
    let path = temp_state_path("start-alias");
    let contract = "CONTRACT".to_string();

    let mut store =
        PersistentArchiveStore::load_or_create(path.clone(), contract.clone(), Some(123)).unwrap();

    store
        .append_encrypted_note_record(0, [1u8; 32], vec![1, 2, 3], "event-note-0", 123)
        .unwrap();

    let app = app(Arc::new(RwLock::new(store)));

    let notes: EncryptedNotesResponse = get_json(app.clone(), "/v1/encrypted-notes?start=0").await;

    assert_eq!(notes.index, 0);
    assert_eq!(notes.returned, 1);

    drop(app);
    let _ = DB::destroy(&Options::default(), path);
}

#[tokio::test]
async fn rejects_conflicting_index_and_start() {
    let path = temp_state_path("bad-query");
    let contract = "CONTRACT".to_string();

    let store =
        PersistentArchiveStore::load_or_create(path.clone(), contract.clone(), Some(123)).unwrap();

    let app = app(Arc::new(RwLock::new(store)));

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/encrypted-notes?index=0&start=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let _ = DB::destroy(&Options::default(), path);
}

async fn get_json<T: serde::de::DeserializeOwned>(app: axum::Router, uri: &str) -> T {
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();

    serde_json::from_slice(&bytes).unwrap()
}
