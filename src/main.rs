mod config;
mod indexer;

use anyhow::Result;
use config::ServerConfig;
use indexer::StellarMixerArchiveIndexer;
use mixer_archive_server::state_store::PersistentArchiveStore;
use mixer_archive_server::{app, SharedStore};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .or_else(|_| std::env::var("MIXER_ARCHIVE_LOG"))
                .unwrap_or_else(|_| "mixer_archive_server=info,info".to_string()),
        )
        .init();

    let config = ServerConfig::from_env()?;

    info!(?config, "starting mixer-archive-server");

    let store = PersistentArchiveStore::load_or_create(
        config.db_path.clone(),
        config.mixer_contract_id.clone(),
        config.start_ledger,
    )?;

    let store: SharedStore = Arc::new(RwLock::new(store));

    let mut indexer = StellarMixerArchiveIndexer::new(config.clone(), store.clone());

    indexer.catch_up_once().await?;

    let indexer_task = tokio::spawn(async move {
        if let Err(error) = indexer.run_forever().await {
            error!(%error, "mixer archive indexer stopped");
            std::process::exit(1);
        }
    });

    let api = app(store);

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!(addr = %config.bind_addr, "mixer-archive-server is listening");

    let server = axum::serve(listener, api).with_graceful_shutdown(shutdown_signal());

    tokio::select! {
        result = server => {
            result?;
        }
        result = indexer_task => {
            result?;
        }
    }

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
