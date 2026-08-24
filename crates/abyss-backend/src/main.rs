//! HTTP entrypoint for the Abyss backend service.

#![expect(
    clippy::multiple_crate_versions,
    reason = "Axum and Diesel currently pull a few distinct transitive crate versions."
)]

mod api;
mod config;
mod db;
mod error;
mod identity;
mod search;
mod usage;

use std::{error::Error, time::Duration};

use config::Config;
use db::{create_pool, run_migrations};
use identity::IdentityAuthenticator;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let config = Config::from_env()?;
    validate_runtime_config(&config)?;
    init_tracing(&config);

    let pool = create_pool(&config)?;
    if config.run_migrations {
        run_migrations(&pool)?;
    }

    let search = config
        .search
        .as_ref()
        .map(search::SearchService::new)
        .transpose()?;
    let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(false);
    let search_worker = match (&search, config.search.as_ref()) {
        (Some(search), Some(search_config)) => Some(search::worker::SearchIndexer::spawn(
            pool.clone(),
            search.client(),
            search_config,
            shutdown_receiver,
        )),
        _ => None,
    };

    let address = config.addr;
    let app = api::router(api::AppState {
        environment: config.environment,
        max_ingest_batch_size: config.max_ingest_batch_size,
        summary_scan_limit: config.summary_scan_limit,
        default_page_size: config.default_page_size,
        identity: IdentityAuthenticator::new(config.identity),
        search,
        pool,
    });

    let listener = tokio::net::TcpListener::bind(&address).await?;
    tracing::info!(%address, "abyss-backend listening");
    let server_result = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal().await;
        if shutdown_sender.send(true).is_err() {
            tracing::trace!("session search indexer already stopped");
        }
    })
    .await;
    stop_search_worker(search_worker).await;
    server_result?;

    Ok(())
}

async fn stop_search_worker(worker: Option<tokio::task::JoinHandle<()>>) {
    let Some(mut worker) = worker else {
        return;
    };
    match tokio::time::timeout(Duration::from_secs(15), &mut worker).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(%error, "session search indexer task failed"),
        Err(_elapsed) => {
            tracing::warn!("session search indexer did not stop before timeout");
            worker.abort();
        }
    }
}

fn validate_runtime_config(config: &Config) -> Result<(), error::AppError> {
    if config.environment == "blackbox"
        && !config.addr.ip().is_loopback()
        && !config.blackbox_allow_non_loopback
    {
        return Err(error::AppError::config(
            "ABYSS_BACKEND_ENV=blackbox requires ABYSS_BACKEND_ADDR to bind a loopback address"
                .to_owned(),
        ));
    }
    Ok(())
}

fn init_tracing(config: &Config) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.log_level.clone()));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "failed to listen for shutdown signal");
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use crate::identity::IdentityConfig;

    use super::{Config, validate_runtime_config};

    #[test]
    fn blackbox_environment_requires_loopback_bind_address() {
        let error = validate_runtime_config(&test_config("blackbox", "0.0.0.0:8080"))
            .expect_err("blackbox should reject non-loopback binds");

        assert!(
            error
                .to_string()
                .contains("ABYSS_BACKEND_ENV=blackbox requires"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn blackbox_environment_allows_loopback_bind_address() {
        validate_runtime_config(&test_config("blackbox", "127.0.0.1:8080"))
            .expect("blackbox should allow loopback binds");
    }

    #[test]
    fn blackbox_environment_explicitly_allows_container_bind_address() {
        let mut config = test_config("blackbox", "0.0.0.0:8080");
        config.blackbox_allow_non_loopback = true;
        validate_runtime_config(&config)
            .expect("explicit blackbox Docker override should allow container bind");
    }

    fn test_config(environment: &str, addr: &str) -> Config {
        Config {
            addr: addr
                .parse::<SocketAddr>()
                .expect("test socket address should parse"),
            environment: environment.to_owned(),
            blackbox_allow_non_loopback: false,
            log_level: "info".to_owned(),
            database_url: "postgres://example.invalid/abyss".to_owned(),
            database_pool_size: 1,
            run_migrations: false,
            max_ingest_batch_size: 1,
            summary_scan_limit: 1,
            default_page_size: 1,
            identity: IdentityConfig::parse(&"0".repeat(64)).expect("test token hash should parse"),
            search: None,
        }
    }
}
