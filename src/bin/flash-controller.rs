use std::{env, process::ExitCode};

use anyhow::{Context, Result};
use heterocloud_flash::reconcile::run_controller;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    install_crypto_provider();
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = ?error, "flash-controller stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let namespace =
        env::var("FLASH_WORKLOAD_NAMESPACE").context("FLASH_WORKLOAD_NAMESPACE is required")?;
    let client = kube::Client::try_default()
        .await
        .context("create Kubernetes client")?;
    run_controller(client, namespace).await
}

fn install_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _result = rustls::crypto::ring::default_provider().install_default();
    }
}
