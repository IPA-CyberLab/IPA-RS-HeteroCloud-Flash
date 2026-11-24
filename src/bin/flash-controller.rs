use std::{collections::BTreeSet, env, process::ExitCode};

use anyhow::{Context, Result};
use heterocloud_flash::reconcile::{
    ControllerConfig, PlacementConfig, run_controller, validate_public_domain,
};
use ipnet::IpNet;
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
    let placement = PlacementConfig::from_env()?;
    let public_domain = optional("FLASH_PUBLIC_DOMAIN");
    if let Some(domain) = &public_domain {
        validate_public_domain(domain)?;
    }
    let additional_protected_networks = network_cidrs("FLASH_ADDITIONAL_PROTECTED_CIDRS_JSON")?;
    let dns_networks = network_cidrs("FLASH_DNS_CIDRS_JSON")?;
    let secret_manager_networks = network_cidrs("FLASH_SECRET_MANAGER_CIDRS_JSON")?;
    if dns_networks.is_empty() {
        anyhow::bail!("FLASH_DNS_CIDRS_JSON must contain at least one DNS service address");
    }
    let client = kube::Client::try_default()
        .await
        .context("create Kubernetes client")?;
    let activator_namespace =
        env::var("FLASH_ACTIVATOR_NAMESPACE").context("FLASH_ACTIVATOR_NAMESPACE is required")?;
    let activator_service =
        env::var("FLASH_ACTIVATOR_SERVICE").context("FLASH_ACTIVATOR_SERVICE is required")?;
    let activator_port = env::var("FLASH_ACTIVATOR_PORT")
        .context("FLASH_ACTIVATOR_PORT is required")?
        .parse::<u16>()
        .context("FLASH_ACTIVATOR_PORT must be a TCP port")?;
    if optional("FLASH_BACKUP_SNAPSHOT_CLASS").is_some() {
        let backup_client = client.clone();
        let backup_namespace = namespace.clone();
        tokio::spawn(async move {
            loop {
                if heterocloud_flash::backups::run_controller(
                    backup_client.clone(),
                    backup_namespace.clone(),
                )
                .await
                .is_err()
                {
                    tracing::warn!("backup controller disconnected; retrying");
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        });
    }
    run_controller(
        client,
        ControllerConfig {
            placement,
            namespace,
            additional_protected_networks,
            dns_networks,
            secret_manager_networks,
            vpc_guard_networks: network_cidrs("FLASH_VPC_GUARD_CIDRS_JSON")?,
            public_domain,
            activator_namespace,
            activator_service,
            activator_port,
        },
    )
    .await
}

fn network_cidrs(name: &str) -> Result<Vec<IpNet>> {
    let values = optional(name)
        .map(|value| serde_json::from_str::<Vec<String>>(&value))
        .transpose()
        .with_context(|| format!("{name} must be a JSON string array"))?
        .unwrap_or_default();
    let mut networks = BTreeSet::new();
    for value in values {
        let network = value
            .parse::<IpNet>()
            .with_context(|| format!("{name} contains invalid CIDR {value}"))?
            .trunc();
        if !networks.insert(network) {
            anyhow::bail!("{name} contains duplicate CIDR {network}");
        }
    }
    Ok(networks.into_iter().collect())
}

fn optional(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn install_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _result = rustls::crypto::ring::default_provider().install_default();
    }
}
