//! Optional isolated acceptance test. The caller creates and later removes a
//! synthetic bucket/proxy, passing only a private fixture file path.
use heterocloud_flash::backup_destinations::{S3Destination, put};
use serde_json::Value;
use uuid::Uuid;

#[tokio::test]
async fn registers_a_real_synthetic_destination() -> anyhow::Result<()> {
    let Ok(path) = std::env::var("FLASH_BACKUP_DESTINATION_E2E_FIXTURE") else {
        return Ok(());
    };
    let _ = rustls::crypto::ring::default_provider().install_default();
    let metadata = std::fs::metadata(&path)?;
    anyhow::ensure!(metadata.is_file(), "fixture must be a regular file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        anyhow::ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "fixture must be private"
        );
    }
    let fixture: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let input: S3Destination = serde_json::from_value(fixture["destination"].clone())?;
    anyhow::ensure!(
        input.bucket.starts_with("flash-target-e2e-") && input.prefix == "isolated-probe",
        "fixture bucket is not isolated"
    );
    let organization: Uuid = serde_json::from_value(fixture["organization_id"].clone())?;
    let id: Uuid = serde_json::from_value(fixture["id"].clone())?;
    let proxy_origin = fixture["proxy_origin"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing proxy"))?;
    let proxy_key: Vec<u8> = serde_json::from_value(fixture["proxy_key"].clone())?;
    let result = put(
        kube::Client::try_default().await?,
        organization,
        id,
        &input,
        proxy_origin,
        &proxy_key,
        "longhorn-system",
        "longhorn-static",
        "flash-backup-verification",
    )
    .await?;
    anyhow::ensure!(
        result.id == id && result.organization_id == organization,
        "fixture scope mismatch"
    );
    println!("registered synthetic destination");
    Ok(())
}
