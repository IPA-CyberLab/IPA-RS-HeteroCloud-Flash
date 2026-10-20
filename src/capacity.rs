use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::time::Duration;

#[derive(Clone)]
pub struct CapacityClient {
    client: reqwest::Client,
    origin: url::Url,
}

impl CapacityClient {
    pub fn from_env() -> Result<Option<Self>> {
        let Some(origin) = std::env::var("FLASH_CAPACITY_ENDPOINT")
            .ok()
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        let origin = url::Url::parse(&origin)?;
        anyhow::ensure!(
            origin.scheme() == "https"
                && origin.username().is_empty()
                && origin.password().is_none()
                && origin.query().is_none()
                && origin.fragment().is_none(),
            "capacity endpoint requires HTTPS"
        );
        let ca = std::fs::read(
            std::env::var("FLASH_CAPACITY_CA_FILE").context("capacity CA file is required")?,
        )?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .add_root_certificate(reqwest::Certificate::from_pem(&ca)?)
            .build()?;
        Ok(Some(Self { client, origin }))
    }

    pub async fn check(&self, kind: &str, object: Value, recreate: bool) -> Result<Option<String>> {
        let response = self
            .client
            .post(self.origin.join("preflight")?)
            .json(&json!({"kind":kind,"object":object,"recreate":recreate}))
            .send()
            .await?;
        if !response.status().is_success() {
            return Ok(Some("capacity_metrics_unavailable".into()));
        }
        let value: Value = response.json().await?;
        if value["allowed"] == true {
            Ok(None)
        } else {
            Ok(Some(
                value["reason"]
                    .as_str()
                    .unwrap_or("capacity_metrics_unavailable")
                    .to_owned(),
            ))
        }
    }

    pub async fn disk_usage(&self, id: &str) -> Option<crate::diagnostics::DiskUsage> {
        let response = self
            .client
            .post(self.origin.join("usage").ok()?)
            .json(&json!({"service_instance_id":id}))
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        response.json().await.ok()
    }
}
