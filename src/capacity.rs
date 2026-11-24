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
        self.evaluate(kind, object, recreate, None).await
    }

    pub async fn check_replicas(
        &self,
        mut template: Value,
        recreate: bool,
        replicas: u32,
    ) -> Result<Option<String>> {
        for field in ["containers", "initContainers"] {
            if let Some(containers) = template["spec"]
                .get_mut(field)
                .and_then(Value::as_array_mut)
            {
                for container in containers.iter_mut().filter_map(Value::as_object_mut) {
                    for key in ["env", "envFrom", "command", "args"] {
                        container.remove(key);
                    }
                }
            }
        }
        self.evaluate("Pod", template, recreate, Some(replicas))
            .await
    }

    async fn evaluate(
        &self,
        kind: &str,
        object: Value,
        recreate: bool,
        replicas: Option<u32>,
    ) -> Result<Option<String>> {
        let mut request = json!({"kind":kind,"object":object,"recreate":recreate});
        if let Some(count) = replicas {
            request["replicas"] = json!(count);
        }
        let response = self
            .client
            .post(self.origin.join("preflight")?)
            .json(&request)
            .send()
            .await?;
        if !response.status().is_success() {
            return Ok(Some("capacity_metrics_unavailable".into()));
        }
        let value: Value = response.json().await?;
        if replicas.is_some_and(|count| value["checked_replicas"].as_u64() != Some(count.into())) {
            return Ok(Some("capacity_metrics_unavailable".into()));
        }
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
