//! Block workload startup until this node has installed its fail-closed VPC guard.
use std::{net::IpAddr, time::Duration};
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let host: IpAddr = std::env::var("NODE_IP")?.parse()?;
    let uid = std::env::var("POD_UID")?;
    let mut url = url::Url::parse(&format!(
        "http://{}:18083/ready",
        std::net::SocketAddr::new(host, 18083).ip()
    ))?;
    url.query_pairs_mut().append_pair("pod_uid", &uid);
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(3))
        .build()?;
    loop {
        if let Ok(response) = client.get(url.clone()).send().await
            && response.status() == reqwest::StatusCode::NO_CONTENT
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
