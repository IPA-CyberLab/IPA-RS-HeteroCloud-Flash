use std::{
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use heterocloud_flash::domain::FlashSpec;
use reqwest::{Client, Method};
use serde_json::{Value, json};
use url::Url;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(version, about = "Manage HeteroCloud Flash services")]
struct Cli {
    #[arg(
        long,
        env = "HETEROCLOUD_API_URL",
        default_value = "https://heterocloud.mizuame.app/api/v1/"
    )]
    api_url: Url,

    #[arg(long, env = "HETEROCLOUD_ORGANIZATION_ID")]
    organization_id: Uuid,

    #[arg(long, env = "HETEROCLOUD_API_TOKEN_FILE")]
    token_file: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    List {
        #[arg(long)]
        project_id: Option<Uuid>,
    },
    Get {
        service_id: Uuid,
    },
    Create {
        #[arg(long)]
        project_id: Uuid,
        #[arg(long)]
        name: String,
        #[arg(long)]
        spec: PathBuf,
    },
    Update {
        service_id: Uuid,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        spec: Option<PathBuf>,
    },
    Delete {
        service_id: Uuid,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    install_crypto_provider();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("flashctl: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let token = read_token(&cli.token_file)?;
    let base = flash_collection_url(&cli.api_url, cli.organization_id)?;
    let client = Client::builder().build().context("build HTTP client")?;
    let (method, url, body) = match cli.command {
        Command::List { project_id } => {
            let mut url = base;
            if let Some(project_id) = project_id {
                url.query_pairs_mut()
                    .append_pair("project_id", &project_id.to_string());
            }
            (Method::GET, url, None)
        }
        Command::Get { service_id } => (Method::GET, resource_url(&base, service_id)?, None),
        Command::Create {
            project_id,
            name,
            spec,
        } => (
            Method::POST,
            base,
            Some(json!({
                "project_id": project_id,
                "name": name,
                "spec": read_spec(&spec)?,
            })),
        ),
        Command::Update {
            service_id,
            name,
            spec,
        } => {
            if name.is_none() && spec.is_none() {
                bail!("update requires --name or --spec");
            }
            let mut body = serde_json::Map::new();
            if let Some(name) = name {
                body.insert("name".into(), Value::String(name));
            }
            if let Some(path) = spec {
                body.insert("spec".into(), serde_json::to_value(read_spec(&path)?)?);
            }
            (
                Method::PUT,
                resource_url(&base, service_id)?,
                Some(Value::Object(body)),
            )
        }
        Command::Delete { service_id } => (Method::DELETE, resource_url(&base, service_id)?, None),
    };
    let mut request = client.request(method, url).bearer_auth(token);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.context("send management request")?;
    let status = response.status();
    let bytes = response.bytes().await.context("read management response")?;
    if !status.is_success() {
        let body = String::from_utf8_lossy(&bytes);
        bail!("HeteroCloud returned HTTP {status}: {body}");
    }
    if !bytes.is_empty() {
        let value: Value = serde_json::from_slice(&bytes).context("decode management response")?;
        println!("{}", serde_json::to_string_pretty(&value)?);
    }
    Ok(())
}

fn flash_collection_url(api_url: &Url, organization_id: Uuid) -> Result<Url> {
    let mut base = api_url.clone();
    if !base.path().ends_with('/') {
        let path = format!("{}/", base.path());
        base.set_path(&path);
    }
    base.join(&format!("organizations/{organization_id}/flash/services"))
        .context("construct Flash management URL")
}

fn resource_url(collection: &Url, service_id: Uuid) -> Result<Url> {
    let mut base = collection.clone();
    let path = format!("{}/", base.path().trim_end_matches('/'));
    base.set_path(&path);
    base.join(&service_id.to_string())
        .context("construct Flash service URL")
}

fn read_spec(path: &Path) -> Result<FlashSpec> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let spec: FlashSpec =
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
    spec.validate().context("validate Flash spec")?;
    Ok(spec)
}

fn read_token(path: &Path) -> Result<String> {
    let metadata = fs::metadata(path).with_context(|| format!("inspect {}", path.display()))?;
    if !metadata.is_file() {
        bail!("{} is not a regular file", path.display());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!(
                "{} must not be accessible by group or others",
                path.display()
            );
        }
    }
    let token = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let token = token.trim_end_matches(['\r', '\n']);
    if token.is_empty() || token.contains(['\r', '\n']) {
        bail!("{} does not contain one valid token", path.display());
    }
    Ok(token.to_owned())
}

fn install_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _result = rustls::crypto::ring::default_provider().install_default();
    }
}
