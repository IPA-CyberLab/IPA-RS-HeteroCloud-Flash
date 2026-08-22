use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;

use ipnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub const MAX_REPLICAS: u32 = 100;
pub const MAX_PORTS: usize = 16;
pub const MAX_ENVIRONMENT_VARIABLES: usize = 128;
pub const MAX_SOURCE_CIDRS: usize = 64;
pub const MAX_EFFECTIVE_SOURCE_CIDRS: usize = 4_096;
pub const MAX_CPU_MILLIS: u32 = 4_000;
pub const MAX_MEMORY_MIB: u32 = 8_128;
pub const MIN_EPHEMERAL_STORAGE_GIB: u32 = 1;
pub const MAX_EPHEMERAL_STORAGE_GIB: u32 = 10;
pub const DEFAULT_EPHEMERAL_STORAGE_GIB: u32 = 10;

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlashSpec {
    pub region: String,
    pub image: String,
    pub replicas: u32,
    pub cpu_millis: u32,
    pub memory_mib: u32,
    #[serde(default = "default_ephemeral_storage_gib")]
    #[schemars(range(min = 1, max = 10))]
    pub ephemeral_storage_gib: u32,
    pub ports: Vec<FlashPort>,
    pub exposure: FlashExposure,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

impl FlashSpec {
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_text("region", &self.region, 64)?;
        if self.image.len() > 512
            || self.image.trim() != self.image
            || self.image.is_empty()
            || self.image.chars().any(char::is_whitespace)
        {
            return Err(ValidationError::Field(
                "image must be a non-empty container reference of at most 512 characters".into(),
            ));
        }
        if !(1..=MAX_REPLICAS).contains(&self.replicas) {
            return Err(ValidationError::Field(format!(
                "replicas must be between 1 and {MAX_REPLICAS}"
            )));
        }
        if !(10..=MAX_CPU_MILLIS).contains(&self.cpu_millis) {
            return Err(ValidationError::Field(format!(
                "cpu_millis must be between 10 and {MAX_CPU_MILLIS}"
            )));
        }
        if !(16..=MAX_MEMORY_MIB).contains(&self.memory_mib) {
            return Err(ValidationError::Field(format!(
                "memory_mib must be between 16 and {MAX_MEMORY_MIB}"
            )));
        }
        if !(MIN_EPHEMERAL_STORAGE_GIB..=MAX_EPHEMERAL_STORAGE_GIB)
            .contains(&self.ephemeral_storage_gib)
        {
            return Err(ValidationError::Field(format!(
                "ephemeral_storage_gib must be between {MIN_EPHEMERAL_STORAGE_GIB} and {MAX_EPHEMERAL_STORAGE_GIB}"
            )));
        }
        if self.ports.is_empty() || self.ports.len() > MAX_PORTS {
            return Err(ValidationError::Field(format!(
                "ports must contain between 1 and {MAX_PORTS} entries"
            )));
        }

        let mut names = BTreeSet::new();
        let mut published = BTreeSet::new();
        for port in &self.ports {
            validate_dns_label("port name", &port.name)?;
            if port.container_port == 0 || port.service_port == 0 {
                return Err(ValidationError::Field(
                    "container_port and service_port must be between 1 and 65535".into(),
                ));
            }
            if !names.insert(port.name.as_str()) {
                return Err(ValidationError::Field(format!(
                    "port name {:?} is duplicated",
                    port.name
                )));
            }
            if !published.insert((port.protocol, port.service_port)) {
                return Err(ValidationError::Field(format!(
                    "{} service port {} is duplicated",
                    port.protocol.as_kubernetes(),
                    port.service_port
                )));
            }
        }

        if self.exposure.kind == ExposureType::Internal
            && self.exposure.traffic_mode != TrafficMode::Forwarded
        {
            return Err(ValidationError::Field(
                "internal exposure requires forwarded traffic_mode".into(),
            ));
        }
        self.exposure.effective_source_networks()?;
        if self.env.len() > MAX_ENVIRONMENT_VARIABLES {
            return Err(ValidationError::Field(format!(
                "env must not contain more than {MAX_ENVIRONMENT_VARIABLES} entries"
            )));
        }
        for (name, value) in &self.env {
            validate_env_name(name)?;
            if value.len() > 32_768 {
                return Err(ValidationError::Field(format!(
                    "environment variable {name:?} exceeds 32768 characters"
                )));
            }
        }
        validate_string_list("command", &self.command, 128)?;
        validate_string_list("args", &self.args, 256)?;
        Ok(())
    }
}

const fn default_ephemeral_storage_gib() -> u32 {
    DEFAULT_EPHEMERAL_STORAGE_GIB
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlashPort {
    pub name: String,
    pub protocol: TransportProtocol,
    pub container_port: u16,
    pub service_port: u16,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "lowercase")]
pub enum TransportProtocol {
    Tcp,
    Udp,
}

impl TransportProtocol {
    #[must_use]
    pub const fn as_kubernetes(self) -> &'static str {
        match self {
            Self::Tcp => "TCP",
            Self::Udp => "UDP",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlashExposure {
    #[serde(rename = "type")]
    pub kind: ExposureType,
    pub traffic_mode: TrafficMode,
    #[serde(default)]
    #[schemars(length(max = 64))]
    pub allowed_source_cidrs: Vec<String>,
    #[serde(default)]
    #[schemars(length(max = 64))]
    pub denied_source_cidrs: Vec<String>,
}

impl FlashExposure {
    pub fn source_networks(&self) -> Result<(Vec<IpNet>, Vec<IpNet>), ValidationError> {
        Ok((
            parse_source_networks("allowed_source_cidrs", &self.allowed_source_cidrs)?,
            parse_source_networks("denied_source_cidrs", &self.denied_source_cidrs)?,
        ))
    }

    pub fn effective_source_networks(&self) -> Result<Vec<IpNet>, ValidationError> {
        let (allowed, denied) = self.source_networks()?;
        let mut effective = if allowed.is_empty() {
            vec![
                "0.0.0.0/0"
                    .parse::<IpNet>()
                    .map_err(|_| ValidationError::Field("invalid IPv4 root network".into()))?,
                "::/0"
                    .parse::<IpNet>()
                    .map_err(|_| ValidationError::Field("invalid IPv6 root network".into()))?,
            ]
        } else {
            IpNet::aggregate(&allowed)
        };
        for denied_network in denied {
            let mut next = Vec::new();
            for allowed_network in effective {
                subtract_network(allowed_network, denied_network, &mut next)?;
                if next.len() > MAX_EFFECTIVE_SOURCE_CIDRS {
                    return Err(ValidationError::Field(format!(
                        "source access policy expands beyond {MAX_EFFECTIVE_SOURCE_CIDRS} CIDRs"
                    )));
                }
            }
            effective = IpNet::aggregate(&next);
        }
        Ok(effective)
    }

    #[must_use]
    pub fn has_source_policy(&self) -> bool {
        !self.allowed_source_cidrs.is_empty() || !self.denied_source_cidrs.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ExposureType {
    Internal,
    Public,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TrafficMode {
    Forwarded,
    Direct,
}

fn validate_text(name: &str, value: &str, maximum: usize) -> Result<(), ValidationError> {
    if value.trim() != value || value.is_empty() || value.len() > maximum {
        return Err(ValidationError::Field(format!(
            "{name} must contain between 1 and {maximum} trimmed characters"
        )));
    }
    Ok(())
}

fn parse_source_networks(field: &str, values: &[String]) -> Result<Vec<IpNet>, ValidationError> {
    if values.len() > MAX_SOURCE_CIDRS {
        return Err(ValidationError::Field(format!(
            "{field} must contain at most {MAX_SOURCE_CIDRS} entries"
        )));
    }
    let mut networks = BTreeSet::new();
    for value in values {
        if value.is_empty() || value.trim() != value {
            return Err(ValidationError::Field(format!(
                "{field} entries must be trimmed IP addresses or CIDRs"
            )));
        }
        let network = value
            .parse::<IpNet>()
            .or_else(|_| value.parse::<IpAddr>().map(IpNet::from))
            .map_err(|_| {
                ValidationError::Field(format!(
                    "{field} entry {value:?} must be an IPv4/IPv6 address or CIDR"
                ))
            })?
            .trunc();
        if !networks.insert(network) {
            return Err(ValidationError::Field(format!(
                "{field} must not contain duplicate networks"
            )));
        }
    }
    Ok(networks.into_iter().collect())
}

fn subtract_network(
    allowed: IpNet,
    denied: IpNet,
    output: &mut Vec<IpNet>,
) -> Result<(), ValidationError> {
    if denied.contains(&allowed) {
        return Ok(());
    }
    if !allowed.contains(&denied) {
        output.push(allowed);
        return Ok(());
    }
    let child_prefix = allowed.prefix_len().checked_add(1).ok_or_else(|| {
        ValidationError::Field("source access policy prefix cannot be subdivided".into())
    })?;
    let children = allowed.subnets(child_prefix).map_err(|_| {
        ValidationError::Field("source access policy prefix cannot be subdivided".into())
    })?;
    for child in children {
        subtract_network(child, denied, output)?;
    }
    Ok(())
}

fn validate_dns_label(name: &str, value: &str) -> Result<(), ValidationError> {
    let valid = !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric);
    if !valid {
        return Err(ValidationError::Field(format!(
            "{name} must be a lowercase DNS label"
        )));
    }
    Ok(())
}

fn validate_env_name(value: &str) -> Result<(), ValidationError> {
    let mut bytes = value.bytes();
    let valid_start = bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_');
    if !valid_start
        || value.len() > 253
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(ValidationError::Field(format!(
            "environment variable name {value:?} is invalid"
        )));
    }
    Ok(())
}

fn validate_string_list(
    name: &str,
    values: &[String],
    maximum: usize,
) -> Result<(), ValidationError> {
    if values.len() > maximum
        || values
            .iter()
            .any(|value| value.is_empty() || value.len() > 32_768)
    {
        return Err(ValidationError::Field(format!(
            "{name} must contain at most {maximum} non-empty entries"
        )));
    }
    Ok(())
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ValidationError {
    #[error("{0}")]
    Field(String),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        ExposureType, FlashExposure, FlashPort, FlashSpec, TrafficMode, TransportProtocol,
    };

    fn valid_spec() -> FlashSpec {
        FlashSpec {
            region: "heteronet-global".into(),
            image: "ghcr.io/example/udp-server:v1".into(),
            replicas: 3,
            cpu_millis: 500,
            memory_mib: 256,
            ephemeral_storage_gib: 10,
            ports: vec![FlashPort {
                name: "game-udp".into(),
                protocol: TransportProtocol::Udp,
                container_port: 7777,
                service_port: 7777,
            }],
            exposure: FlashExposure {
                kind: ExposureType::Public,
                traffic_mode: TrafficMode::Forwarded,
                allowed_source_cidrs: Vec::new(),
                denied_source_cidrs: Vec::new(),
            },
            env: BTreeMap::new(),
            command: Vec::new(),
            args: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn accepts_udp_service() {
        assert!(valid_spec().validate().is_ok());
    }

    #[test]
    fn defaults_and_bounds_ephemeral_storage() -> Result<(), Box<dyn std::error::Error>> {
        let mut value = serde_json::to_value(valid_spec())?;
        value
            .as_object_mut()
            .ok_or("Flash spec must be an object")?
            .remove("ephemeral_storage_gib");
        let defaulted = serde_json::from_value::<FlashSpec>(value)?;
        assert_eq!(defaulted.ephemeral_storage_gib, 10);

        let mut oversized = valid_spec();
        oversized.ephemeral_storage_gib = 11;
        assert!(oversized.validate().is_err());
        Ok(())
    }

    #[test]
    fn rejects_duplicate_protocol_and_service_port() {
        let mut spec = valid_spec();
        spec.ports.push(FlashPort {
            name: "other".into(),
            protocol: TransportProtocol::Udp,
            container_port: 8888,
            service_port: 7777,
        });
        assert!(spec.validate().is_err());
    }

    #[test]
    fn internal_service_cannot_claim_direct_routing() {
        let mut spec = valid_spec();
        spec.exposure.kind = ExposureType::Internal;
        spec.exposure.traffic_mode = TrafficMode::Direct;
        assert!(spec.validate().is_err());
    }

    #[test]
    fn source_policy_accepts_addresses_and_applies_denies_first() {
        let mut spec = valid_spec();
        spec.exposure.allowed_source_cidrs = vec!["192.0.2.0/24".into()];
        spec.exposure.denied_source_cidrs = vec!["192.0.2.128/25".into()];
        assert!(spec.validate().is_ok());
        assert_eq!(
            spec.exposure
                .effective_source_networks()
                .map(|values| values.into_iter().map(|value| value.to_string()).collect()),
            Ok(vec!["192.0.2.0/25".to_string()])
        );
    }

    #[test]
    fn source_policy_rejects_invalid_and_duplicate_networks() {
        let mut spec = valid_spec();
        spec.exposure.allowed_source_cidrs = vec!["not-an-ip".into()];
        assert!(spec.validate().is_err());

        let mut spec = valid_spec();
        spec.exposure.denied_source_cidrs = vec!["203.0.113.7".into(), "203.0.113.7/32".into()];
        assert!(spec.validate().is_err());
    }
}
