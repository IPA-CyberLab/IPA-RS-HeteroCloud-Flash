//! Stable, bounded user-facing reasons; backend bodies and environment are excluded.
use k8s_openapi::api::core::v1::Pod;
use kube::ResourceExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct Diagnostic {
    pub code: String,
    pub severity: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct DiskUsage {
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub measured_at: String,
}

pub fn reason(code: &str) -> Diagnostic {
    let (severity, message) = match code {
        "insufficient_disk_capacity" => (
            "error",
            "Host disk capacity is insufficient for the image and writable storage.",
        ),
        "insufficient_replicated_storage" => (
            "error",
            "There is insufficient free storage for every persistent disk replica.",
        ),
        "insufficient_compute_capacity" => (
            "warning",
            "No eligible host has enough CPU or memory available.",
        ),
        "insufficient_inodes" => ("error", "The host filesystem has insufficient free inodes."),
        "capacity_metrics_unavailable" | "image_capacity_unknown" => (
            "warning",
            "Capacity could not be verified; the workload will be retried.",
        ),
        "vpc_pending" => ("warning", "Waiting for private network policies and DNS."),
        "oom_killed" => (
            "error",
            "The container exceeded its memory limit and was terminated.",
        ),
        "oom_recovered" => (
            "warning",
            "The container restarted after exceeding its memory limit.",
        ),
        "image_pull_failed" => (
            "error",
            "The container image could not be downloaded. Check the image and registry access.",
        ),
        "container_start_failed" => (
            "error",
            "The container process could not start or is repeatedly failing.",
        ),
        "disk_full" => (
            "error",
            "Persistent disk usage is at least 95%. Free space or expand the disk.",
        ),
        "disk_usage_high" => ("warning", "Persistent disk usage is at least 80%."),
        "disk_usage_critical" => ("error", "Persistent disk usage is at least 90%."),
        "deployment_failed" => (
            "error",
            "The deployment could not complete. The previous revision may still be serving.",
        ),
        _ => (
            "warning",
            "Waiting for an eligible host or workload readiness.",
        ),
    };
    Diagnostic {
        code: code.into(),
        severity: severity.into(),
        message: message.into(),
        resource: None,
    }
}

pub fn collect(
    pods: &[Pod],
    ready: bool,
    message: Option<&str>,
    disk: Option<&DiskUsage>,
) -> Vec<Diagnostic> {
    let mut output = Vec::new();
    for pod in pods.iter().filter(|p| {
        p.metadata.deletion_timestamp.is_none()
            && !matches!(
                p.status.as_ref().and_then(|s| s.phase.as_deref()),
                Some("Failed" | "Succeeded")
            )
    }) {
        let Some(status) = &pod.status else {
            continue;
        };
        for c in status
            .init_container_statuses
            .iter()
            .chain(status.container_statuses.iter())
            .flatten()
        {
            let state = c.state.as_ref();
            let terminated = state.and_then(|s| s.terminated.as_ref());
            let last_oom = c
                .last_state
                .as_ref()
                .and_then(|s| s.terminated.as_ref())
                .filter(|s| s.reason.as_deref() == Some("OOMKilled"));
            let waiting = state
                .and_then(|s| s.waiting.as_ref())
                .and_then(|s| s.reason.as_deref());
            let code = if terminated.is_some_and(|s| s.reason.as_deref() == Some("OOMKilled"))
                || (!ready && last_oom.is_some())
            {
                Some("oom_killed")
            } else if ready
                && last_oom.is_some_and(|s| {
                    s.finished_at.as_ref().is_some_and(|t| {
                        (0..86400).contains(
                            &chrono::Utc::now()
                                .timestamp()
                                .saturating_sub(t.0.as_second()),
                        )
                    })
                })
            {
                Some("oom_recovered")
            } else if matches!(
                waiting,
                Some("ErrImagePull" | "ImagePullBackOff" | "InvalidImageName")
            ) {
                Some("image_pull_failed")
            } else if !ready
                && (terminated.is_some()
                    || matches!(
                        waiting,
                        Some(
                            "CrashLoopBackOff" | "CreateContainerConfigError" | "RunContainerError"
                        )
                    ))
            {
                Some("container_start_failed")
            } else {
                None
            };
            if let Some(code) = code {
                let mut item = reason(code);
                item.resource = Some(pod.name_any());
                output.push(item);
            }
        }
    }
    if let Some(disk) = disk.filter(|d| d.capacity_bytes > 0) {
        let used = u128::from(disk.used_bytes) * 100 / u128::from(disk.capacity_bytes);
        if used >= 80 {
            output.push(reason(if used >= 95 {
                "disk_full"
            } else if used >= 90 {
                "disk_usage_critical"
            } else {
                "disk_usage_high"
            }));
        }
    }
    if !ready && output.is_empty() {
        let message = message.unwrap_or_default();
        let code = [
            "insufficient_disk_capacity",
            "insufficient_replicated_storage",
            "insufficient_compute_capacity",
            "insufficient_inodes",
            "capacity_metrics_unavailable",
            "image_capacity_unknown",
        ]
        .into_iter()
        .find(|code| message.contains(code));
        if let Some(code) = code {
            output.push(reason(code));
        } else if message.contains("VPC") {
            output.push(reason("vpc_pending"));
        } else if message.contains("cannot complete") {
            output.push(reason("deployment_failed"));
        }
    }
    output.truncate(16);
    output
}
