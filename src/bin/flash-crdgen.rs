use std::process::ExitCode;

use heterocloud_flash::crd::{
    validated_crd, validated_gpu_device_crd, validated_gpu_job_crd, validated_usage_record_crd,
};

fn main() -> ExitCode {
    let kind = std::env::args().nth(1).unwrap_or_else(|| "service".into());
    let generated = match kind.as_str() {
        "service" => validated_crd(),
        "gpu-device" => validated_gpu_device_crd(),
        "gpu-job" => validated_gpu_job_crd(),
        "usage-record" => validated_usage_record_crd(),
        _ => Err(anyhow::anyhow!(
            "expected service, gpu-device, gpu-job, or usage-record"
        )),
    };
    match generated.and_then(|crd| Ok(serde_yaml::to_string(&crd)?)) {
        Ok(document) => {
            print!("{document}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("flash-crdgen: {error}");
            ExitCode::FAILURE
        }
    }
}
