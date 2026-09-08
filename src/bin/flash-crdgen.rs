use std::process::ExitCode;

use heterocloud_flash::crd::validated_crd;

fn main() -> ExitCode {
    match validated_crd().and_then(|crd| Ok(serde_yaml::to_string(&crd)?)) {
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
