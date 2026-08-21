use std::process::ExitCode;

use heterocloud_flash::crd::FlashService;
use kube::CustomResourceExt;

fn main() -> ExitCode {
    match serde_yaml::to_string(&FlashService::crd()) {
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
