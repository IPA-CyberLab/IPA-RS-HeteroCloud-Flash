use std::env;
use std::io::ErrorKind;
use std::net::{SocketAddr, UdpSocket};
use std::process::ExitCode;

const DEFAULT_LISTEN: &str = "0.0.0.0:7777";
const MAX_DATAGRAM_SIZE: usize = 65_535;

fn configured_listen_addr() -> Result<SocketAddr, String> {
    let value = match env::var("FLASH_ECHO_LISTEN") {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => DEFAULT_LISTEN.to_owned(),
        Err(env::VarError::NotUnicode(_)) => {
            return Err("FLASH_ECHO_LISTEN must be valid UTF-8".to_owned());
        }
    };

    if value.is_empty() {
        return Err("FLASH_ECHO_LISTEN must not be empty".to_owned());
    }

    value.parse::<SocketAddr>().map_err(|error| {
        format!(
            "invalid FLASH_ECHO_LISTEN {value:?}: expected an IP socket address such as 0.0.0.0:7777 ({error})"
        )
    })
}

fn run() -> Result<(), String> {
    let listen_addr = configured_listen_addr()?;
    let socket = UdpSocket::bind(listen_addr)
        .map_err(|error| format!("failed to bind UDP socket on {listen_addr}: {error}"))?;
    let bound_addr = socket
        .local_addr()
        .map_err(|error| format!("failed to read bound UDP address: {error}"))?;

    println!("HeteroCloud Flash UDP echo listening on {bound_addr}");

    let mut buffer = [0_u8; MAX_DATAGRAM_SIZE];
    loop {
        let (received, source) = match socket.recv_from(&mut buffer) {
            Ok(datagram) => datagram,
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => return Err(format!("failed to receive UDP datagram: {error}")),
        };

        let sent = socket
            .send_to(&buffer[..received], source)
            .map_err(|error| format!("failed to echo UDP datagram to {source}: {error}"))?;
        if sent != received {
            return Err(format!(
                "incomplete UDP echo to {source}: sent {sent} of {received} bytes"
            ));
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("flash-udp-echo: {error}");
            ExitCode::FAILURE
        }
    }
}
