use std::net::TcpListener;

use clap::{arg, builder::PossibleValue, command, value_parser};
use kvs::{server, Result};
use log::{self, debug, error, warn};

fn main() -> Result<()> {
    env_logger::init();
    warn!("some warning");

    let matches = command!()
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            arg!(
                --addr <IP_PORT> "IP address of the server"
            )
            .required(false)
            .default_value("127.0.0.1:4000")
            .global(true)
            .id("ip")
            .value_parser(value_parser!(String)),
        )
        .arg(
            arg!(
                --engine <ENGINE_NAME> "Database engine name"
            )
            .required(false)
            .id("engine")
            .default_value("kvs")
            .global(true)
            .value_parser([PossibleValue::new("kvs"), PossibleValue::new("sled")]),
        )
        .get_matches();

    let ip = matches.get_one::<String>("ip").unwrap();
    debug!("Trying to listen on {}", ip);
    let listener = TcpListener::bind(ip)?;
    debug!("Listening on: {}", ip);

    if let Some(engine) = matches.get_one::<String>("engine") {
        debug!("current engine: {}", engine);
    }

    // accept connections and process them serially
    for stream in listener.incoming() {
        match stream {
            Ok(s) => server::handle_client_req(s),
            Err(e) => error!("failed to parse client request, err: {}", e),
        }
    }

    Ok(())
}
