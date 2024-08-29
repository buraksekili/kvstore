use std::env::current_dir;

use clap::{arg, builder::PossibleValue, command, value_parser};
use kvs::{server::KvServer, KvStore, Result};
use log::{self, debug, warn};

fn main() -> Result<()> {
    env_logger::init();

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

    let server = KvServer::new(KvStore::open(current_dir()?)?, ip.to_string());

    if let Some(engine) = matches.get_one::<String>("engine") {
        debug!("current engine: {}", engine);
    }

    server.start()?;

    Ok(())
}
