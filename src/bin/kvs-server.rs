use std::{
    env::{self, current_dir},
    fs,
    process::exit,
};

use clap::{arg, builder::PossibleValue, command, value_parser};
use kvs::{
    server::KvServer,
    thread_pool::{SharedQueueThreadPool, ThreadPool},
    Result,
};
use log::{self, info};

fn main() -> Result<()> {
    if env::var("KVS_LOG").is_err() {
        env::set_var("KVS_LOG", "info")
    }

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

    info!("KV Store, version: {}", env!("CARGO_PKG_VERSION"));

    let ip = matches.get_one::<String>("ip").unwrap();

    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Listening at {} ", ip.to_string());

    let pool = SharedQueueThreadPool::new(48).unwrap();

    let s = KvServer::new();
    s.start(ip.to_string(), pool)?;

    Ok(())
}
