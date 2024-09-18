use std::{
    env::{self, current_dir},
    fs,
    process::exit,
};

use clap::{arg, builder::PossibleValue, command, value_parser};
use kvs::{
    server::KvServer,
    thread_pool::{SharedQueueThreadPool, ThreadPool},
    KvStore, Result, SledKvsEngine,
};
use log::{self, debug, error, info};

fn main() -> Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info")
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

    let curr_engine = matches
        .get_one::<String>("engine")
        .expect("failed to parse --engine for server");

    // if there is an engine log file in the current directory, check if it aligns with
    // the engine provided to the server. if there is a mismatch between them, return an
    // error.
    //
    // Engine specified in 'engine' file must be the same as --engine flag value.
    let engine_log = current_dir()?.join("engine");
    if engine_log.exists() {
        debug!("engine log exists");
        let engine = fs::read_to_string(engine_log).expect("failed to read engine log file");
        if engine != *curr_engine {
            error!("Wrong engine!");
            exit(1);
        }
    }

    // write engine to engine file
    fs::write(current_dir()?.join("engine"), format!("{}", curr_engine))?;

    debug!("DEBUGGING ENABLED");
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine {}", curr_engine);
    info!("Listening at {} ", ip.to_string());

    let pool = SharedQueueThreadPool::new(48).unwrap();

    let s = KvServer::new();
    s.start(ip.to_string(), pool)?;

    Ok(())
}
