use std::env::current_dir;

use clap::{arg, builder::PossibleValue, command, value_parser};
use kvs::{server::KvServer, KvStore, Result, SledKvsEngine};
use log::{self, debug, info, LevelFilter};

fn main() -> Result<()> {
    env_logger::builder().filter_level(LevelFilter::Info).init();

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
    info!("current engine: {}", curr_engine);

    if curr_engine == "sled" {
        debug!("using sled engine");
        let server = KvServer::new(
            SledKvsEngine::new(sled::open(current_dir()?)?),
            ip.to_string(),
        );

        server.start()?;
    } else {
        debug!("using kvs engine");
        let server = KvServer::new(KvStore::open(current_dir()?)?, ip.to_string());

        server.start()?;
    }

    Ok(())
}
