use clap::{arg, builder::PossibleValue, command, value_parser};
use kvs::Result;
use log::{self, debug, error, info, warn, LevelFilter};

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

    if let Some(ip) = matches.get_one::<String>("ip") {
        info!("ip address: {}", ip);
    }

    if let Some(engine) = matches.get_one::<String>("engine") {
        info!("current engine: {}", engine);
    }

    Ok(())
}
