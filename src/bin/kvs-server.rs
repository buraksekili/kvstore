use std::{env::current_dir, process::exit};

use clap::{arg, command, value_parser, Command};
use kvs::{KvStore, KvsError, Result};

fn main() -> Result<()> {
    command!()
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            arg!(
                --addr <IP_PORT> "IP address of the server"
            )
            .required(false)
            .global(true)
            .value_parser(value_parser!(String)),
        )
        .arg(
            arg!(
                --engine <ENGINE_NAME> "Database engine name"
            )
            .required(false)
            .default_value("kvs")
            .global(true)
            .value_parser(value_parser!(String)),
        )
        .get_matches();

    Ok(())
}
