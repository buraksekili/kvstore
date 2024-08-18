use std::{env::current_dir, f32::consts::E, path::PathBuf};

use clap::{arg, command, value_parser, Command};
use kvs::{KvStore, KvsError, Result};

fn main() -> Result<()> {
    let matches = command!()
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            Command::new("set")
                .about("Set the value of a string key to a string")
                .arg(
                    arg!(<KEY>)
                        .help("A string key")
                        .id("key")
                        .required(true)
                        .value_parser(value_parser!(String)),
                )
                .arg(
                    arg!(<VALUE>)
                        .help("A string value")
                        .id("val")
                        .required(true)
                        .value_parser(value_parser!(String)),
                ),
        )
        .subcommand(
            Command::new("get")
                .about("Get the string value of a given string key")
                .arg(
                    arg!(<KEY>)
                        .help("A string key to fetch from in-memory db")
                        .id("key")
                        .required(true)
                        .value_parser(value_parser!(String)),
                ),
        )
        .subcommand(
            Command::new("rm").about("Remove a given key").arg(
                arg!(<KEY>)
                    .help("A string key to delete from in-memory db")
                    .id("key")
                    .required(true)
                    .value_parser(value_parser!(String)),
            ),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("set", sub_m)) => {
            let key = sub_m.get_one::<String>("key").unwrap();
            let val = sub_m.get_one::<String>("val").unwrap();

            KvStore::open(current_dir()?)?.set(key.into(), val.into())
        }
        Some(("get", sub_m)) => {
            // let key = sub_m.get_one::<String>("key").unwrap();
            // match KvStore::new()?.get(key.to_string()) {
            //     Ok(x) => println!("found {:?}", x),

            Ok(())
        }
        Some(("rm", sub_m)) => {
            let key = sub_m.get_one::<String>("key").unwrap();

            KvStore::open(current_dir()?)?.remove(key.into())
        }
        _ => {
            eprintln!("unimplemented method, run `help`");
            std::process::exit(1);
        }
    }
}
