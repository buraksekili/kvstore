use std::{env::current_dir, process::exit};

use clap::{arg, command, value_parser, Arg, Command};
use kvs::{KvStore, KvsError, Result};

fn main() -> Result<()> {
    let matches = command!()
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
        .subcommand(Command::new("t"))
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
        Some(("t", _sub_m)) => {
            let mut store = KvStore::open(current_dir()?)?;
            store.set("key1".to_owned(), "value1".to_owned())?;
            store.set("key2".to_owned(), "value2".to_owned())?;
            drop(store);

            if let Ok(result) = KvStore::open(current_dir()?)?.get("key1".to_string()) {
                if let Some(v) = result {
                    println!("{}", v);
                } else {
                    println!("Key not found for key1");
                }
            };

            Ok(())
        }
        Some(("set", sub_m)) => {
            let key = sub_m.get_one::<String>("key").unwrap();
            let val = sub_m.get_one::<String>("val").unwrap();

            KvStore::open(current_dir()?)?.set(key.into(), val.into())?;

            Ok(())
        }
        Some(("get", sub_m)) => {
            let key = sub_m.get_one::<String>("key").unwrap();
            if let Ok(result) = KvStore::open(current_dir()?)?.get(key.to_string()) {
                if let Some(v) = result {
                    println!("{}", v);
                } else {
                    println!("Key not found");
                }
            };

            Ok(())
        }
        Some(("rm", sub_m)) => {
            let key = sub_m.get_one::<String>("key").unwrap();

            match KvStore::open(current_dir()?)?.remove(key.into()) {
                Ok(()) => Ok(()),
                Err(KvsError::KeyNotFound) => {
                    print!("Key not found");
                    exit(1);
                }
                Err(e) => Err(e),
            }
        }
        _ => {
            eprintln!("unimplemented method, run `help`");
            std::process::exit(1);
        }
    }
}
