use std::{
    io::{BufReader, BufWriter, Write},
    net::TcpStream,
};

use clap::{arg, command, value_parser, Command};
use kvs::{transport::Response, KvsError, Result};
use kvs_protocol::request::Request;
use kvs_protocol::serializer::serialize;
use log::debug;
use serde::Deserialize;

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
            .id("ip")
            .default_value("127.0.0.1:4000")
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

    let ip = matches.get_one::<String>("ip").unwrap();
    debug!("Trying to connect server on {}", ip);
    let read_stream = TcpStream::connect(ip)?;
    let response_reader = BufReader::new(&read_stream);

    let write_stream = read_stream.try_clone()?;
    let mut request_writer = BufWriter::new(&write_stream);
    debug!("Connected to server on {}", ip);

    match matches.subcommand() {
        Some(("set", sub_m)) => {
            let key = sub_m.get_one::<String>("key").unwrap();
            let val = sub_m.get_one::<String>("val").unwrap();

            let serialized_cmd = serialize(&Request::Set {
                key: key.to_string(),
                val: val.to_string(),
            });

            request_writer.write_all(serialized_cmd.as_bytes())?;
            request_writer.flush()?;

            Ok(())
        }
        Some(("get", sub_m)) => {
            let key = sub_m.get_one::<String>("key").unwrap();

            let s = serialize(&Request::Get {
                key: key.to_string(),
            });
            request_writer.write_all(s.as_bytes())?;
            request_writer.flush()?;

            let mut de = serde_json::Deserializer::from_reader(response_reader);
            let resp = Response::deserialize(&mut de)?;
            if let Some(_e) = resp.error {
                println!("Key not found");
            } else {
                println!("{}", resp.result);
            }

            Ok(())
        }
        Some(("rm", sub_m)) => {
            let key = sub_m.get_one::<String>("key").unwrap();

            let s = serialize(&Request::Rm {
                key: key.to_string(),
            });
            request_writer.write_all(s.as_bytes())?;
            request_writer.flush()?;

            let mut de = serde_json::Deserializer::from_reader(response_reader);
            let resp = Response::deserialize(&mut de)?;
            if let Some(e) = resp.error {
                eprintln!("{}", e);
                return Err(KvsError::KeyNotFound);
            }

            Ok(())
        }
        _ => {
            eprintln!("unimplemented method, run `help`");
            std::process::exit(1);
        }
    }
}
