use std::{
    env::current_dir,
    io::{BufReader, BufWriter, Write},
    net::TcpStream,
    process::exit,
};

use clap::{arg, command, value_parser, Command};
use kvs::{
    transport::{Request, Response},
    KvStore, KvsError, Result,
};
use log::{debug, error, info};
use serde::Deserialize;
use serde_json::Deserializer;

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
    let mut response_reader = BufReader::new(&read_stream);

    let write_stream = read_stream.try_clone()?;
    let mut request_writer = BufWriter::new(&write_stream);
    debug!("Connected to server on {}", ip);

    match matches.subcommand() {
        Some(("set", sub_m)) => {
            let key = sub_m.get_one::<String>("key").unwrap();
            let val = sub_m.get_one::<String>("val").unwrap();

            match serde_json::to_writer(
                &mut request_writer,
                &Request::Set {
                    key: key.to_string(),
                    val: val.to_string(),
                },
            ) {
                Err(e) => error!("failed to serialize SET request, err: {}", e),
                Ok(_) => debug!("serialized the SET request"),
            };

            request_writer.flush()?;

            Ok(())
        }
        Some(("get", sub_m)) => {
            let key = sub_m.get_one::<String>("key").unwrap();

            match serde_json::to_writer(
                &mut request_writer,
                &Request::Get {
                    key: key.to_string(),
                },
            ) {
                Err(e) => error!("failed to serialize GET request, err: {}", e),
                Ok(_) => debug!("serialized the GET request"),
            };
            request_writer.flush()?;

            debug!("waiting to receive");
            // It is expected that the input stream ends after the deserialized object.
            // If the stream does not end, such as in the case of a persistent socket connection,
            // this function will not return.
            // It is possible instead to deserialize from a prefix of an input stream without
            // looking for EOF by managing your own Deserializer.
            // reference:
            //  -   https://docs.rs/serde_json/latest/serde_json/fn.from_reader.html
            //  -   https://github.com/serde-rs/json/issues/522
            let mut de = serde_json::Deserializer::from_reader(response_reader);
            debug!("1");
            let resp = Response::deserialize(&mut de)?;

            if let Some(e) = resp.Error {
                eprintln!("Error: {}", e);
                return Err(KvsError::JSONParser(e));
            }

            println!("{}", resp.Result);

            Ok(())
        }
        Some(("rm", sub_m)) => {
            let key = sub_m.get_one::<String>("key").unwrap();

            match serde_json::to_writer(
                &mut request_writer,
                &Request::Rm {
                    key: key.to_string(),
                },
            ) {
                Err(e) => error!("failed to serialize RM request, err: {}", e),
                Ok(_) => debug!("serialized the RM request"),
            };

            request_writer.flush()?;

            Ok(())
        }
        _ => {
            eprintln!("unimplemented method, run `help`");
            std::process::exit(1);
        }
    }
}
