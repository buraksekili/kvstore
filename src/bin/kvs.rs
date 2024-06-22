use clap::{arg, command, value_parser, Command};

fn main() {
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
            eprintln!("unimplemented");
            std::process::exit(1);
            // let key = sub_m.get_one::<String>("key").unwrap();
            // let val = sub_m.get_one::<String>("val").unwrap();
            // println!("Setting {key}={val}");
        }
        Some(("get", sub_m)) => {
            eprintln!("unimplemented");
            std::process::exit(1);
            // let key = sub_m.get_one::<String>("key").unwrap();
            // println!("Getting {key}");
        }
        Some(("rm", sub_m)) => {
            eprintln!("unimplemented");
            std::process::exit(1);
            // let key = sub_m.get_one::<String>("key").unwrap();
            // println!("Removing {key}");
        }
        _ => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
    }

    // if let Some(matches) = matches.subcommand_matches("set") {
    //     // "$ myapp test" was run
    //     if matches.args_present() {
    //         let key = matches.get_one::<String>("key").unwrap();
    //         let val = matches.get_one::<String>("val").unwrap();
    //         println!("Setting {key}={val}");
    //     } else {
    //         println!("Not printing testing lists...");
    //     }
    // }
}
