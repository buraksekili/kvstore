use serde::{Deserialize, Serialize};

use crate::{KvsError, Result};
use std::{
    env,
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
    result,
};

/// KvStore implements in memory database.
pub struct KvStore {
    writer: BufWriter<File>,
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, val: String },
}

/// KvStore implements in memory database.
impl KvStore {
    /// new does something
    pub fn new() -> Result<Self> {
        let file_name = format!("{}.log", last_log_file_num().unwrap_or(0) + 1);
        println!("file: {}", file_name);

        match OpenOptions::new().append(true).create(true).open(file_name) {
            Err(e) => {
                eprintln!("failed to open log, err: {}", e);
                Err(KvsError::LogInit)
            }
            Ok(file) => Ok(Self {
                writer: BufWriter::new(file),
            }),
        }
    }

    /// set runs set
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let c = Command::Set { key: key, val: val };

        serde_json::to_writer(&mut self.writer, &c)?;
        self.writer.flush()?;
        Ok(())
    }

    /// get runs get
    pub fn get(&self, key: String) -> Result<Option<String>> {
        panic!();
    }

    /// remove runs remove
    pub fn remove(&mut self, key: String) -> Result<()> {
        panic!();
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        panic!();
    }
}

fn last_log_file_num() -> Option<u32> {
    let curr = env::current_dir().unwrap();

    let entries = fs::read_dir(curr).unwrap();

    let mut y: Vec<_> = entries
        .filter_map(result::Result::ok)
        .filter(|e| e.path().extension() == Some(PathBuf::from("log").as_os_str()))
        .map(|e| {
            e.path()
                .file_stem()
                .and_then(OsStr::to_str)
                .map(str::to_owned)
            // .map(str::parse::<u64>)
        })
        .filter_map(|file_stem_str| file_stem_str.and_then(|s| s.parse::<u32>().ok()))
        .collect(); // Collect the results into a Vec<String>

    y.sort_unstable();

    y.last().copied()
}
