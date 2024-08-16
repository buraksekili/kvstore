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
    Rm { key: String },
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
        let c = Command::Rm { key: key };

        serde_json::to_writer(&mut self.writer, &c)?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        panic!();
    }
}

fn last_log_file_num() -> Option<u32> {
    let curr = env::current_dir().unwrap();

    let entries = fs::read_dir(curr).unwrap();

    let mut y: Vec<u32> = entries
        .filter_map(result::Result::ok)
        .map(|a| a.path())
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|file_name_str| {
                    let x = file_name_str.trim_end_matches(".log");
                    x.parse::<u32>()
                })
        })
        .filter_map(result::Result::ok)
        .collect();

    y.sort_unstable();

    y.last().copied()
}
