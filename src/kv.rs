use serde::{Deserialize, Serialize};

use crate::{KvsError, Result};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
};

/// KvStore implements in memory database.
pub struct KvStore {
    writer: BufWriter<File>,
}

#[derive(Serialize, Deserialize, Debug)]
struct SetCmd {
    key: String,
    val: String,
}

/// KvStore implements in memory database.
impl KvStore {
    /// new does something
    pub fn new(log_path: PathBuf) -> Result<Self> {
        match OpenOptions::new().append(true).create(true).open(log_path) {
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
        let c = SetCmd {
            key: key.clone(),
            val: val.clone(),
        };

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
