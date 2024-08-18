use serde::{Deserialize, Serialize};

use crate::{KvsError, Result};
use std::{
    collections::HashMap,
    env,
    ffi::OsStr,
    fs::{self, read, File, OpenOptions},
    hash::Hash,
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    result,
};

/// KvStore implements in memory database.
pub struct KvStore {
    writer: BufWriter<File>,
    readers: HashMap<u32, BufReader<File>>,
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, val: String },
    Rm { key: String },
}

fn new_writer(p: &Path) -> Result<BufWriter<File>> {
    let buf_writer = BufWriter::new(
        OpenOptions::new()
            .append(true)
            .create(true)
            .write(true)
            .open(p)?,
    );

    Ok(buf_writer)
}

/// KvStore implements in memory database.
impl KvStore {
    /// set runs set
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let c = Command::Set { key: key, val: val };

        serde_json::to_writer(&mut self.writer, &c)?;
        self.writer.flush()?;
        Ok(())
    }

    /// get runs get
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(Some(String::from("x")))
    }

    /// remove runs remove
    pub fn remove(&mut self, key: String) -> Result<()> {
        let c = Command::Rm { key: key };

        serde_json::to_writer(&mut self.writer, &c)?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let p = path.into();

        let mut readers: HashMap<u32, BufReader<File>> = HashMap::new();
        let log_files = log_files(&p);

        // read all log files, and save them in hash map.
        for lf in &log_files {
            readers.insert(*lf, BufReader::new(File::open(format!("{}.log", lf))?));
        }

        let new_log_idx = log_files.last().unwrap_or(&0) + 1;
        let new_log_file_name = format!("{}.log", new_log_idx);
        let new_log_writer: BufWriter<File> = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&new_log_file_name)?,
        );
        readers.insert(new_log_idx, BufReader::new(File::open(new_log_file_name)?));

        println!("[DEBUG] writing into: {}", format!("{}.log", new_log_idx));

        Ok(KvStore {
            readers: readers,
            writer: new_log_writer,
        })
    }
}

fn log_files(p: &Path) -> Vec<u32> {
    let entries = fs::read_dir(p).unwrap();

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

    y
}
