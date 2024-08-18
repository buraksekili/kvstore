use crate::{buf_reader::BufReaderWithPos, buf_writer::BufWriterWithPos, KvsError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use std::{
    collections::{BTreeMap, HashMap},
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    result,
};

/// KvStore implements in memory database.
pub struct KvStore {
    writer: BufWriterWithPos<File>,
    // readers stores the reader of each log files as value, and the
    // index of the logs as key.
    readers: HashMap<u32, BufReaderWithPos<File>>,

    // A map of keys to log pointers.
    index: BTreeMap<String, CommandPos>,

    log_idx: u32,
}

#[derive(Debug)]
struct CommandPos {
    log_idx: u32,
    starting_pos: u64,
    len: u64,
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, val: String },
    Remove { key: String },
}

/// KvStore implements in memory database.
impl KvStore {
    pub fn t(&mut self) {
        for i in &self.index {
            println!("i: {:?}\t{:?}", i.0, i.1);
        }
    }
    /// set runs set
    pub fn set(&mut self, k: String, val: String) -> Result<()> {
        let key = k.clone();
        let c = Command::Set { key: key, val: val };
        let prev_pos = self.writer.pos;

        // Write Set command to the log.
        serde_json::to_writer(&mut self.writer, &c)?;
        self.writer.flush()?;

        // Now, update in-memory index file. Whenever clients want to read
        // a key, we'll first go through the index map.

        let cmd_pos = CommandPos {
            log_idx: self.log_idx,
            starting_pos: prev_pos,
            len: self.writer.pos - prev_pos,
        };

        self.index.insert(k.clone(), cmd_pos);
        // println!("testing: {:?}", self.index.get(&k.clone()));

        Ok(())
    }

    /// get runs get
    pub fn get(&mut self, input_key: String) -> Result<Option<String>> {
        let result = self.index.get(&input_key).and_then(|pos| {
            let reader = self.readers.get_mut(&pos.log_idx).unwrap();
            // move file pointer to the position where our log starts.
            reader.seek(SeekFrom::Start(pos.starting_pos)).unwrap();
            // create a reader which reads `pos.len` many bytes from the log file.
            let cmd_reader = reader.take(pos.len);
            let curr_cmd: Command = serde_json::from_reader(cmd_reader).unwrap();
            if let Command::Set { key: _, val } = curr_cmd {
                Some(val)
            } else {
                None
            }
        });

        // println!("*******************************");
        // for i in &self.index {
        //     println!("i: {:?}\t{:?}", i.0, i.1);
        // }
        // println!("*******************************");

        if let Some(r) = result {
            Ok(Some(r))
        } else {
            Ok(None)
        }

        // let mut last_seen_idx: i32 = -1;
        // let mut value = String::new();
        // self.readers.iter_mut().for_each(|element| {
        //     if let Ok(Command::Set { key, val }) = serde_json::from_reader(element.1) {
        //         let curr_idx: i32 = *element.0 as i32;
        //         if key == input_key && curr_idx > last_seen_idx {
        //             value = val;
        //             last_seen_idx = curr_idx;
        //         }
        //     }
        // });

        // if last_seen_idx == -1 {
        //     return Ok(None);
        // }

        // Ok(Some(value))
    }

    /// remove runs remove
    pub fn remove(&mut self, key: String) -> Result<()> {
        let tmp_key: String = key.clone();
        let c = Command::Remove { key: key };

        serde_json::to_writer(&mut self.writer, &c)?;
        self.writer.flush()?;

        if let Some(_) = self.index.remove(&tmp_key) {
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        // copy the path
        let p = path.into();
        // fs::create_dir_all(&p)?;

        // get all log files in the given path
        let log_files = log_files(&p);
        let mut index: BTreeMap<String, CommandPos> = BTreeMap::new();

        let new_log_idx = log_files.last().unwrap_or(&0) + 1;
        let new_log_file_name = format!("{}.log", new_log_idx);
        let new_log_file_path = p.join(&new_log_file_name);

        // read all log files, and save them in hash map.
        let mut readers: HashMap<u32, BufReaderWithPos<File>> = HashMap::new();
        for lf_idx in &log_files {
            let mut reader = BufReaderWithPos::new(File::open(p.join(format!("{}.log", lf_idx)))?)?;

            // build up our index tree.
            // start reading from the beginning.
            let mut starting_pos = reader.seek(SeekFrom::Start(0))?;
            // create a deserializer as iterator. Read each file while deserializing it.
            let mut command_iter =
                Deserializer::from_reader(reader.by_ref()).into_iter::<Command>();

            // iterate through each element that we deserialize during iteration.
            while let Some(cmd) = command_iter.next() {
                // get the total number of bytes read so far.
                let read_so_far = command_iter.byte_offset() as u64;

                match cmd? {
                    Command::Set { key, val: _ } => {
                        index.insert(
                            key,
                            CommandPos {
                                log_idx: *lf_idx,
                                starting_pos,
                                len: read_so_far - starting_pos,
                            },
                        );
                    }
                    Command::Remove { key } => {
                        index.remove(&key);
                    }
                };

                // update the starting position to the current position which is total number of bytes
                // read until now.
                //
                // For example, assume that we read 1 command which takes 35bytes in the log file.
                // in the next iteration, the starting point needs to be 35.
                starting_pos = read_so_far;
            }

            // once completing operations on a reader, add it to an in-memory
            // structure for future references.
            readers.insert(*lf_idx, reader);
        }

        // create a new log file.
        let new_log_writer: BufWriterWithPos<File> = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&new_log_file_path)?,
        )?;
        readers.insert(
            new_log_idx,
            BufReaderWithPos::new(File::open(new_log_file_path)?)?,
        );

        Ok(KvStore {
            readers,
            log_idx: new_log_idx,
            index,
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
