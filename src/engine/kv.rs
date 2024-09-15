use crate::{
    buf_reader::BufReaderWithPos, buf_writer::BufWriterWithPos, KvsEngine, KvsError, Result,
};
use crossbeam_skiplist::SkipMap;
use kvs_protocol::{
    deserializer::deserialize as kvs_deserialize, parser::KvReqParser, request::Request,
    serializer::serialize as kvs_serialize,
};
use log::{error, info};

use std::{
    cell::RefCell,
    collections::BTreeMap,
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    result,
    sync::{Arc, Mutex},
    u32,
};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

// KvsWriter runs on a single thread
pub struct KvsWriter {
    writer: BufWriterWithPos<File>,
    log_idx: u32,
    uncompacted: u64,
    key_dir: Arc<SkipMap<String, CommandPos>>,
    path: Arc<PathBuf>,
    reader: KvsReader,
}

impl KvsWriter {
    /// set runs set
    pub fn set(&mut self, k: String, val: String) -> Result<()> {
        let key = k.clone(); // todo: mixed up usage of k and key - need to do type alias one of them maybe?
        let prev_pos = self.writer.pos;

        // Write Set command to the log.
        let c = Request::Set { key, val };
        self.writer.write(kvs_serialize(&c).as_bytes())?;
        self.writer.flush()?;

        // Now, update in-memory index file. Whenever clients want to read
        // a key, we'll first go through the index map.
        let cmd_pos = CommandPos {
            log_idx: self.log_idx,
            starting_pos: prev_pos,
            len: self.writer.pos - prev_pos,
        };

        // if we have some here, it means that our map already contains the value.
        // So, we can understand that our storage will include some duplicated
        // data which can be uncompacted.
        if let Some(old_cmd) = self.key_dir.get(&k) {
            self.uncompacted += old_cmd.value().len;
        }

        self.key_dir.insert(k, cmd_pos);

        if self.uncompacted > COMPACTION_THRESHOLD {
            return self.compaction();
        }

        Ok(())
    }

    /// remove runs remove
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.key_dir.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }

        let tmp_key: String = key.clone();

        let c = Request::Rm { key };
        let pos_before_writing = self.writer.pos;
        self.writer.write(kvs_serialize(&c).as_bytes())?;
        self.writer.flush()?;
        let pos_after_writing = self.writer.pos;
        self.uncompacted += pos_after_writing - pos_before_writing;

        self.key_dir.remove(&tmp_key).unwrap();

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compaction()?;
        }

        Ok(())
    }

    // compaction runs merging of bitcask.
    // when uncompacted bytes amount reaches the threshold, the compaction will be run in next set command.
    //
    // 1- it first creates a new log entry which will copy entries from previous logs that are
    // active at the moment (which means active in key_dir hash map). Therefore, the new log entry
    // will be the reflection of our in-memory key_dir map.
    // 2- after creating this new log file, it removes the previous log files.
    fn compaction(&mut self) -> Result<()> {
        // Create a new log file including all the commands stored in in-memory keydir map.
        let new_compaction_log_idx = self.log_idx + 1;
        let new_log_file_path = self.path.join(format!("{}.log", &new_compaction_log_idx));
        println!("running -> new compaction idx: {}", new_compaction_log_idx);

        // self.log_idx + 1 corresponds to the new log file which will include all active
        // commands in the memory. So, the new requests need to be moved to self.log_idx + 2
        // which will be new log entry in the file system.
        self.log_idx += 2;
        // now, update the writer so that the new log entries will be written into a new log file.
        self.writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(self.path.join(format!("{}.log", self.log_idx)))?,
        )?;

        println!("running -> new log idx: {}", self.log_idx);

        // create a writer for the log entry which will include the command details of the
        // existing commands on the memory.
        let mut new_log_writer: BufWriterWithPos<File> = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&new_log_file_path)?,
        )?;

        let mut new_starting_pos = 0 as u64;
        // iterate through the active keys on the memory.
        for cmd_pos in self.key_dir.iter() {
            let copied_bytes = self
                .reader
                .read_cmd_from_log_and_copy(&cmd_pos.value(), &mut new_log_writer)?;

            self.key_dir.insert(
                cmd_pos.key().clone(),
                CommandPos {
                    log_idx: new_compaction_log_idx,
                    starting_pos: new_starting_pos,
                    len: copied_bytes,
                },
            );
            new_starting_pos += copied_bytes;
        }
        new_log_writer.flush()?;

        // todo: this is not efficient in case of big number of log files.
        // it always starts iterating from 1 to the recent log file and tries to delete them all the time.
        for i in 1..new_compaction_log_idx {
            fs::remove_file(self.path.join(format!("{}.log", i)))?;
        }

        self.uncompacted = 0;

        Ok(())
    }
}

pub struct KvsReader {
    path: Arc<PathBuf>,
    // readers stores the reader of each log files as value, and the
    // index of the logs as key.
    // The primary reason for using RefCell here is to allow mutable access
    // to the readers map even if the KvStoreReader itself is borrowed immutably
    readers: RefCell<BTreeMap<u32, BufReaderWithPos<File>>>,
    // In file systems and I/O operations, a "handle" typically refers to a reference or identifier for an open file or I/O resource.
}

impl Clone for KvsReader {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            readers: RefCell::new(BTreeMap::new()),
        }
    }
}

impl KvsReader {
    pub fn read_cmd_from_log_and_copy(
        &self,
        cmd_pos: &CommandPos,
        writer: &mut BufWriterWithPos<File>,
    ) -> Result<u64> {
        let mut readers = self.readers.borrow_mut();

        if !readers.contains_key(&cmd_pos.log_idx) {
            let curr_file_path = self.path.join(format!("{}.log", cmd_pos.log_idx));
            let reader = BufReaderWithPos::new(File::open(&curr_file_path)?)?;
            readers.insert(cmd_pos.log_idx, reader);
        }
        let reader = readers.get_mut(&cmd_pos.log_idx).unwrap();

        reader.seek(SeekFrom::Start(cmd_pos.starting_pos)).unwrap();
        let mut cmd_reader = reader.take(cmd_pos.len);

        let copied_bytes = io::copy(&mut cmd_reader, writer)?;
        return Ok(copied_bytes);
    }

    pub fn read_cmd_from_log(&self, cmd_pos: &CommandPos) -> Result<Request> {
        let mut readers = self.readers.borrow_mut();

        if !readers.contains_key(&cmd_pos.log_idx) {
            let curr_file_path = self.path.join(format!("{}.log", cmd_pos.log_idx));
            let reader = BufReaderWithPos::new(File::open(&curr_file_path)?)?;
            readers.insert(cmd_pos.log_idx, reader);
        }
        let reader = readers.get_mut(&cmd_pos.log_idx).unwrap();

        reader.seek(SeekFrom::Start(cmd_pos.starting_pos)).unwrap();
        let mut cmd_reader = reader.take(cmd_pos.len);

        let mut buf_str = String::new();
        match cmd_reader.read_to_string(&mut buf_str) {
            Err(e) => error!("failed to read command, {}", e),
            Ok(_) => {}
        };

        match kvs_deserialize::<Request>(&mut buf_str) {
            Ok(c) => Ok(c),
            Err(e) => Err(KvsError::KvsDeserializer(buf_str, e.to_string())),
        }
    }
}

/// KvStore implements in memory database.
#[derive(Clone)]
pub struct KvStore {
    writer: Arc<Mutex<KvsWriter>>,
    reader: KvsReader,

    // A map of keys to log pointers.
    // We store each command's position in the log file.
    // To find out which log file contains this key, you can check
    // CommandPos's `log_idx` field.
    // SkipMap is an alternative to BTreeMap` which supports
    /// concurrent access across multiple threads.
    key_dir: Arc<SkipMap<String, CommandPos>>,

    // path refers to the directory path for the log files.
    path: Arc<PathBuf>,
}

#[derive(Debug, Clone, Copy)]
pub struct CommandPos {
    log_idx: u32,
    starting_pos: u64,
    len: u64,
}

impl KvsEngine for KvStore {
    fn set(&self, k: String, val: String) -> Result<()> {
        self.writer.lock().unwrap().set(k, val)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.key_dir.get(&key) {
            if let Request::Set { val, .. } = self.reader.read_cmd_from_log(cmd_pos.value())? {
                Ok(Some(val))
            } else {
                Err(KvsError::KeyNotFound) // TODO: fix the error type, like unknown command
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

/// KvStore implements in memory database.
impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        // copy the path
        let path = Arc::new(path.into());

        // get all log files in the given path
        let log_files = log_files(&path);
        let mut readers: BTreeMap<u32, BufReaderWithPos<File>> = BTreeMap::new();
        let key_dir = Arc::new(SkipMap::new());

        let mut uncompacted = 0 as u64;
        for lf_idx in &log_files {
            let curr_log_path = path.join(format!("{}.log", lf_idx));
            let mut reader = BufReaderWithPos::new(File::open(curr_log_path)?)?;

            let mut starting_pos = reader.seek(SeekFrom::Start(0))?;
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer)?;
            let mut parser = KvReqParser::new(&buffer);

            while let Some(v) = parser.next() {
                let parsed_str = String::from_utf8_lossy(v);
                let _cmd: result::Result<Request, kvs_protocol::error::Error> =
                    kvs_deserialize::<Request>(&parsed_str);

                if let Ok(cmd) = _cmd {
                    let read_so_far = parser.read_so_far() as u64;
                    match cmd {
                        Request::Set { key, val: _ } => {
                            let old_cmd = key_dir.insert(
                                key,
                                CommandPos {
                                    log_idx: *lf_idx,
                                    starting_pos,
                                    len: read_so_far - starting_pos,
                                },
                            );
                            uncompacted += old_cmd.value().len;
                        }
                        Request::Rm { key } => {
                            if let Some(old_cmd) = key_dir.remove(&key) {
                                uncompacted += old_cmd.value().len;
                            }
                        }
                        _ => {} // no logs for Get request.
                    }
                    starting_pos = read_so_far;
                } else {
                    info!("failed to get Request");
                }
            }
            readers.insert(*lf_idx, reader);
        }

        let new_log_file_idx = log_files.last().unwrap_or(&0) + 1;
        let new_log_file_path = path.join(format!("{}.log", new_log_file_idx));

        // create a new log file.
        let new_log_writer: BufWriterWithPos<File> = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&new_log_file_path)?,
        )?;

        readers.insert(
            new_log_file_idx,
            BufReaderWithPos::new(File::open(new_log_file_path)?)?,
        );

        let reader = KvsReader {
            path: Arc::clone(&path),
            readers: RefCell::new(readers),
        };

        let writer = KvsWriter {
            writer: new_log_writer,
            log_idx: new_log_file_idx,
            uncompacted,
            key_dir: Arc::clone(&key_dir),
            path: Arc::clone(&path),
            reader: reader.clone(),
        };

        Ok(KvStore {
            writer: Arc::new(Mutex::new(writer)),
            reader,
            key_dir,
            path,
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
