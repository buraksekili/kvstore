use crate::{
    buf_reader::BufReaderWithPos, buf_writer::BufWriterWithPos, KvsEngine, KvsError, Result,
};
use dashmap::DashMap;
use kvs_protocol::{
    deserializer::deserialize as kvs_deserialize, parser::KvReqParser, request::Request,
    serializer::serialize as kvs_serialize,
};
use log::info;

use std::{
    collections::BTreeMap,
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    result,
    sync::{Arc, RwLock},
    u32,
};

const COMPACTION_THRESHOLD: u64 = 20;

#[derive(Debug, Clone, Copy)]
pub struct CommandPos {
    log_idx: u32,
    starting_pos: u64,
    len: u64,
}

// KvsWriter runs on a single thread
pub struct Compactor {
    log_idx: u32,
    uncompacted: u64,
    key_dir: Arc<DashMap<String, CommandPos>>,
    path: Arc<PathBuf>,
    reader: Arc<KvsReader>,
}

impl Compactor {
    // compaction runs merging of bitcask.
    // when uncompacted bytes amount reaches the threshold, the compaction will be run in next set command.
    //
    // 1- it first creates a new log entry which will copy entries from previous logs that are
    // active at the moment (which means active in key_dir hash map). Therefore, the new log entry
    // will be the reflection of our in-memory key_dir map.
    // 2- after creating this new log file, it removes the previous log files.
    fn compaction(&mut self, writer: &mut BufWriterWithPos<File>) -> Result<()> {
        // Create a new log file including all the commands stored in in-memory keydir map.
        // So, it will be our new starting idx for logs.
        // All logs up to `self.log_idx + 1` will be included in to `new_log_file_path`.
        let new_compaction_log_idx = self.log_idx + 1;
        let new_log_file_path = self.path.join(format!("{}.log", &new_compaction_log_idx));

        println!(
            "[compaction]: new compaction log file idx {}, compaction file name {:?}",
            new_compaction_log_idx, new_log_file_path
        );

        // create a writer for the log entry which will include the command details of the
        // existing commands on the memory.
        let mut compaction_log_writer: BufWriterWithPos<File> = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(&new_log_file_path)?,
        )?;

        let mut new_starting_pos = 0 as u64;
        info!("=====> COPYING OLD LOGS");
        // iterate through the active keys on the memory.
        for mut entry in self.key_dir.iter_mut() {
            let copied_bytes = self
                .reader
                .read_cmd_from_log_and_copy(entry.value(), &mut compaction_log_writer)?;

            let v = entry.value_mut();
            *v = CommandPos {
                log_idx: new_compaction_log_idx,
                starting_pos: new_starting_pos,
                len: copied_bytes,
            };

            new_starting_pos += copied_bytes;
        }
        compaction_log_writer.flush()?;
        info!("=====> COPYING OLD LOGS DONE");

        // todo: this is not efficient in case of big number of log files.
        // it always starts iterating from 1 to the recent log file and tries to delete them all the time.
        for i in 1..new_compaction_log_idx {
            fs::remove_file(self.path.join(format!("{}.log", i)))?;
        }

        // self.log_idx + 1 corresponds to the new log file which will include all active
        // commands in the memory. So, the new requests need to be moved to self.log_idx + 2
        // which will be new log entry in the file system.
        self.log_idx += 2;
        // now, update the writer so that the new log entries will be written into a new log file.
        *writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(self.path.join(format!("{}.log", self.log_idx)))?,
        )?;
        info!("[compaction]: writer of the compaction is updated! the new commands will be appended into the log idx: {}", self.log_idx);

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
    readers: Arc<RwLock<BTreeMap<u32, BufReaderWithPos<File>>>>,
    // In file systems and I/O operations, a "handle" typically refers to a reference or identifier for an open file or I/O resource.
}

impl Clone for KvsReader {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            readers: self.readers.clone(),
        }
    }
}

impl KvsReader {
    pub fn new(path: Arc<PathBuf>) -> Self {
        KvsReader {
            path,
            readers: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn read_cmd_from_log_and_copy(
        &self,
        cmd_pos: &CommandPos,
        writer: &mut BufWriterWithPos<File>,
    ) -> Result<u64> {
        let mut readers = self.readers.write().unwrap();

        if !readers.contains_key(&cmd_pos.log_idx) {
            let curr_file_path = self.path.join(format!("{}.log", cmd_pos.log_idx));
            let reader = BufReaderWithPos::new(File::open(&curr_file_path)?)?;
            readers.insert(cmd_pos.log_idx, reader);
        }
        let reader = readers.get_mut(&cmd_pos.log_idx).unwrap();

        reader.seek(SeekFrom::Start(cmd_pos.starting_pos))?;
        let mut cmd_reader = reader.take(cmd_pos.len);

        let copied_bytes = io::copy(&mut cmd_reader, writer)?;
        Ok(copied_bytes)
    }

    pub fn read_cmd_from_log(&self, cmd_pos: &CommandPos) -> Result<Request> {
        let mut readers = self.readers.write().unwrap();

        if !readers.contains_key(&cmd_pos.log_idx) {
            let curr_file_path = self.path.join(format!("{}.log", cmd_pos.log_idx));
            let reader = BufReaderWithPos::new(File::open(&curr_file_path)?)?;
            readers.insert(cmd_pos.log_idx, reader);
        }
        let reader = readers.get_mut(&cmd_pos.log_idx).unwrap();

        reader.seek(SeekFrom::Start(cmd_pos.starting_pos))?;
        let mut cmd_reader = reader.take(cmd_pos.len);

        let mut buf_str = String::new();
        cmd_reader.read_to_string(&mut buf_str)?;

        kvs_deserialize::<Request>(&mut buf_str)
            .map_err(|e| KvsError::KvsDeserializer(buf_str, e.to_string()))
    }
}

/// KvStore implements in memory database.
#[derive(Clone)]
pub struct KvStore {
    writer: Arc<RwLock<BufWriterWithPos<File>>>,
    log_idx: u32,
    key_dir: Arc<DashMap<String, CommandPos>>,
    reader: Arc<KvsReader>,
    compactor: Arc<RwLock<Compactor>>,
}

impl KvsEngine for KvStore {
    fn set(&self, k: String, val: String) -> Result<()> {
        let mut writer = self.writer.write().unwrap();
        let prev_pos = writer.pos;

        let c = Request::Set {
            key: k.clone(),
            val: val.clone(),
        };
        writer.write(kvs_serialize(&c).as_bytes())?;
        writer.flush()?;

        // Perform insert and capture old command
        let old_cmd_len = if let Some(old_cmd) = self.key_dir.insert(
            k,
            CommandPos {
                log_idx: self.log_idx,
                starting_pos: prev_pos,
                len: writer.pos - prev_pos,
            },
        ) {
            old_cmd.len
        } else {
            0
        };
        drop(writer);

        // Update uncompacted outside of key_dir lock
        if old_cmd_len > 0 {
            let mut compactor = self.compactor.write().unwrap();
            compactor.uncompacted += old_cmd_len;
        }

        if self.compactor.read().unwrap().uncompacted > COMPACTION_THRESHOLD {
            let mut writer = self.writer.write().unwrap();
            let mut compactor = self.compactor.write().unwrap();
            compactor.compaction(&mut *writer)?;
        }

        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.key_dir.get(&key) {
            match self.reader.read_cmd_from_log(cmd_pos.value())? {
                Request::Set { val, .. } => Ok(Some(val)),
                _ => Err(KvsError::UnexpectedCommandType(cmd_pos.key().to_owned())),
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        // Use DashMap's remove method which returns the removed value
        if let Some((_, old_cmd)) = self.key_dir.remove(&key) {
            let mut buf_writer = self.writer.write().unwrap();
            let c = Request::Rm { key };
            let pos_before_writing = buf_writer.pos;
            buf_writer.writer.write(kvs_serialize(&c).as_bytes())?;
            buf_writer.writer.flush()?;
            let pos_after_writing = buf_writer.pos;
            drop(buf_writer);

            {
                let mut compactor = self.compactor.write().unwrap();
                compactor.uncompacted += pos_after_writing - pos_before_writing;
                compactor.uncompacted += old_cmd.len;

                if compactor.uncompacted > COMPACTION_THRESHOLD {
                    let mut buf_writer = self.writer.write().unwrap();
                    compactor.compaction(&mut *buf_writer)?;
                }
            }

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

/// KvStore implements in memory database.
impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        // copy the path
        let path = Arc::new(path.into());

        // get all log files in the given path
        let log_files = log_files(&path);
        let reader = Arc::new(KvsReader::new(Arc::clone(&path)));
        let key_dir = Arc::new(DashMap::new());

        let mut temp_readers = BTreeMap::new();

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
                            if let Some(old_cmd) = key_dir.insert(
                                key,
                                CommandPos {
                                    log_idx: *lf_idx,
                                    starting_pos,
                                    len: read_so_far - starting_pos,
                                },
                            ) {
                                uncompacted += old_cmd.len;
                            }
                        }
                        Request::Rm { key } => {
                            if let Some(old_cmd) = key_dir.remove(&key) {
                                uncompacted += old_cmd.1.len;
                            }
                        }
                        _ => {} // no logs for Get request.
                    }
                    starting_pos = read_so_far;
                } else {
                    info!("failed to get Request");
                }
            }
            temp_readers.insert(*lf_idx, reader);
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
        let compaction_log: BufWriterWithPos<File> = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&new_log_file_path)?,
        )?;

        temp_readers.insert(
            new_log_file_idx,
            BufReaderWithPos::new(File::open(new_log_file_path)?)?,
        );

        // Update the KvsReader's readers map with the temporary map
        {
            let mut readers = reader.readers.write().unwrap();
            *readers = temp_readers;
        }

        let active_log_writer = Arc::new(RwLock::new(new_log_writer));

        Ok(KvStore {
            writer: Arc::clone(&active_log_writer),
            reader: Arc::clone(&reader),
            key_dir: Arc::clone(&key_dir),
            log_idx: new_log_file_idx,
            compactor: Arc::new(RwLock::new(Compactor {
                log_idx: new_log_file_idx,
                uncompacted,
                key_dir: Arc::clone(&key_dir),
                path: Arc::clone(&path),
                reader: Arc::clone(&reader),
            })),
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
