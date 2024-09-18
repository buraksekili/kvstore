use crate::{
    buf_reader::BufReaderWithPos, buf_writer::BufWriterWithPos, server::TxMessage, KvsEngine,
    KvsError, Result,
};
use crossbeam_channel::Sender;
use dashmap::DashMap;
use kvs_protocol::{
    deserializer::deserialize as kvs_deserialize, parser::KvReqParser, request::Request,
    serializer::serialize as kvs_serialize,
};
use log::info;

use std::{
    cell::RefCell,
    collections::BTreeMap,
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    result,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    u32,
};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct CommandPos {
    pub log_idx: u32,
    pub starting_pos: u64,
    pub len: u64,
}

pub struct KvsReader {
    pub path: PathBuf,
    // readers stores the reader of each log files as value, and the
    // index of the logs as key.
    // The primary reason for using RefCell here is to allow mutable access
    // to the readers map even if the KvStoreReader itself is borrowed immutably
    pub readers: RefCell<BTreeMap<u32, BufReaderWithPos<File>>>,
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

        reader.seek(SeekFrom::Start(cmd_pos.starting_pos))?;
        let mut cmd_reader = reader.take(cmd_pos.len);

        let copied_bytes = io::copy(&mut cmd_reader, writer)?;
        Ok(copied_bytes)
    }

    pub fn read_cmd_from_log(&self, cmd_pos: &CommandPos) -> Result<Request> {
        let mut readers = self.readers.borrow_mut();

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
    // I can't read while someone is writing because compaction may happen
    // which might invalidate current read by deleting a file for instance?
    //
    // PROBLEM: During compaction, i can't access the logs which prevents read access
    // from functioning?
    pub log_writer: Arc<Mutex<BufWriterWithPos<File>>>,
    pub tx_compaction: Option<Sender<TxMessage>>,
    pub log_idx: Arc<AtomicU64>,
    pub key_dir: Arc<DashMap<String, CommandPos>>,
    pub uncompacted: Arc<RwLock<u64>>,
    reader: KvsReader,
    path: PathBuf,
}

impl KvsEngine for KvStore {
    fn set(&self, k: String, val: String) -> Result<()> {
        let mut writer = self.log_writer.lock().unwrap();
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
                log_idx: self.log_idx.load(Ordering::SeqCst) as u32,
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
            let mut uncompacted = self.uncompacted.write().unwrap();
            *uncompacted += old_cmd_len;
        }

        if *self.uncompacted.read().unwrap() > COMPACTION_THRESHOLD {
            if let Some(sender) = &self.tx_compaction {
                sender
                    .send(TxMessage {
                        log_idx: Arc::clone(&self.log_idx),
                        path: self.path.to_owned(),
                    })
                    .unwrap();
            }
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
            let mut buf_writer = self.log_writer.lock().unwrap();
            let c = Request::Rm { key };
            let pos_before_writing = buf_writer.pos;
            buf_writer.writer.write(kvs_serialize(&c).as_bytes())?;
            buf_writer.writer.flush()?;
            let pos_after_writing = buf_writer.pos;
            drop(buf_writer);

            {
                let mut uncompacted = self.uncompacted.write().unwrap();
                *uncompacted += pos_after_writing - pos_before_writing;
                *uncompacted += old_cmd.len;
            }
            if *self.uncompacted.read().unwrap() > COMPACTION_THRESHOLD {
                if let Some(tx) = &self.tx_compaction {
                    tx.send(TxMessage {
                        log_idx: Arc::clone(&self.log_idx),
                        path: self.path.to_owned(),
                    })
                    .unwrap();
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
    pub fn new(tx_compaction: Sender<TxMessage>, path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut store = KvStore::open(path)?;
        if store.tx_compaction.is_none() {
            store.tx_compaction.replace(tx_compaction);
        }

        Ok(store)
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();

        // get all log files in the given path
        let log_files = log_files(&path);

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
        let log_idx = AtomicU64::new(new_log_file_idx as u64);
        let new_log_file_path = path.join(format!("{}.log", new_log_file_idx));

        // create a new log file.
        let new_log_writer: BufWriterWithPos<File> = BufWriterWithPos::new(
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

        let reader = KvsReader {
            path: path.clone(),
            readers: RefCell::new(temp_readers),
        };

        let active_log_writer = Arc::new(Mutex::new(new_log_writer));

        Ok(KvStore {
            uncompacted: Arc::new(RwLock::new(uncompacted)),
            log_writer: Arc::clone(&active_log_writer),
            path,
            reader,
            key_dir: Arc::clone(&key_dir),
            log_idx: Arc::new(log_idx),
            tx_compaction: None,
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
