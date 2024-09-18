use std::{
    cell::RefCell,
    collections::BTreeMap,
    env::current_dir,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    net::TcpListener,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
};

use crossbeam_channel::{unbounded, Receiver};
use log::{debug, error, info};

use crate::{
    buf_writer::BufWriterWithPos,
    engine::{CommandPos, KvsEngine, KvsReader},
    thread_pool::ThreadPool,
    transport::Response,
    KvStore, Result,
};
use kvs_protocol::{deserializer::deserialize, request::Request};

pub struct KvServer {
    pub engine: KvStore,
    rx_compaction: Receiver<TxMessage>,
    path: PathBuf,
}

pub struct TxMessage {
    pub log_idx: Arc<AtomicU64>,
    pub path: PathBuf,
}

impl KvServer {
    pub fn new_with_path(p: PathBuf) -> KvServer {
        let (tx_compaction, rx_compaction) = unbounded::<TxMessage>();

        let engine = KvStore::new(tx_compaction.clone(), p.clone()).unwrap();

        KvServer {
            engine: engine.to_owned(),
            rx_compaction,
            path: p,
        }
    }

    pub fn new() -> KvServer {
        let (tx_compaction, rx_compaction) = unbounded::<TxMessage>();

        let p = current_dir().unwrap();
        let engine = KvStore::new(tx_compaction.clone(), p.clone()).unwrap();

        KvServer {
            engine: engine.to_owned(),
            rx_compaction,
            path: p,
        }
    }

    pub fn start<P: ThreadPool>(&self, addr: String, thread_pool: P) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        let rx_compaction = self.rx_compaction.to_owned();
        let log_writer = Arc::clone(&self.engine.log_writer);
        let key_dir = self.engine.key_dir.clone();
        let uncompacted = Arc::clone(&self.engine.uncompacted);

        let mut reader = KvsReader {
            path: self.path.clone(),
            readers: RefCell::new(BTreeMap::new()),
        };

        let r: JoinHandle<Result<()>> = thread::spawn(move || loop {
            println!("[receiver]: waiting for a signal");
            let msg = rx_compaction.recv().unwrap();
            {
                let mut log_writer = log_writer.lock().unwrap();
                let mut log_idx = msg.log_idx.load(Ordering::SeqCst);
                let path = msg.path;

                let new_compaction_log_idx = log_idx + 1;
                let new_compaction_file_path =
                    path.join(format!("{}.log", &new_compaction_log_idx));

                println!(
                    "[compaction]: new compaction log file idx {}, compaction file name {:?}",
                    new_compaction_log_idx, new_compaction_file_path
                );

                // create a writer for the log entry which will include the command details of the
                // existing commands on the memory.
                let mut compaction_log_writer: BufWriterWithPos<File> = BufWriterWithPos::new(
                    OpenOptions::new()
                        .create(true)
                        .write(true)
                        .open(&new_compaction_file_path)?,
                )?;

                let mut new_starting_pos = 0 as u64;

                info!("=====> COPYING OLD LOGS");
                // iterate through the active keys on the memory.
                for mut entry in key_dir.iter_mut() {
                    let copied_bytes = reader
                        .read_cmd_from_log_and_copy(entry.value(), &mut compaction_log_writer)?;

                    let v = entry.value_mut();
                    *v = CommandPos {
                        log_idx: new_compaction_log_idx as u32,
                        starting_pos: new_starting_pos,
                        len: copied_bytes,
                    };

                    new_starting_pos += copied_bytes;
                }
                compaction_log_writer.flush()?;
                info!("=====> COPYING OLD LOGS DONE");

                let keys_to_delete: Vec<u32> = {
                    let borrowed_map = reader.readers.borrow();
                    borrowed_map
                        .iter()
                        .filter_map(|(&key, reader)| {
                            // Your condition for deletion goes here
                            // For example, let's say we want to delete readers at position 0
                            if key < new_compaction_log_idx as u32 {
                                Some(key)
                            } else {
                                None
                            }
                        })
                        .collect()
                };

                {
                    let mut borrowed_map = reader.readers.borrow_mut();
                    for key in keys_to_delete {
                        borrowed_map.remove(&key);
                        println!("Removed reader with key: {}", key);
                    }
                }

                info!("DELETING OLD LOGS, LEN {}", new_compaction_log_idx);
                // todo: this is not efficient in case of big number of log files.
                // it always starts iterating from 1 to the recent log file and tries to delete them all the time.
                for i in 1..new_compaction_log_idx as u32 {
                    info!("trying to delete old log file {} from from fs done\n", i);
                    fs::remove_file(path.join(format!("{}.log", i))).or_else(|e| {
                        if e.kind() == io::ErrorKind::NotFound {
                            info!("log file {} is not found", i);
                            Ok(())
                        } else {
                            info!("Failed to delete log file {}, err: {}", i, e);
                            Err(e)
                        }
                    })?;
                    info!("deleting old log file {} from from fs done\n", i);
                }
                info!("=====> DELETING OLD LOGS");

                // self.log_idx + 1 corresponds to the new log file which will include all active
                // commands in the memory. So, the new requests need to be moved to self.log_idx + 2
                // which will be new log entry in the file system.
                log_idx += 2;
                // now, update the writer so that the new log entries will be written into a new log file.
                info!("updating log writer");
                *log_writer = BufWriterWithPos::new(
                    OpenOptions::new()
                        .create(true)
                        .write(true)
                        .open(path.join(format!("{}.log", log_idx)))?,
                )?;

                msg.log_idx.store(log_idx, Ordering::SeqCst);
                {
                    match uncompacted.try_write() {
                        Ok(mut u) => *u = 0,
                        Err(_) => info!("failed to obtain a lock while updating the uncompaction"),
                    }
                }
                info!("[compaction]: writer of the compaction is updated! the new commands will be appended into the log idx: {}", log_idx);
            }
        });

        for stream in listener.incoming() {
            let engine = self.engine.clone();
            thread_pool.spawn(move || match stream {
                Ok(stream) => {
                    if let Err(e) = handle_client_req(engine, stream) {
                        error!("Error on serving client: {}", e);
                    }
                }
                Err(e) => error!("Connection failed: {}", e),
            })
        }

        Ok(())
    }
}

fn handle_client_req<E>(engine: E, stream: std::net::TcpStream) -> Result<()>
where
    E: KvsEngine,
{
    info!("==> New request!");
    let mut request_reader = BufReader::new(stream.try_clone().unwrap());
    let mut response_writer = BufWriter::new(stream);

    // TODO: error handling in the read_line
    let mut buf = String::new();
    if let Err(err) = request_reader.read_line(&mut buf) {
        return Err(crate::KvsError::TCP(err.to_string()));
    }

    match deserialize::<Request>(buf.as_str()) {
        Err(e) => {
            error!("failed to deserialize the request, err: {}", e);
            Err(crate::KvsError::TCP(e.to_string()))
        }
        Ok(req) => {
            match &req {
                Request::Get { key } => {
                    info!("==> GET request {} ", key);
                    if let Ok(v) = engine.get(key.to_string()) {
                        let mut resp: Response = Response {
                            ..Default::default()
                        };
                        if let Some(val) = v {
                            resp.result = val.clone();
                        } else {
                            resp.error = Some("Key not found".to_string());
                        }
                        info!("==> DONE GET request {} -> {:?}", key, resp);

                        serde_json::to_writer(&mut response_writer, &resp).unwrap();
                        response_writer.flush().unwrap(); // TODO
                    } else {
                        info!("no response:");
                    }
                }
                Request::Set { key, val } => {
                    info!("==> SET request {} {} ", key, val);

                    match engine.set(key.to_string(), val.to_string()) {
                        Ok(_) => {
                            debug!("key: '{}' with value: '{}' inserted succesfully", key, val)
                        }
                        Err(e) => error!("failed to write key: '{}', err: {}", key, e),
                    }
                    info!("==> DONE SET request {} {} ", key, val);
                }
                Request::Rm { key } => {
                    info!("==> RM request {} ", key);

                    let mut resp: Response = Response {
                        ..Default::default()
                    };

                    if let Err(e) = engine.remove(key.to_string()) {
                        error!("failed to remove the key, err: {}", e);

                        resp.error = Some("Key not found".to_string());
                    }

                    info!("==> DONE RM request {} ", key);
                    serde_json::to_writer(&mut response_writer, &resp).unwrap();
                    response_writer.flush().unwrap(); // TODO
                }
            };
            Ok(())
        }
    }
}

// // compaction runs merging of bitcask.
// // when uncompacted bytes amount reaches the threshold, the compaction will be run in next set command.
// //
// // 1- it first creates a new log entry which will copy entries from previous logs that are
// // active at the moment (which means active in key_dir hash map). Therefore, the new log entry
// // will be the reflection of our in-memory key_dir map.
// // 2- after creating this new log file, it removes the previous log files.
// fn compaction(
//     writer: &mut BufWriterWithPos<File>,
//     log_idx: u64,
//     path: PathBuf,
//     key_dir: DashMap<String, CommandPos>,
//     reader: KvsReader,
// ) -> Result<()> {
//     // Create a new log file including all the commands stored in in-memory keydir map.
//     // So, it will be our new starting idx for logs.
//     // All logs up to `self.log_idx + 1` will be included in to `new_log_file_path`.
//     let new_compaction_log_idx = log_idx + 1;
//     let new_compaction_file_path = path.join(format!("{}.log", &new_compaction_log_idx));

//     println!(
//         "[compaction]: new compaction log file idx {}, compaction file name {:?}",
//         new_compaction_log_idx, new_compaction_file_path
//     );

//     // create a writer for the log entry which will include the command details of the
//     // existing commands on the memory.
//     let mut compaction_log_writer: BufWriterWithPos<File> = BufWriterWithPos::new(
//         OpenOptions::new()
//             .create(true)
//             .write(true)
//             .open(&new_compaction_file_path)?,
//     )?;

//     let mut new_starting_pos = 0 as u64;
//     info!("=====> COPYING OLD LOGS");
//     // iterate through the active keys on the memory.
//     for mut entry in key_dir.iter_mut() {
//         let copied_bytes = self
//             .reader
//             .read_cmd_from_log_and_copy(entry.value(), &mut compaction_log_writer)?;

//         let v = entry.value_mut();
//         *v = CommandPos {
//             log_idx: new_compaction_log_idx as u32,
//             starting_pos: new_starting_pos,
//             len: copied_bytes,
//         };

//         new_starting_pos += copied_bytes;
//     }
//     compaction_log_writer.flush()?;
//     info!("=====> COPYING OLD LOGS DONE");

//     // todo: this is not efficient in case of big number of log files.
//     // it always starts iterating from 1 to the recent log file and tries to delete them all the time.
//     for i in 1..new_compaction_log_idx {
//         fs::remove_file(path.join(format!("{}.log", i)))?;
//     }

//     // self.log_idx + 1 corresponds to the new log file which will include all active
//     // commands in the memory. So, the new requests need to be moved to self.log_idx + 2
//     // which will be new log entry in the file system.
//     log_idx += 2;
//     // now, update the writer so that the new log entries will be written into a new log file.
//     *writer = BufWriterWithPos::new(
//         OpenOptions::new()
//             .create(true)
//             .write(true)
//             .open(path.join(format!("{}.log", log_idx)))?,
//     )?;
//     info!("[compaction]: writer of the compaction is updated! the new commands will be appended into the log idx: {}", log_idx);

//     Ok(())
// }
