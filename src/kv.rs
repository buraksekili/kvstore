use crate::{
    buf_reader::BufReaderWithPos, buf_writer::BufWriterWithPos, KvEngine, KvsError, Result,
};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use std::{
    collections::{BTreeMap, HashMap},
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    result, u32,
};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// KvStore implements in memory database.
pub struct KvStore {
    writer: BufWriterWithPos<File>,
    // readers stores the reader of each log files as value, and the
    // index of the logs as key.
    readers: HashMap<u32, BufReaderWithPos<File>>,

    // A map of keys to log pointers.
    // We store each command's position in the log file.
    // To find out which log file contains this key, you can check
    // CommandPos's `log_idx` field.
    key_dir: BTreeMap<String, CommandPos>,

    log_idx: u32,

    // path refers to the directory path for the log files.
    path: PathBuf,

    uncompacted: u64,
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

impl KvEngine for KvStore {
    fn set(&mut self, k: String, val: String) -> Result<()> {
        self.set(k, val)
    }

    fn get(&mut self, input_key: String) -> Result<Option<String>> {
        self.get(input_key)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.remove(key)
    }
}

/// KvStore implements in memory database.
impl KvStore {
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
        for cmd_pos in &mut self.key_dir.values_mut() {
            // get the reader of the log entry.
            let reader = self
                .readers
                .get_mut(&cmd_pos.log_idx)
                .expect("failed to find reader of the key");

            // if the position of the reader is not pointing to the correct command in the log file,
            // move it to the current position for the command.
            // assume that you have multiple commands in a log file where duplication for keys may appear.
            // for example, one log file may include "{set a 1}, {set b 2}, {set a 2}" where we have duplicated
            // set command for key 'a' and the first one is redundant. based on this log, our memory only have
            // one entry for key 'a' which has value 2. So, while reading this log, we must ensure that
            // reader is point to the position where '{set a 2}' command belongs to instead of '{set a 1}' command.
            // therefore, seek the offset to the correct starting point for the key here if it is not pointing
            // to the correct place.
            if reader.pos != cmd_pos.starting_pos {
                reader.seek(SeekFrom::Start(cmd_pos.starting_pos))?;
            }

            // After moving the correct place in the file, now we need to copy the existing entry
            // to the new log. In order to do that, we need to read `cmd_pos.len` many bytes from
            // the reader and then copy the content of it to the new log writer.
            let mut r = reader.take(cmd_pos.len);
            let copied_bytes = io::copy(&mut r, &mut new_log_writer)?;

            // after copying the entry to the new log, we must ensure that our in-memory reference
            // to CommandPos (which includes details of each command in the log file) is up to date
            // according to the latest changes.
            *cmd_pos = CommandPos {
                log_idx: new_compaction_log_idx,
                starting_pos: new_starting_pos,
                len: copied_bytes,
            };

            // as we wrote 'copied_bytes' many bytes to the new log file, we must move 'new_starting_pos'
            // to 'copied_bytes' ahead.
            new_starting_pos += copied_bytes;
        }
        new_log_writer.flush()?;

        // Now, it is time to delete previous log files as their details already moved to the new log.
        let mut stale_log_idx: Vec<u32> = Vec::new();

        // Do not directly delete from self.readers while reading it as it
        // will corrupt hash map structure.
        for i in &self.readers {
            stale_log_idx.push(*i.0);
        }
        println!("stale: {:?}", stale_log_idx);

        for i in &stale_log_idx {
            println!("removing: {}", &i);
            self.readers.remove(&i);
            fs::remove_file(self.path.join(format!("{}.log", i)))?;
        }

        self.uncompacted = 0;

        Ok(())
    }

    pub fn t(&mut self) {
        for i in &self.key_dir {
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
        println!("DONE WRITING ");

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
        if let Some(old_cmd) = self.key_dir.insert(k, cmd_pos) {
            self.uncompacted += old_cmd.len;
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            return self.compaction();
        }

        Ok(())
    }

    /// get runs get
    pub fn get(&mut self, input_key: String) -> Result<Option<String>> {
        let result = self.key_dir.get(&input_key).and_then(|pos| {
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

        if let Some(_) = self.key_dir.remove(&tmp_key) {
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
        let mut uncompacted = 0 as u64;
        for lf_idx in &log_files {
            let curr_file_path = p.join(format!("{}.log", lf_idx));
            let mut reader = BufReaderWithPos::new(File::open(&curr_file_path)?)?;

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
                        if let Some(old_cmd) = index.insert(
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
                    Command::Remove { key } => {
                        if let Some(old_cmd) = index.remove(&key) {
                            uncompacted += old_cmd.len;
                        }
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
            key_dir: index,
            writer: new_log_writer,
            path: p,
            uncompacted,
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
