//! A simple key/value store.
mod buf_reader;
mod buf_writer;
mod error;
mod kv;
pub mod server;
pub mod transport;

pub use error::{KvsError, Result};
pub use kv::KvStore;
