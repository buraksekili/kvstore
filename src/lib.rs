//! A simple key/value store.
mod buf_reader;
mod buf_writer;
mod engine;
mod error;
mod kv;
pub mod server;
pub mod transport;

pub use engine::KvEngine;
mod data_format;

pub use error::{KvsError, Result};
pub use kv::KvStore;
