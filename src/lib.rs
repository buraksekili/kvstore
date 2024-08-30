//! A simple key/value store.
mod buf_reader;
mod buf_writer;
mod engine;
mod error;
pub mod server;
pub mod transport;

mod data_format;

pub use engine::{KvStore, KvsEngine, SledKvsEngine};
pub use error::{KvsError, Result};
