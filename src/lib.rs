//! A simple key/value store.
mod error;
mod kv;

pub use error::{KvsError, Result};
pub use kv::KvStore;
