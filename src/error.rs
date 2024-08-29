use std::io;

use failure::Fail;

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "Failed to find the key")]
    KeyNotFound,

    #[fail(display = "Failed to read or create the log file")]
    LogInit,

    #[fail(display = "{}", 0)]
    Parser(String),

    #[fail(display = "{}", 0)]
    IO(String),
}

impl From<serde_json::Error> for KvsError {
    fn from(value: serde_json::Error) -> KvsError {
        Self::Parser(value.to_string())
    }
}

impl From<io::Error> for KvsError {
    fn from(value: io::Error) -> Self {
        Self::IO(value.to_string())
    }
}

impl From<kvs_protocol::error::Error> for KvsError {
    fn from(value: kvs_protocol::error::Error) -> Self {
        Self::Parser(value.to_string())
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;
