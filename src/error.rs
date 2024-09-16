use std::{io, string::FromUtf8Error};

use failure::Fail;

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "Failed to find the key")]
    KeyNotFound,

    #[fail(display = "Failed to read or create the log file")]
    LogInit,

    #[fail(display = "Failed to parse {}", 0)]
    Parser(String),

    #[fail(display = "Failed to deserialize {}, err: {}", 0, 1)]
    KvsDeserializer(String, String),

    #[fail(display = "sled error: {}", _0)]
    Sled(#[cause] sled::Error),

    #[fail(display = "{}", 0)]
    IO(String),

    #[fail(display = "")]
    Pooling,

    #[fail(display = "failed to handle tcp request, err: {}", 0)]
    TCP(String),

    /// Key or value is invalid UTF-8 sequence
    #[fail(display = "UTF-8 error: {}", _0)]
    Utf8(#[cause] FromUtf8Error),

    /// Key or value is invalid UTF-8 sequence
    #[fail(display = "Unexpected  {}", _0)]
    UnexpectedCommandType(String),
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

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> KvsError {
        KvsError::Sled(err)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> KvsError {
        KvsError::Utf8(err)
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;
