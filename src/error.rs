use failure::Fail;

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "Failed to find the key")]
    KeyNotFound,

    #[fail(display = "Failed to read or create the log file")]
    LogInit,
}

pub type Result<T> = std::result::Result<T, KvsError>;
