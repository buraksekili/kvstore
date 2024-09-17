use crate::Result;

mod kv;
mod sled;
pub use self::kv::CommandPos;
pub use self::kv::KvStore;
pub use self::kv::KvsReader;
pub use self::sled::SledKvsEngine;

pub trait KvsEngine: Clone + Send + 'static {
    fn set(&self, key: String, value: String) -> Result<()>;
    fn get(&self, key: String) -> Result<Option<String>>;
    fn remove(&self, key: String) -> Result<()>;
}
