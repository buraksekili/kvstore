use crate::Result;

pub trait KvsEngine {
    fn set(&mut self, k: String, val: String) -> Result<()>;
    fn get(&mut self, input_key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}
