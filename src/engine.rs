use crate::Result;

pub trait KvEngine {
    fn set(&mut self, k: String, val: String) -> Result<()>;
}
