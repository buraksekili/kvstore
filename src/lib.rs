pub struct KvStore {}

impl KvStore {
    pub fn new() -> Self {
        return KvStore {};
    }

    pub fn set(&self, key: String, val: String) {
        panic!("unimplemented set")
    }
    pub fn get(&self, key: String) -> Option<String> {
        panic!("unimplemented get")
        // Some(String::from("hello world"))
    }
    pub fn remove(&self, key: String) {
        panic!("unimplemented remove")
    }
}
