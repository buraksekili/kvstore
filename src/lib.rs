#![deny(missing_docs)]
//! A simple key/value store.

/// import hashmap
use std::collections::HashMap;

/// KvStore implements in memory database.
#[derive(Default)]
pub struct KvStore {
    map: HashMap<String, String>,
}

/// KvStore implements in memory database.
impl KvStore {
    /// new does something
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
        }
    }

    /// set runs set
    pub fn set(&mut self, key: String, val: String) {
        self.map.insert(key.clone(), val.clone());
    }

    /// get runs get
    pub fn get(&self, key: String) -> Option<String> {
        return self.map.get(&key).cloned();
    }

    /// remove runs remove
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
