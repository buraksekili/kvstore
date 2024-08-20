use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Get { key: String },
    Set { key: String, val: String },
    Rm { key: String },
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Response {
    pub Error: Option<String>,
    pub Result: String,
}
