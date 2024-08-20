use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Get { key: String },
    Set { key: String, val: String },
    Rm { key: String },
}

#[derive(Serialize, Deserialize)]
pub struct Response {
    Error: Option<String>,
    Result: String,
}
