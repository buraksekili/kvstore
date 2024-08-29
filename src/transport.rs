use serde::{Deserialize, Serialize};

// #[derive(Serialize, Deserialize, Debug)]
// pub enum Request {
//     Get { key: String },
//     Set { key: String, val: String },
//     Rm { key: String },
// }

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Response {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub error: Option<String>,
    pub result: String,
}
