use std::io::BufReader;

use log::debug;

use crate::transport::Request;

pub fn handle_client_req(s: std::net::TcpStream) {
    debug!("handling the request");

    let mut reader = BufReader::new(s);
    let req: Request = serde_json::from_reader(reader).unwrap();

    match &req {
        Request::Get { key } => debug!("we got GET request"),
        Request::Set { key, val } => debug!("we got SET request"),
    }

    debug!("The request: {:?}", req);
}
