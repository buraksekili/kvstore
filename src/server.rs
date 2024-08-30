use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    net::TcpListener,
};

use log::{debug, error, info};

use crate::{engine::KvsEngine, transport::Response, Result};
use kvs_protocol::request::Request;

pub struct KvServer<E: KvsEngine> {
    engine: E,
    addr: String,
}

impl<E: KvsEngine> KvServer<E> {
    pub fn new(engine: E, addr: String) -> KvServer<E> {
        KvServer { engine, addr }
    }

    pub fn start(mut self) -> Result<()> {
        debug!("Trying to listen on {}", self.addr);
        let listener = TcpListener::bind(&self.addr)?;
        info!("Listening on: {}", self.addr);

        for stream in listener.incoming() {
            match stream {
                Ok(s) => self.handle_client_req(s),
                Err(e) => error!("failed to parse client request, err: {}", e),
            }
        }

        Ok(())
    }

    fn handle_client_req(&mut self, stream: std::net::TcpStream) {
        let mut request_reader = BufReader::new(stream.try_clone().unwrap());
        let mut response_writer = BufWriter::new(stream);

        // TODO: error handling in the read_line
        let mut buf = String::new();
        match request_reader.read_line(&mut buf) {
            Err(e) => error!("failed to read, err: {}", e),
            _ => {}
        };

        debug!("received a client request: {}", &buf);

        match kvs_protocol::deserializer::deserialize::<kvs_protocol::request::Request>(
            buf.as_str(),
        ) {
            Err(e) => error!("failed to deserialize the request, err: {}", e),
            Ok(req) => {
                match &req {
                    Request::Get { key } => {
                        if let Ok(v) = self.engine.get(key.to_string()) {
                            let mut resp: Response = Response {
                                ..Default::default()
                            };
                            if let Some(val) = v {
                                resp.result = val.clone();
                            } else {
                                resp.error = Some("Key not found".to_string());
                            }

                            serde_json::to_writer(&mut response_writer, &resp).unwrap();
                            response_writer.flush().unwrap(); // TODO
                        } else {
                            info!("no response:");
                        }
                    }
                    Request::Set { key, val } => {
                        match self.engine.set(key.to_string(), val.to_string()) {
                            Ok(_) => {
                                debug!("key: '{}' with value: '{}' inserted succesfully", key, val)
                            }
                            Err(e) => error!("failed to write key: '{}', err: {}", key, e),
                        }
                    }
                    Request::Rm { key } => {
                        let mut resp: Response = Response {
                            ..Default::default()
                        };

                        if let Err(e) = self.engine.remove(key.to_string()) {
                            error!("failed to remove the key, err: {}", e);

                            resp.error = Some("Key not found".to_string());
                        }

                        serde_json::to_writer(&mut response_writer, &resp).unwrap();
                        response_writer.flush().unwrap(); // TODO
                    }
                }
            }
        }
    }
}
