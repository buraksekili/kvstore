use std::{
    io::{BufReader, BufWriter, Write},
    net::TcpListener,
};

use log::{debug, error, info};
use serde_json::Deserializer;

use crate::{
    engine::KvEngine,
    transport::{Request, Response},
    Result,
};

pub struct KvServer<E: KvEngine> {
    engine: E,
    addr: String,
}

impl<E: KvEngine> KvServer<E> {
    pub fn new(engine: E, addr: String) -> KvServer<E> {
        KvServer { engine, addr }
    }

    pub fn start(mut self) -> Result<()> {
        debug!("Trying to listen on {}", self.addr);
        let listener = TcpListener::bind(&self.addr)?;
        debug!("Listening on: {}", self.addr);

        // accept connections and process them serially
        println!("start streaming");
        for stream in listener.incoming() {
            println!("handling the stream");
            match stream {
                Ok(s) => self.handle_client_req(s),
                Err(e) => error!("failed to parse client request, err: {}", e),
            }
        }

        Ok(())
    }

    fn handle_client_req(&mut self, stream: std::net::TcpStream) {
        debug!("handling the request started");
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = BufWriter::new(stream);

        debug!("2");

        // It is expected that the input stream ends after the deserialized object.
        // If the stream does not end, such as in the case of a persistent socket connection,
        // this function will not return.
        // It is possible instead to deserialize from a prefix of an input stream without
        // looking for EOF by managing your own Deserializer.
        // reference: https://docs.rs/serde_json/latest/serde_json/fn.from_reader.html
        let mut de = serde_json::Deserializer::from_reader(reader).into_iter::<Request>();
        for req_ in de {
            if let Ok(req) = req_ {
                debug!("handling the request{:?}", req);

                match &req {
                    Request::Get { key } => {
                        debug!("we got GET request");
                        if let Ok(v) = self.engine.get(key.to_string()) {
                            info!("got response: {:?}", v);
                            let mut resp: Response = Response {
                                ..Default::default()
                            };
                            if let Some(val) = v {
                                resp.Result = val.clone();
                                debug!("wrote {} ", val);
                            } else {
                                resp.Error = Some("Key not found".to_string());
                            }

                            serde_json::to_writer(&mut writer, &resp).unwrap();
                            writer.flush().unwrap(); // TODO
                        } else {
                            info!("no response:");
                        }
                    }
                    Request::Set { key, val } => {
                        debug!("we got SET request");
                        if let Ok(v) = self.engine.set(key.to_string(), val.to_string()) {
                            info!("got response: {:?}", v);
                        } else {
                            info!("no response:");
                        }
                    }
                    Request::Rm { key } => {
                        debug!("we got RM request");
                        if let Err(e) = self.engine.remove(key.to_string()) {
                            let mut resp: Response = Response {
                                ..Default::default()
                            };

                            resp.Error = Some("Key not found".to_string());

                            serde_json::to_writer(&mut writer, &resp).unwrap();
                            writer.flush().unwrap(); // TODO
                        }
                    }
                }
            }
        }
    }
}
