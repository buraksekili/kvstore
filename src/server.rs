use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    net::TcpListener,
};

use log::{debug, error, info};

use crate::{engine::KvsEngine, thread_pool::ThreadPool, transport::Response, Result};
use kvs_protocol::{deserializer::deserialize, request::Request};

pub struct KvServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    addr: String,
    thread_pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvServer<E, P> {
    pub fn new(engine: E, addr: String, thread_pool: P) -> KvServer<E, P> {
        KvServer {
            engine,
            addr,
            thread_pool,
        }
    }

    pub fn start(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr)?;
        for stream in listener.incoming() {
            let engine = self.engine.clone();
            self.thread_pool.spawn(move || match stream {
                Ok(stream) => {
                    if let Err(e) = handle_client_req(engine, stream) {
                        error!("Error on serving client: {}", e);
                    }
                }
                Err(e) => error!("Connection failed: {}", e),
            })
        }

        Ok(())
    }
}

fn handle_client_req<E>(engine: E, stream: std::net::TcpStream) -> Result<()>
where
    E: KvsEngine,
{
    info!("==> New request!");
    let mut request_reader = BufReader::new(stream.try_clone().unwrap());
    let mut response_writer = BufWriter::new(stream);

    // TODO: error handling in the read_line
    let mut buf = String::new();
    if let Err(err) = request_reader.read_line(&mut buf) {
        return Err(crate::KvsError::TCP(err.to_string()));
    }

    match deserialize::<Request>(buf.as_str()) {
        Err(e) => {
            error!("failed to deserialize the request, err: {}", e);
            Err(crate::KvsError::TCP(e.to_string()))
        }
        Ok(req) => {
            match &req {
                Request::Get { key } => {
                    info!("==> GET request {} ", key);
                    if let Ok(v) = engine.get(key.to_string()) {
                        let mut resp: Response = Response {
                            ..Default::default()
                        };
                        if let Some(val) = v {
                            resp.result = val.clone();
                        } else {
                            resp.error = Some("Key not found".to_string());
                        }
                        info!("==> DONE GET request {} -> {:?}", key, resp);

                        serde_json::to_writer(&mut response_writer, &resp).unwrap();
                        response_writer.flush().unwrap(); // TODO
                    } else {
                        info!("no response:");
                    }
                }
                Request::Set { key, val } => {
                    info!("==> SET request {} {} ", key, val);

                    match engine.set(key.to_string(), val.to_string()) {
                        Ok(_) => {
                            debug!("key: '{}' with value: '{}' inserted succesfully", key, val)
                        }
                        Err(e) => error!("failed to write key: '{}', err: {}", key, e),
                    }
                    info!("==> DONE SET request {} {} ", key, val);
                }
                Request::Rm { key } => {
                    info!("==> RM request {} ", key);

                    let mut resp: Response = Response {
                        ..Default::default()
                    };

                    if let Err(e) = engine.remove(key.to_string()) {
                        error!("failed to remove the key, err: {}", e);

                        resp.error = Some("Key not found".to_string());
                    }

                    info!("==> DONE RM request {} ", key);
                    serde_json::to_writer(&mut response_writer, &resp).unwrap();
                    response_writer.flush().unwrap(); // TODO
                }
            };
            Ok(())
        }
    }
}
