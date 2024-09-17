use std::env::{self};

use kvs::{
    server::MyKvServer,
    thread_pool::{SharedQueueThreadPool, ThreadPool},
    Result,
};
use log::{self, debug, info};

fn main() -> Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info")
    }

    env_logger::init();

    debug!("DEBUGGING ENABLED");
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));

    let pool = SharedQueueThreadPool::new(48).unwrap();

    let s = MyKvServer::new();
    s.start("127.0.0.1:4000".to_string(), pool)?;

    Ok(())
}
