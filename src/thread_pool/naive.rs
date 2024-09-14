use std::{
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam_queue::SegQueue;

use super::ThreadPool;

type Job = Box<dyn FnOnce() -> () + Send + 'static>;

struct Worker {
    jh: JoinHandle<()>,
}

impl Worker {
    fn new(queue: Arc<SegQueue<Job>>) -> Worker {
        let jh = thread::spawn(move || loop {
            match queue.pop() {
                Some(t) => t(),
                None => {
                    thread::sleep(Duration::from_millis(1200));
                }
            }
        });
        Worker { jh }
    }
}

pub struct NaiveThreadPool {
    queue: Arc<SegQueue<Job>>,
    workers: Vec<Worker>,
}

impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let queue = Arc::new(SegQueue::new());
        let mut workers: Vec<Worker> = Vec::with_capacity(threads as usize);
        for _ in 0..=threads {
            workers.push(Worker::new(queue.clone()))
        }

        Ok(NaiveThreadPool { queue, workers })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let f = Box::new(job);
        self.queue.push(f);
    }
}
