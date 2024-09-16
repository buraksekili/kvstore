use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::{self},
    time::{Duration, Instant},
};

use crate::Result;
use crossbeam_queue::SegQueue;

use super::ThreadPool;

struct Worker {
    // id corresponds to the arbitrary id for the thread
    // useful while debugging :)
    id: usize,
    // thread is the actual thread which is going
    // to execute a real task.
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(
        id: usize,
        job_queue: Arc<SegQueue<Job>>,
        job_signal: Arc<(Mutex<bool>, Condvar)>,
        running: Arc<AtomicBool>,
    ) -> Worker {
        let thread = thread::spawn(move || loop {
            match job_queue.pop() {
                Some(Job::Task(task)) => task(),
                Some(Job::Shutdown) => {
                    break;
                }
                None => {
                    let (lock, cvar) = &*job_signal;
                    let mut job_available = lock.lock().unwrap();
                    while !*job_available && running.load(Ordering::Relaxed) {
                        job_available = cvar
                            .wait_timeout(job_available, Duration::from_millis(100))
                            .unwrap()
                            .0;
                    }
                    *job_available = false;
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}

pub enum Job {
    Task(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}

pub struct NaiveThreadPool {
    // workers keep track of all worker threads.
    workers: Vec<Worker>,
    // job_queue corresponds to a shared queue for distributing jobs to workers.
    job_queue: Arc<SegQueue<Job>>,
    // job_signal is notifier for workers when new jobs are available.
    job_signal: Arc<(Mutex<bool>, Condvar)>,
    // running indicates whether the threadpool is actively running or not.
    // it is mainly checked by worker threads to understand the status
    // of the pool.
    running: Arc<AtomicBool>,
}

impl ThreadPool for NaiveThreadPool {
    fn new(size: u32) -> Result<Self>
    where
        Self: Sized,
    {
        assert!(size > 0);

        let job_queue = Arc::new(SegQueue::new());
        let job_signal = Arc::new((Mutex::new(false), Condvar::new()));
        let mut workers = Vec::with_capacity(size as usize);
        let running = Arc::new(AtomicBool::new(true));

        for id in 0..size {
            workers.push(Worker::new(
                id as usize,
                Arc::clone(&job_queue),
                Arc::clone(&job_signal),
                Arc::clone(&running),
            ));
        }

        Ok(NaiveThreadPool {
            workers,
            job_queue,
            job_signal,
            running,
        })
    }

    fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // We create a new Job::Task, wrapping our closure 'f'
        let job = Job::Task(Box::new(f));
        // Push this job to our queue
        self.job_queue.push(job);
        // Signal that a new job is available
        let (lock, cvar) = &*self.job_signal;
        let mut job_available = lock.lock().unwrap();
        *job_available = true;
        cvar.notify_all();
    }
}

impl NaiveThreadPool {
    pub fn shutdown(&mut self, timeout: Duration) -> Result<()> {
        let start = Instant::now();
        // Step 1: Signal all workers to stop
        self.running.store(false, Ordering::SeqCst);

        // Step 2: Wake up all waiting threads
        let (lock, cvar) = &*self.job_signal;
        match lock.try_lock() {
            Ok(mut job_available) => {
                *job_available = true;
                cvar.notify_all();
            }
            Err(_) => {
                // We couldn't acquire the lock, but we've set running to false,
                // so workers will eventually notice
                println!("Warning: Couldn't acquire lock to notify workers. They will exit on their next timeout check.");
            }
        }

        // Step 3: Wait for all workers to finish
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                // Step 4: Calculate remaining time
                let remaining = timeout
                    .checked_sub(start.elapsed())
                    .unwrap_or(Duration::ZERO);

                // Step 5: Check if we've exceeded the timeout
                if remaining.is_zero() {
                    return Err(crate::KvsError::Pooling);
                }

                // Step 6: Wait for the worker to finish
                if thread.join().is_err() {
                    return Err(crate::KvsError::Pooling);
                }
            }
        }
        // Step 7: Final timeout check
        if start.elapsed() > timeout {
            Err(crate::KvsError::Pooling)
        } else {
            Ok(())
        }
    }
}

impl Drop for NaiveThreadPool {
    fn drop(&mut self) {
        if !self.workers.is_empty() {
            let _ = self.shutdown(Duration::from_secs(2));
        }
    }
}
