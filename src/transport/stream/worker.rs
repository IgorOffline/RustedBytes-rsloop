//! Blocking helper threads owned by a transport or server.
//!
//! Readers, writers, and blocking accept loops all run on plain OS threads
//! rather than the loop runtime. `WorkerThread` gives them a uniform stop
//! protocol: set the flag, optionally wake a thread parked in a syscall, then
//! join with a timeout so one stuck worker cannot block loop shutdown.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;

use super::tuning::WORKER_JOIN_TIMEOUT;

pub(super) struct WorkerThread {
    stop: Arc<AtomicBool>,
    wake: Option<Box<dyn FnOnce() + Send>>,
    join: thread::JoinHandle<()>,
    done_rx: Receiver<()>,
}

impl WorkerThread {
    pub(super) fn spawn(
        name: &'static str,
        task: impl FnOnce(Arc<AtomicBool>) + Send + 'static,
    ) -> io::Result<Self> {
        Self::spawn_with_stack(name, None, task)
    }

    pub(super) fn spawn_with_stack(
        name: &'static str,
        stack_size: Option<usize>,
        task: impl FnOnce(Arc<AtomicBool>) + Send + 'static,
    ) -> io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let mut builder = thread::Builder::new().name(name.to_owned());
        if let Some(stack_size) = stack_size {
            builder = builder.stack_size(stack_size);
        }
        let (done_tx, done_rx) = mpsc::channel();
        let join = builder.spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                task(thread_stop);
            }));
            let _ = done_tx.send(());
            if let Err(payload) = result {
                std::panic::resume_unwind(payload);
            }
        })?;
        Ok(Self {
            stop,
            wake: None,
            join,
            done_rx,
        })
    }

    pub(super) fn spawn_interruptible(
        name: &'static str,
        wake: impl FnOnce() + Send + 'static,
        task: impl FnOnce(Arc<AtomicBool>) + Send + 'static,
    ) -> io::Result<Self> {
        let mut worker = Self::spawn(name, task)?;
        worker.wake = Some(Box::new(wake));
        Ok(worker)
    }

    pub(super) fn abort(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(wake) = self.wake.take() {
            wake();
        }
        if self.join.thread().id() == thread::current().id() {
            return;
        }
        match self.done_rx.recv_timeout(WORKER_JOIN_TIMEOUT) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => {
                let _ = self.join.join();
            }
            Err(RecvTimeoutError::Timeout) => {
                // Detach a stuck worker instead of blocking loop shutdown.
            }
        }
    }
}
