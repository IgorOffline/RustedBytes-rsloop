//! Shared server state behind `PyServer`.
//!
//! A `ServerCore` owns its listeners, the accept workers, and the counters that
//! `wait_closed` waits on. TLS servers additionally admit only
//! `max_pending_tls_handshakes()` concurrent handshakes: `reserve_tls_handshake`
//! hands out a guard whose `Drop` releases the slot, so a handshake flood is
//! shed at accept time rather than exhausting worker threads.

use std::fs;
use std::net::TcpStream as StdTcpStream;
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use pyo3_async_runtimes::TaskLocals;

use super::platform::tcp_listener_raw_fd;
#[cfg(unix)]
use super::platform::unix_raw_fd;
#[cfg(unix)]
use super::run_unix_accept_loop;
use super::tuning::max_pending_tls_handshakes;
use super::worker::WorkerThread;
use super::{
    BlockingAcceptLoop, PendingTlsHandshake, ServerCore, ServerListener, run_server_accept_task,
    run_tcp_accept_loop, task_locals_for_loop,
};
use crate::context::{ensure_running_loop, run_in_context};
use crate::engine::{LoopCommand, LoopIoCommand};

impl ServerCore {
    pub(super) fn close_python_sockets(&self) {
        let _ = Python::try_attach(|py| -> PyResult<()> {
            for socket in &self.sockets {
                let _ = socket.bind(py).call_method0("close");
            }
            Ok(())
        });
    }

    pub(crate) fn report_error(&self, err: PyErr, message: &str) {
        let _ = Python::try_attach(|py| -> PyResult<()> {
            let context = PyDict::new(py);
            context.set_item("message", message)?;
            context.set_item("exception", err.value(py))?;
            self.loop_core.call_exception_handler(
                py,
                Some(&self.loop_obj),
                context.unbind().into_any(),
            )
        });
    }

    pub(super) fn create_protocol_with_py(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ensure_running_loop(py, &self.loop_obj)?;
        let callback = self.protocol_factory.bind(py).clone().unbind();
        let args = PyTuple::empty(py).unbind();
        run_in_context(py, &self.context, self.context_needs_run, &callback, &args)
    }

    #[inline]
    pub(super) fn locals(&self, py: Python<'_>) -> PyResult<TaskLocals> {
        task_locals_for_loop(py, &self.loop_obj)
    }

    #[inline]
    pub(super) fn is_closed(&self) -> bool {
        self.state.lock().expect("poisoned server state").closed
    }

    pub(super) fn is_serving(&self) -> bool {
        let state = self.state.lock().expect("poisoned server state");
        state.serving && !state.closed
    }

    #[inline]
    pub(super) fn connection_opened(&self) {
        self.active_connections.fetch_add(1, Ordering::SeqCst);
    }

    #[inline]
    pub(super) fn connection_lost(&self) {
        self.active_connections.fetch_sub(1, Ordering::SeqCst);
        self.closed_notify.notify_all();
    }

    pub(super) fn reserve_tls_handshake(self: &Arc<Self>) -> Option<PendingTlsHandshake> {
        if self.is_closed() {
            return None;
        }
        let limit = max_pending_tls_handshakes();
        let reserved = self.pending_tls_handshakes.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| (current < limit).then_some(current + 1),
        );
        reserved.ok().map(|_| PendingTlsHandshake {
            server: Arc::clone(self),
        })
    }

    pub(super) fn close(&self) {
        {
            let mut state = self.state.lock().expect("poisoned server state");
            if state.closed {
                return;
            }
            state.closed = true;
            state.serving = false;
            state.listeners.clear();
        }

        // Blocking TLS accept workers are woken by connecting to their listening
        // address.  Keep the exposed Python socket alive until after that wake:
        // on Windows duplicated sockets share listener state, so closing the
        // Python handle first makes the wake connect pay the TCP failure timeout.
        for task in self
            .accept_tasks
            .lock()
            .expect("poisoned accept tasks")
            .drain(..)
        {
            task.abort();
        }
        for fd in self
            .accept_fds
            .lock()
            .expect("poisoned accept fds")
            .drain(..)
        {
            if self.loop_core.on_runtime_thread() {
                self.loop_core.stop_io_task(fd);
            } else {
                let _ = self
                    .loop_core
                    .send_command(LoopCommand::Io(LoopIoCommand::StopServerAccept(fd)));
            }
        }

        self.close_python_sockets();

        if let Some(path) = &self.cleanup_path {
            let _ = fs::remove_file(path);
        }

        self.closed_notify.notify_all();
    }

    pub fn spawn_accept_tasks(self: &Arc<Self>) {
        let listeners = {
            let mut state = self.state.lock().expect("poisoned server state");
            if state.closed || state.serving {
                return;
            }
            state.serving = true;
            std::mem::take(&mut state.listeners)
        };

        if self.tls.is_some() {
            let mut tasks = self.accept_tasks.lock().expect("poisoned accept tasks");
            for listener in listeners {
                let server = Arc::clone(self);
                let task = match listener {
                    ServerListener::Tcp(listener) => {
                        let wake_addr = listener.local_addr().ok();
                        WorkerThread::spawn_interruptible(
                            "rsloop-tcp-accept",
                            move || {
                                if let Some(addr) = wake_addr {
                                    let _ = StdTcpStream::connect(addr);
                                }
                            },
                            move |stop| {
                                run_tcp_accept_loop(BlockingAcceptLoop::new(server, listener, stop))
                            },
                        )
                    }
                    #[cfg(unix)]
                    ServerListener::Unix(listener) => {
                        let wake_path = listener
                            .local_addr()
                            .ok()
                            .and_then(|addr| addr.as_pathname().map(PathBuf::from));
                        WorkerThread::spawn_interruptible(
                            "rsloop-unix-accept",
                            move || {
                                if let Some(path) = wake_path {
                                    let _ = StdUnixStream::connect(path);
                                }
                            },
                            move |stop| {
                                run_unix_accept_loop(BlockingAcceptLoop::new(
                                    server, listener, stop,
                                ))
                            },
                        )
                    }
                };
                match task {
                    Ok(task) => tasks.push(task),
                    Err(err) => self.report_error(
                        PyRuntimeError::new_err(err.to_string()),
                        "failed to spawn server accept worker",
                    ),
                }
            }
            return;
        }

        let mut accept_fds = self.accept_fds.lock().expect("poisoned accept fds");
        for listener in listeners {
            let fd = match &listener {
                ServerListener::Tcp(listener) => tcp_listener_raw_fd(listener),
                #[cfg(unix)]
                ServerListener::Unix(listener) => unix_raw_fd(listener.as_raw_fd()),
            };
            accept_fds.push(fd);
            let server = Arc::clone(self);
            // On the loop thread, host the accept loop directly on the loop's
            // own runtime so accepted connections are delivered without a
            // cross-thread hop. Off-thread callers fall back to the transitional
            // runtime-thread command path.
            if self.loop_core.on_runtime_thread() {
                self.loop_core
                    .spawn_io_tracked(fd, run_server_accept_task(server, listener));
            } else {
                let _ = self.loop_core.send_command(LoopCommand::Io(
                    LoopIoCommand::StartServerAccept {
                        fd,
                        server,
                        listener,
                    },
                ));
            }
        }
    }
}
