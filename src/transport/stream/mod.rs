//! Stream, server, socket, pipe, and TLS transport implementation.
//!
//! This file holds the vocabulary the whole transport shares — the two owner
//! types (`StreamTransportCore`, `ServerCore`), the state they guard, the
//! commands and events passed between threads, and the two pyclasses Python
//! sees. Everything that *acts* on them lives in a submodule:
//!
//! - construction: [`socket_transport`], [`pipe_transport`], [`tls_transport`],
//!   [`server`], with the shared assembly in [`builder`]
//! - the core's own behaviour, split by concern: [`core_events`],
//!   [`core_protocol`], [`core_state`], [`core_write`], and [`server_core`]
//! - the Python surface: [`py_transport`], [`py_server`], and the native fast
//!   streams in [`fast`]
//! - I/O: [`accept`], [`connect`], [`reader`], [`reader_task`], [`writer`], and
//!   the TLS session plumbing in [`tls_session`]
//!
//! Submodules reach back into the private fields declared here, which is why
//! those stay private: the module tree is the encapsulation boundary, not the
//! individual file.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Condvar, Mutex, Weak};

use pyo3::prelude::*;

use super::tls::ServerTlsSettings;
use crate::async_event::AsyncEvent;
use crate::engine::LoopCore;
use crate::fd_ops;

mod accept;
mod buffers;
mod builder;
mod connect;
mod core_events;
mod core_protocol;
mod core_state;
mod core_write;
mod fast;
mod io_targets;
mod pipe_transport;
mod platform;
mod poll;
mod protocol;
mod py_server;
mod py_transport;
mod reader;
mod reader_task;
mod server;
mod server_core;
mod server_types;
mod socket_transport;
mod stats;
mod tls_session;
mod tls_transport;
mod tuning;
mod worker;
mod writer;

#[cfg(unix)]
use accept::run_unix_accept_loop;
use accept::{BlockingAcceptLoop, run_tcp_accept_loop};
pub(crate) use accept::{run_server_accept_task, spawn_accepted_transport_with_py};
use buffers::{OwnedWriteBuffer, ReadBufferPool};
use builder::make_stream_extra;
pub use builder::task_locals_for_loop;
pub(crate) use connect::run_connect_watch_task;
pub use fast::{PyFastStreamReader, PyFastStreamWriter, open_connection, start_server};
pub use io_targets::ReaderTarget;
use io_targets::{LazyWriterConfig, TaskedDirectWriter};
pub use pipe_transport::{spawn_read_pipe_transport, spawn_write_pipe_transport};
use protocol::ProtocolCallbacks;
pub(crate) use reader::run_socket_reader_blocking;
use reader::{
    spawn_reader_worker, spawn_socket_reader, spawn_tls_reader_worker, stop_socket_reader,
    stop_socket_reader_nowait,
};
pub(crate) use reader_task::run_tcp_socket_reader_task;
#[cfg(unix)]
pub(crate) use reader_task::run_unix_socket_reader_task;
pub use server::{create_server, tcp_server_listener};
#[cfg(unix)]
pub use server::{remove_unix_socket_if_present, unix_server_listener};
pub use server_types::{AcceptedStream, ServerCreateParams, ServerListener, TransportSpawnContext};
#[cfg(unix)]
use socket_transport::duplicate_unix_direct_writer;
use socket_transport::{duplicate_configured_tcp_stream, tcp_stream_from_owned_socket_fd};
pub use socket_transport::{
    tcp_listener_from_owned_socket_fd, transport_from_socket, transport_from_socket_server_tls,
    transport_from_socket_tls,
};
#[cfg(unix)]
pub use socket_transport::{unix_listener_from_owned_socket_fd, unix_stream_from_owned_socket_fd};
pub use stats::{reset_transport_stats, transport_stats};
pub use tls_transport::{prepare_start_tls_transport, start_tls_transport};
use tuning::{
    DEFAULT_WRITE_BUFFER_HIGH_WATER, DEFAULT_WRITE_BUFFER_LOW_WATER, max_pending_tls_handshakes,
};
use worker::WorkerThread;
use writer::{spawn_tls_writer_worker, spawn_writer_worker};

enum WriterCommand {
    Data(OwnedWriteBuffer),
    WriteEof,
    Close,
    Abort,
    Stop,
}

enum PendingReadEvent {
    Data(Vec<u8>),
    Eof,
    ConnectionLost(Option<String>),
    PauseWriting,
    ResumeWriting,
}

struct StreamTransportState {
    io_fd: Option<fd_ops::RawFd>,
    runtime_socket_io: bool,
    protocol: Py<PyAny>,
    callbacks: ProtocolCallbacks,
    context: Py<PyAny>,
    context_needs_run: bool,
    extra: HashMap<String, Py<PyAny>>,
    lazy_socket_family: Option<i32>,
    closing: bool,
    read_paused: bool,
    read_backpressured: bool,
    reading: bool,
    writable: bool,
    write_eof_requested: bool,
    can_write_eof: bool,
    close_on_write_eof: bool,
    lost_called: bool,
    writer_registered: bool,
    write_buffer: StreamWriteBufferState,
    server: Option<Weak<ServerCore>>,
}

struct StreamWriteBufferState {
    size: usize,
    high_water: usize,
    low_water: usize,
    protocol_paused: bool,
}

impl Default for StreamWriteBufferState {
    fn default() -> Self {
        Self {
            size: 0,
            high_water: DEFAULT_WRITE_BUFFER_HIGH_WATER,
            low_water: DEFAULT_WRITE_BUFFER_LOW_WATER,
            protocol_paused: false,
        }
    }
}

/// Shared stream owner. I/O paths enqueue events; the loop thread drains them
/// and invokes the Python protocol in order.
pub struct StreamTransportCore {
    loop_core: Arc<LoopCore>,
    loop_obj: Py<PyAny>,
    state: Mutex<StreamTransportState>,
    pending_read_events: Mutex<VecDeque<PendingReadEvent>>,
    read_buffer_pool: Arc<ReadBufferPool>,
    pending_read_bytes: AtomicUsize,
    read_events_scheduled: AtomicBool,
    reading: AtomicBool,
    detached: AtomicBool,
    writer_tx: Sender<WriterCommand>,
    direct_writer: Option<Mutex<Option<TaskedDirectWriter>>>,
    pending_direct_write: Mutex<Vec<u8>>,
    direct_write_scheduled: AtomicBool,
    #[cfg(windows)]
    poll_reader_requested: AtomicBool,
    #[cfg(windows)]
    poll_reader_ready: AtomicBool,
    lazy_writer: Mutex<Option<LazyWriterConfig>>,
    workers: Mutex<Vec<WorkerThread>>,
    // Signaled whenever `read_paused` clears or the transport starts
    // closing, so a paused reader worker resumes immediately instead of
    // sleeping through a poll interval.
    state_cv: Condvar,
    read_state_notify: AsyncEvent,
    // The extra map is fixed at construction; cache the text-mode marker so
    // the per-write hot path avoids a state lock plus hash lookup.
    has_text_encoding: bool,
    #[cfg(windows)]
    server_side: bool,
    coalesce_small_server_writes: bool,
}

struct ServerState {
    closed: bool,
    serving: bool,
    listeners: Vec<ServerListener>,
}

/// Shared server owner for listeners, accept workers, and active connections.
pub struct ServerCore {
    loop_core: Arc<LoopCore>,
    loop_obj: Py<PyAny>,
    protocol_factory: Py<PyAny>,
    context: Py<PyAny>,
    context_needs_run: bool,
    sockets: Vec<Py<PyAny>>,
    state: Mutex<ServerState>,
    accept_tasks: Mutex<Vec<WorkerThread>>,
    accept_fds: Mutex<Vec<fd_ops::RawFd>>,
    active_connections: AtomicUsize,
    pending_tls_handshakes: AtomicUsize,
    tls_overload_reported: AtomicBool,
    closed_notify: AsyncEvent,
    cleanup_path: Option<PathBuf>,
    tls: Option<Arc<ServerTlsSettings>>,
}

struct PendingTlsHandshake {
    server: Arc<ServerCore>,
}

impl Drop for PendingTlsHandshake {
    fn drop(&mut self) {
        let previous = self
            .server
            .pending_tls_handshakes
            .fetch_sub(1, Ordering::AcqRel);
        if previous.saturating_sub(1) < max_pending_tls_handshakes() / 2 {
            self.server
                .tls_overload_reported
                .store(false, Ordering::Release);
        }
        self.server.closed_notify.notify_all();
    }
}

#[pyclass(name = "Server", module = "rsloop._loop")]
pub struct PyServer {
    pub core: Arc<ServerCore>,
}

#[pyclass(name = "StreamTransport", module = "rsloop._loop")]
pub struct PyStreamTransport {
    pub core: Arc<StreamTransportCore>,
}
