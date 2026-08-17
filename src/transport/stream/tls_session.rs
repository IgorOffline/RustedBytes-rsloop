//! rustls session state and the record-level plumbing around it.
//!
//! `TlsIoState` pairs a rustls connection with the socket it speaks over, and
//! everything here operates on that pair under one lock: the initial blocking
//! handshake, moving records in and out of the connection, draining decrypted
//! plaintext into the transport's read queue, and the close-notify shutdown.
//! Keeping the lock discipline in a single module is what lets the reader and
//! writer workers share one session without stepping on each other.

use std::io::{self, Read as _, Write as _};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::Duration;

use rustls::{ClientConnection, ServerConnection};

use super::io_targets::StreamKind;
use super::poll::{wait_socket_ready, wait_socket_ready_once, wait_socket_ready_until};
use super::{PendingReadEvent, ServerCore, StreamTransportCore};
use crate::fd_ops;

pub(super) enum TlsConnectionKind {
    Client(ClientConnection),
    Server(ServerConnection),
}

impl TlsConnectionKind {
    pub(super) fn is_handshaking(&self) -> bool {
        match self {
            Self::Client(conn) => conn.is_handshaking(),
            Self::Server(conn) => conn.is_handshaking(),
        }
    }

    pub(super) fn wants_read(&self) -> bool {
        match self {
            Self::Client(conn) => conn.wants_read(),
            Self::Server(conn) => conn.wants_read(),
        }
    }

    pub(super) fn wants_write(&self) -> bool {
        match self {
            Self::Client(conn) => conn.wants_write(),
            Self::Server(conn) => conn.wants_write(),
        }
    }

    pub(super) fn read_tls(&mut self, stream: &mut StreamKind) -> io::Result<usize> {
        match self {
            Self::Client(conn) => conn.read_tls(stream),
            Self::Server(conn) => conn.read_tls(stream),
        }
    }

    pub(super) fn write_tls(&mut self, stream: &mut StreamKind) -> io::Result<usize> {
        match self {
            Self::Client(conn) => conn.write_tls(stream),
            Self::Server(conn) => conn.write_tls(stream),
        }
    }

    pub(super) fn process_new_packets(&mut self) -> Result<(), rustls::Error> {
        match self {
            Self::Client(conn) => conn.process_new_packets().map(|_| ()),
            Self::Server(conn) => conn.process_new_packets().map(|_| ()),
        }
    }

    pub(super) fn reader_read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Client(conn) => conn.reader().read(buf),
            Self::Server(conn) => conn.reader().read(buf),
        }
    }

    pub(super) fn writer_write_all(&mut self, data: &[u8]) -> io::Result<()> {
        match self {
            Self::Client(conn) => conn.writer().write_all(data),
            Self::Server(conn) => conn.writer().write_all(data),
        }
    }

    pub(super) fn send_close_notify(&mut self) {
        match self {
            Self::Client(conn) => conn.send_close_notify(),
            Self::Server(conn) => conn.send_close_notify(),
        }
    }
}

pub(super) struct TlsIoState {
    pub(super) stream: StreamKind,
    pub(super) connection: TlsConnectionKind,
    pub(super) shutdown_timeout: Duration,
}

pub(super) type SharedTlsIoState = Arc<Mutex<TlsIoState>>;

impl TlsIoState {
    #[inline]
    pub(super) fn fd(&self) -> fd_ops::RawFd {
        self.stream.fd()
    }

    pub(super) fn pollable(&self) -> bool {
        self.stream.pollable()
    }

    #[inline]
    pub(super) fn shutdown_close(&self) -> io::Result<()> {
        self.stream.shutdown_close()
    }

    #[inline]
    pub(super) fn read_tls(&mut self) -> io::Result<usize> {
        self.connection.read_tls(&mut self.stream)
    }

    #[inline]
    pub(super) fn write_tls(&mut self) -> io::Result<usize> {
        self.connection.write_tls(&mut self.stream)
    }
}

pub(super) enum TlsReadOutcome {
    Continue,
    Eof,
    ConnectionLost(String),
}

pub(super) fn tls_server_closed(server: Option<&Weak<ServerCore>>) -> bool {
    server.is_some_and(|server| server.upgrade().is_none_or(|server| server.is_closed()))
}

pub(super) fn complete_tls_handshake(
    tls_state: &SharedTlsIoState,
    timeout: Duration,
    server: Option<&Weak<ServerCore>>,
) -> io::Result<()> {
    profiling::scope!("stream.complete_tls_handshake");
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if tls_server_closed(server) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "TLS handshake cancelled",
            ));
        }
        if std::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "TLS handshake timed out",
            ));
        }

        if tls_handshake_step(tls_state)? {
            return Ok(());
        }
    }
}

pub(super) fn tls_handshake_step(tls_state: &SharedTlsIoState) -> io::Result<bool> {
    let mut state = tls_state.lock().expect("poisoned tls state");
    if !state.connection.is_handshaking() {
        if state.connection.wants_write() {
            flush_tls_io_locked(&mut state)?;
        }
        return Ok(true);
    }

    if state.connection.wants_write() {
        flush_tls_io_locked(&mut state)?;
        return Ok(false);
    }

    if state.connection.wants_read() {
        let fd = state.fd();
        let pollable = state.pollable();
        drop(state);
        continue_tls_handshake_read(tls_state, fd, pollable)?;
        return Ok(false);
    }

    thread::sleep(Duration::from_millis(10));
    Ok(false)
}

pub(super) fn continue_tls_handshake_read(
    tls_state: &SharedTlsIoState,
    fd: fd_ops::RawFd,
    pollable: bool,
) -> io::Result<()> {
    if !wait_socket_ready_once(fd, pollable, true, false)? {
        return Ok(());
    }
    let mut state = tls_state.lock().expect("poisoned tls state");
    let n = state.read_tls()?;
    if n == 0 && state.connection.is_handshaking() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "TLS handshake ended before completion",
        ));
    }
    state.connection.process_new_packets().map_err(tls_io_error)
}

pub(super) fn drain_tls_plaintext_locked(
    core: &Arc<StreamTransportCore>,
    state: &mut TlsIoState,
    plaintext: &mut [u8],
) -> Result<bool, String> {
    let mut saw_data = false;
    loop {
        match state.connection.reader_read(plaintext) {
            Ok(0) => break,
            Ok(n) => {
                saw_data = true;
                core.enqueue_pending_read_event(PendingReadEvent::Data(plaintext[..n].to_vec()));
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err.to_string()),
        }
    }
    Ok(saw_data)
}

pub(super) fn drain_buffered_tls_plaintext(
    core: &Arc<StreamTransportCore>,
    tls_state: &SharedTlsIoState,
    plaintext: &mut [u8],
) -> TlsReadOutcome {
    let mut state = tls_state.lock().expect("poisoned tls state");
    match drain_tls_plaintext_locked(core, &mut state, plaintext) {
        Ok(true) => TlsReadOutcome::Continue,
        Ok(false) => TlsReadOutcome::Eof,
        Err(err) => TlsReadOutcome::ConnectionLost(err),
    }
}

pub(super) fn read_tls_records(
    core: &Arc<StreamTransportCore>,
    tls_state: &SharedTlsIoState,
    plaintext: &mut [u8],
) -> TlsReadOutcome {
    let mut state = tls_state.lock().expect("poisoned tls state");
    match state.read_tls() {
        Ok(0) => {
            if let Err(err) = state.connection.process_new_packets().map_err(tls_io_error) {
                return TlsReadOutcome::ConnectionLost(err.to_string());
            }
            match drain_tls_plaintext_locked(core, &mut state, plaintext) {
                Ok(true) => TlsReadOutcome::Continue,
                Ok(false) => TlsReadOutcome::Eof,
                Err(err) => TlsReadOutcome::ConnectionLost(err),
            }
        }
        Ok(_) => {
            if let Err(err) = state.connection.process_new_packets().map_err(tls_io_error) {
                return TlsReadOutcome::ConnectionLost(err.to_string());
            }
            if let Err(err) = flush_tls_io_locked(&mut state) {
                return TlsReadOutcome::ConnectionLost(err.to_string());
            }
            match drain_tls_plaintext_locked(core, &mut state, plaintext) {
                Ok(_) => TlsReadOutcome::Continue,
                Err(err) => TlsReadOutcome::ConnectionLost(err),
            }
        }
        Err(err)
            if err.kind() == io::ErrorKind::WouldBlock
                || err.kind() == io::ErrorKind::Interrupted =>
        {
            TlsReadOutcome::Continue
        }
        Err(err) => TlsReadOutcome::ConnectionLost(err.to_string()),
    }
}

pub(super) fn tls_socket_wait_target(tls_state: &SharedTlsIoState) -> (fd_ops::RawFd, bool) {
    let state = tls_state.lock().expect("poisoned tls state");
    (state.fd(), state.pollable())
}

pub(super) fn close_tls_writer(tls_state: &SharedTlsIoState) -> io::Result<()> {
    let mut state = tls_state.lock().expect("poisoned tls state");
    let shutdown_timeout = state.shutdown_timeout;
    state.connection.send_close_notify();
    let result = flush_tls_close_io_locked(&mut state, shutdown_timeout);
    let close_result = state.shutdown_close();
    result.and(close_result)
}

pub(super) fn abort_tls_writer(tls_state: &SharedTlsIoState) -> io::Result<()> {
    let state = tls_state.lock().expect("poisoned tls state");
    state.shutdown_close()
}

pub(super) fn flush_tls_io_locked(state: &mut TlsIoState) -> io::Result<()> {
    while state.connection.wants_write() {
        match state.write_tls() {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to flush TLS records",
                ));
            }
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                wait_socket_ready(state.fd(), state.pollable(), false, true)?;
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

pub(super) fn flush_tls_close_io_locked(
    state: &mut TlsIoState,
    timeout: Duration,
) -> io::Result<()> {
    let deadline = std::time::Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(std::time::Instant::now);
    while state.connection.wants_write() {
        match state.write_tls() {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to flush TLS records",
                ));
            }
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                wait_socket_ready_until(state.fd(), state.pollable(), false, true, deadline)?;
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

pub(super) fn tls_io_error(err: rustls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err.to_string())
}
