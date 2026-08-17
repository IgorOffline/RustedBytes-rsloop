//! Socket readers hosted on the loop's `vibeio` runtime.
//!
//! These are the readers that actually run for TCP and Unix connections. They
//! read into a pooled buffer and hand it to the transport by move whenever the
//! chunk is large enough to be worth it, so a busy connection recycles one
//! allocation instead of allocating per read.
//!
//! Windows needs both modes: overlapped receives are fastest for bulk reads,
//! but a server that is mid-`WSARecv` blocks a duplicated writer socket. When a
//! write asks for it, the reader cancels the overlapped receive (surfacing as
//! `ERROR_OPERATION_ABORTED`) and rebinds the same socket in readiness mode
//! until the write is through.

use std::io;
use std::net::TcpStream as StdTcpStream;
#[cfg(unix)]
use std::os::unix::net::UnixStream as StdUnixStream;
use std::sync::Arc;
#[cfg(windows)]
use std::sync::atomic::Ordering;

use tokio::io::AsyncReadExt;
#[cfg(windows)]
use vibeio::io::AsyncRead as VibeAsyncRead;
use vibeio::net::PollTcpStream as VibePollTcpStream;
#[cfg(unix)]
use vibeio::net::PollUnixStream as VibePollUnixStream;
#[cfg(windows)]
use vibeio::net::TcpStream as VibeTcpStream;
#[cfg(windows)]
use windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED;

use super::buffers::ReadBufferPool;
use super::tuning::{
    MAX_STREAM_READ_BUFFER_SIZE, OWNED_READ_HANDOFF_MIN_BYTES, STREAM_READ_BUFFER_SIZE,
};
#[cfg(windows)]
use super::tuning::{
    SERVER_POLL_READER_TINY_TRIGGER_MAX_BYTES, SERVER_POLL_READER_WRITE_THRESHOLD,
};
use super::{PendingReadEvent, StreamTransportCore};

#[cfg(not(windows))]
pub(crate) async fn run_tcp_socket_reader_task(
    core: Arc<StreamTransportCore>,
    stream: Arc<StdTcpStream>,
) {
    profiling::scope!("stream.run_tcp_socket_reader_task");
    let mut reader = match VibePollTcpStream::from_shared(stream) {
        Ok(reader) => reader,
        Err(err) => {
            core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(Some(
                err.to_string(),
            )));
            return;
        }
    };
    // `read_buf` writes into spare capacity, so idle transports reserve this
    // address space without eagerly zero-filling and faulting in every page.
    let mut buf = Vec::with_capacity(STREAM_READ_BUFFER_SIZE);

    loop {
        if core.is_closing() {
            core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(None));
            return;
        }

        core.wait_until_async_readable().await;
        if core.is_closing() {
            core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(None));
            return;
        }

        buf.clear();
        match reader.read_buf(&mut buf).await {
            Ok(0) => {
                core.enqueue_pending_read_event(PendingReadEvent::Eof);
                return;
            }
            Ok(_) => {
                let data = take_async_read_data(&mut buf, &core.read_buffer_pool);
                core.enqueue_pending_read_event(PendingReadEvent::Data(data));
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => continue,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => {
                core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(Some(
                    err.to_string(),
                )));
                return;
            }
        }
    }
}

#[cfg(windows)]
pub(crate) async fn run_tcp_socket_reader_task(
    core: Arc<StreamTransportCore>,
    stream: Arc<StdTcpStream>,
) {
    profiling::scope!("stream.run_tcp_socket_reader_task");

    // All TCP protocols start in completion mode on Windows. Besides avoiding
    // readiness polling for callback-driven protocols (aiohttp, websockets,
    // uvicorn), this keeps their hot path aligned with native fast streams.
    // start_tls and large server writes request a synchronous rebind to poll
    // mode before they reclaim or heavily write through the shared socket.
    let mut reader = match VibeTcpStream::from_shared(stream, vibeio::RegistrationMode::Completion)
    {
        Ok(reader) => reader,
        Err(err) => {
            core.mark_poll_reader_ready(false);
            core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(Some(
                err.to_string(),
            )));
            return;
        }
    };
    let mut buf = Vec::with_capacity(STREAM_READ_BUFFER_SIZE);

    loop {
        if core.is_closing() {
            core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(None));
            return;
        }
        core.wait_until_async_readable().await;
        if core.is_closing() {
            core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(None));
            return;
        }

        if core.poll_reader_requested() {
            match reader.into_poll() {
                Ok(poll_reader) => {
                    core.mark_poll_reader_ready(true);
                    run_windows_poll_tcp_reader(core, poll_reader, buf).await;
                }
                Err(err) => {
                    core.mark_poll_reader_ready(false);
                    core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(Some(
                        err.to_string(),
                    )));
                }
            }
            return;
        }

        buf.clear();
        let (result, returned_buf) = VibeAsyncRead::read(&mut reader, buf).await;
        buf = returned_buf;
        match result {
            Ok(0) => {
                if core.poll_reader_requested() {
                    core.mark_poll_reader_ready(false);
                }
                core.enqueue_pending_read_event(PendingReadEvent::Eof);
                return;
            }
            Ok(read) => {
                let saturated = read == buf.capacity();
                let data = take_async_read_data(&mut buf, &core.read_buffer_pool);
                let tiny_server_trigger =
                    core.server_side && read <= SERVER_POLL_READER_TINY_TRIGGER_MAX_BYTES;

                // Completion reads win on normal request/response traffic. A
                // full inbound buffer indicates sustained transfer; a tiny
                // server command commonly triggers a large outbound response.
                // Rebind before delivering either event so the protocol cannot
                // begin bulk writes while an overlapped receive is outstanding.
                if saturated || tiny_server_trigger {
                    if core.server_side {
                        core.poll_reader_requested.store(true, Ordering::Release);
                    }
                    match reader.into_poll() {
                        Ok(poll_reader) => {
                            core.mark_poll_reader_ready(true);
                            core.enqueue_pending_read_event(PendingReadEvent::Data(data));
                            run_windows_poll_tcp_reader(core, poll_reader, buf).await;
                        }
                        Err(err) => {
                            core.mark_poll_reader_ready(false);
                            core.enqueue_pending_read_event(PendingReadEvent::Data(data));
                            core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(
                                Some(err.to_string()),
                            ));
                        }
                    }
                    return;
                }

                core.enqueue_pending_read_event(PendingReadEvent::Data(data));
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => continue,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err)
                if core.poll_reader_requested()
                    && err.raw_os_error() == Some(ERROR_OPERATION_ABORTED as i32) =>
            {
                match reader.into_poll() {
                    Ok(poll_reader) => {
                        core.mark_poll_reader_ready(true);
                        run_windows_poll_tcp_reader(core, poll_reader, buf).await;
                    }
                    Err(err) => {
                        core.mark_poll_reader_ready(false);
                        core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(Some(
                            err.to_string(),
                        )));
                    }
                }
                return;
            }
            Err(err) => {
                core.mark_poll_reader_ready(false);
                core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(Some(
                    err.to_string(),
                )));
                return;
            }
        }
    }
}

#[cfg(windows)]
pub(super) async fn run_windows_poll_tcp_reader(
    core: Arc<StreamTransportCore>,
    mut reader: VibePollTcpStream,
    mut buf: Vec<u8>,
) {
    loop {
        if core.is_closing() {
            core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(None));
            return;
        }
        core.wait_until_async_readable().await;
        if core.is_closing() {
            core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(None));
            return;
        }

        buf.clear();
        match reader.read_buf(&mut buf).await {
            Ok(0) => {
                core.enqueue_pending_read_event(PendingReadEvent::Eof);
                return;
            }
            Ok(_) => {
                let data = take_async_read_data(&mut buf, &core.read_buffer_pool);
                core.enqueue_pending_read_event(PendingReadEvent::Data(data));
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => continue,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => {
                core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(Some(
                    err.to_string(),
                )));
                return;
            }
        }
    }
}

#[cfg(unix)]
pub(crate) async fn run_unix_socket_reader_task(
    core: Arc<StreamTransportCore>,
    stream: StdUnixStream,
) {
    profiling::scope!("stream.run_unix_socket_reader_task");
    let mut reader = match VibePollUnixStream::from_std(stream) {
        Ok(reader) => reader,
        Err(err) => {
            core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(Some(
                err.to_string(),
            )));
            return;
        }
    };
    let mut buf = Vec::with_capacity(STREAM_READ_BUFFER_SIZE);

    loop {
        if core.is_closing() {
            core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(None));
            return;
        }
        core.wait_until_async_readable().await;
        if core.is_closing() {
            core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(None));
            return;
        }
        buf.clear();
        match reader.read_buf(&mut buf).await {
            Ok(0) => {
                core.enqueue_pending_read_event(PendingReadEvent::Eof);
                return;
            }
            Ok(_) => {
                let data = take_async_read_data(&mut buf, &core.read_buffer_pool);
                core.enqueue_pending_read_event(PendingReadEvent::Data(data));
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => continue,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => {
                core.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(Some(
                    err.to_string(),
                )));
                return;
            }
        }
    }
}

pub(super) fn take_async_read_data(buf: &mut Vec<u8>, pool: &ReadBufferPool) -> Vec<u8> {
    let capacity = buf.capacity().max(STREAM_READ_BUFFER_SIZE);
    let next_capacity = if buf.len() == capacity && capacity < MAX_STREAM_READ_BUFFER_SIZE {
        (capacity * 2).min(MAX_STREAM_READ_BUFFER_SIZE)
    } else if capacity > STREAM_READ_BUFFER_SIZE && buf.len() < capacity / 4 {
        (capacity / 2).max(STREAM_READ_BUFFER_SIZE)
    } else {
        capacity
    };

    if buf.len() < OWNED_READ_HANDOFF_MIN_BYTES {
        let data = buf.clone();
        if next_capacity != capacity {
            let previous = std::mem::replace(buf, pool.acquire(next_capacity));
            pool.release(previous);
        }
        return data;
    }
    std::mem::replace(buf, pool.acquire(next_capacity))
}
