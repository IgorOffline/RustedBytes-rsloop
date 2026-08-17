//! Socket readiness waits for the blocking worker threads.
//!
//! Workers block on `poll()` with a bounded interval so a stop request is
//! noticed promptly. Descriptors that cannot be polled (Windows files) fall
//! back to a short sleep, which keeps the same call shape for every target.

use std::io;
use std::thread;
use std::time::Duration;

use super::tuning::BLOCKING_POLL_INTERVAL_MS;
use crate::fd_ops;

pub(super) fn wait_socket_ready(
    fd: fd_ops::RawFd,
    pollable: bool,
    read: bool,
    write: bool,
) -> io::Result<()> {
    if pollable {
        loop {
            match fd_ops::poll_fd(fd, read, write, BLOCKING_POLL_INTERVAL_MS) {
                Ok((read_ready, write_ready))
                    if (!read || read_ready) && (!write || write_ready) =>
                {
                    return Ok(());
                }
                Ok(_) => continue,
                Err(err) => return Err(err),
            }
        }
    }

    thread::sleep(Duration::from_millis(10));
    Ok(())
}

pub(super) fn wait_socket_ready_once(
    fd: fd_ops::RawFd,
    pollable: bool,
    read: bool,
    write: bool,
) -> io::Result<bool> {
    if pollable {
        return fd_ops::poll_fd(fd, read, write, BLOCKING_POLL_INTERVAL_MS)
            .map(|(read_ready, write_ready)| (!read || read_ready) && (!write || write_ready));
    }
    thread::sleep(Duration::from_millis(10));
    Ok(true)
}

pub(super) fn poll_read_ready(fd: fd_ops::RawFd) -> io::Result<bool> {
    fd_ops::poll_fd(fd, true, false, BLOCKING_POLL_INTERVAL_MS).map(|(ready, _)| ready)
}

pub(super) fn wait_socket_ready_until(
    fd: fd_ops::RawFd,
    pollable: bool,
    read: bool,
    write: bool,
    deadline: std::time::Instant,
) -> io::Result<()> {
    if pollable {
        loop {
            let now = std::time::Instant::now();
            if now >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "SSL shutdown timed out",
                ));
            }

            let remaining_ms = deadline
                .saturating_duration_since(now)
                .as_millis()
                .clamp(1, u128::from(i32::MAX.unsigned_abs()));
            let remaining_ms =
                i32::try_from(remaining_ms).expect("poll timeout is clamped to i32::MAX");
            match fd_ops::poll_fd(fd, read, write, remaining_ms) {
                Ok((read_ready, write_ready))
                    if (!read || read_ready) && (!write || write_ready) =>
                {
                    return Ok(());
                }
                Ok(_) => continue,
                Err(err) => return Err(err),
            }
        }
    }

    if std::time::Instant::now() >= deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "SSL shutdown timed out",
        ));
    }
    thread::sleep(Duration::from_millis(10));
    Ok(())
}
