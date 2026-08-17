//! `loop.sock_*` operations on raw Python sockets.
//!
//! Each one drives the Python socket method and, when it reports a retryable
//! error, waits for readiness before trying again — the socket object stays the
//! source of truth so subclassed or wrapped sockets keep working.

use pyo3::prelude::*;
use pyo3::types::PySlice;

use super::PyLoop;
use super::socket_connect::connect_socket_to_address;
use crate::fd_ops;

pub(super) fn sock_recv<'py>(
    slf: Py<PyLoop>,
    py: Python<'py>,
    sock: Py<PyAny>,
    nbytes: usize,
) -> PyResult<Bound<'py, PyAny>> {
    let locals = PyLoop::task_locals(py, &slf)?;
    let fd = fd_ops::fileobj_to_fd(py, sock.bind(py))?;
    pyo3_async_runtimes::async_std::future_into_py_with_locals(py, locals, async move {
        loop {
            match Python::attach(|py| sock.call_method1(py, "recv", (nbytes,))) {
                Ok(value) => return Ok(value),
                Err(err) => {
                    let retry = Python::attach(|py| fd_ops::is_retryable_socket_error(py, &err))?;
                    if !retry {
                        return Err(err);
                    }
                }
            }
            fd_ops::wait_readable(fd).await?;
        }
    })
}

pub(super) fn sock_recv_into<'py>(
    slf: Py<PyLoop>,
    py: Python<'py>,
    sock: Py<PyAny>,
    buf: Py<PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let locals = PyLoop::task_locals(py, &slf)?;
    let fd = fd_ops::fileobj_to_fd(py, sock.bind(py))?;
    pyo3_async_runtimes::async_std::future_into_py_with_locals(py, locals, async move {
        loop {
            match Python::attach(|py| sock.call_method1(py, "recv_into", (buf.clone_ref(py),))) {
                Ok(value) => return Ok(value),
                Err(err) => {
                    let retry = Python::attach(|py| fd_ops::is_retryable_socket_error(py, &err))?;
                    if !retry {
                        return Err(err);
                    }
                }
            }
            fd_ops::wait_readable(fd).await?;
        }
    })
}

pub(super) fn sock_sendall<'py>(
    slf: Py<PyLoop>,
    py: Python<'py>,
    sock: Py<PyAny>,
    data: Py<PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let locals = PyLoop::task_locals(py, &slf)?;
    let fd = fd_ops::fileobj_to_fd(py, sock.bind(py))?;
    pyo3_async_runtimes::async_std::future_into_py_with_locals(py, locals, async move {
        let total = Python::attach(|py| data.bind(py).len())?;
        let mut sent = 0usize;

        while sent < total {
            let wrote = match Python::attach(|py| -> PyResult<usize> {
                let sent = isize::try_from(sent).expect("Python object length fits in Py_ssize_t");
                let total =
                    isize::try_from(total).expect("Python object length fits in Py_ssize_t");
                let chunk = data.bind(py).get_item(PySlice::new(py, sent, total, 1))?;
                sock.call_method1(py, "send", (chunk,))?.extract(py)
            }) {
                Ok(wrote) => wrote,
                Err(err) => {
                    let retry = Python::attach(|py| fd_ops::is_retryable_socket_error(py, &err))?;
                    if !retry {
                        return Err(err);
                    }
                    fd_ops::wait_writable(fd).await?;
                    continue;
                }
            };
            sent += wrote;
            if sent < total {
                fd_ops::wait_writable(fd).await?;
            }
        }

        Ok(Python::attach(|py| py.None()))
    })
}

pub(super) fn sock_accept<'py>(
    slf: Py<PyLoop>,
    py: Python<'py>,
    sock: Py<PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let locals = PyLoop::task_locals(py, &slf)?;
    let fd = fd_ops::fileobj_to_fd(py, sock.bind(py))?;
    pyo3_async_runtimes::async_std::future_into_py_with_locals(py, locals, async move {
        loop {
            match Python::attach(|py| -> PyResult<Py<PyAny>> {
                let accepted = sock.call_method0(py, "accept")?;
                let client = accepted.bind(py).get_item(0)?;
                client.call_method1("setblocking", (false,))?;
                Ok(accepted)
            }) {
                Ok(value) => return Ok(value),
                Err(err) => {
                    let retry = Python::attach(|py| fd_ops::is_retryable_socket_error(py, &err))?;
                    if !retry {
                        return Err(err);
                    }
                }
            }
            fd_ops::wait_readable(fd).await?;
        }
    })
}

pub(super) fn sock_connect<'py>(
    slf: Py<PyLoop>,
    py: Python<'py>,
    sock: Py<PyAny>,
    address: Py<PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let locals = PyLoop::task_locals(py, &slf)?;
    pyo3_async_runtimes::async_std::future_into_py_with_locals(py, locals, async move {
        connect_socket_to_address(sock, address).await?;
        Ok(Python::attach(|py| py.None()))
    })
}

/// Connects an INET/INET6 stream socket, returning a loop-native Future
/// (not a coroutine — awaited directly, never `create_task`ed). On Unix the
/// writability wait runs on the vibeio reactor and its completion is
/// delivered through the loop's batched, GIL-free ready queue, so many
/// concurrent connections drain in one loop iteration instead of paying a
/// per-connection async-runtime handoff. Non-Unix and non-INET sockets fall
/// back to the general `sock_connect` path.
pub(super) fn sock_connect_fast<'py>(
    slf: Py<PyLoop>,
    py: Python<'py>,
    sock: Py<PyAny>,
    address: Py<PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    // Only `__loop_create_connection` calls this, and only with an
    // INET/INET6 SOCK_STREAM socket it just built, so the family/type check
    // (a `socket` import plus IntEnum property reads on every connection) is
    // pure overhead — skip straight to the loop-thread connect on Unix.
    #[cfg(unix)]
    {
        super::socket_connect::fast_sock_connect(&slf, py, sock, address)
    }
    #[cfg(not(unix))]
    {
        sock_connect(slf, py, sock, address)
    }
}
