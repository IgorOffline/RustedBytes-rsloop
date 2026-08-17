//! Work that runs off the loop thread: executors and name resolution.
//!
//! `getaddrinfo`/`getnameinfo` are here because `asyncio` defines them in terms
//! of `run_in_executor`, and they dispatch back through Python so a subclass that
//! overrides `run_in_executor` still sees them.

use std::time::Duration;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use super::PyLoop;

/// The `socket.getaddrinfo` keyword group as `asyncio` exposes it.
pub(super) struct AddrInfoRequest {
    pub(super) host: Option<Py<PyAny>>,
    pub(super) port: Option<Py<PyAny>>,
    pub(super) family: i32,
    pub(super) sock_type: i32,
    pub(super) proto: i32,
    pub(super) flags: i32,
}

fn warn_default_executor_timeout(py: Python<'_>, timeout: f64) -> PyResult<()> {
    let warnings = py.import("warnings")?;
    let builtins = py.import("builtins")?;
    warnings.call_method(
        "warn",
        (
            format!("The executor did not finishing joining its threads within {timeout} seconds."),
            builtins.getattr("RuntimeWarning")?,
        ),
        Some(&{
            let kwargs = PyDict::new(py);
            kwargs.set_item("stacklevel", 2)?;
            kwargs
        }),
    )?;
    Ok(())
}

pub(super) fn run_in_executor<'py>(
    slf: Py<PyLoop>,
    py: Python<'py>,
    executor: Option<Py<PyAny>>,
    func: Py<PyAny>,
    args: &Bound<'py, PyTuple>,
) -> PyResult<Bound<'py, PyAny>> {
    let selected_executor = if let Some(executor) = executor {
        Some(executor)
    } else {
        let core = slf.borrow(py).core.clone();
        let state = core.state.lock().expect("poisoned loop state");
        if state.executor_shutdown_called {
            return Err(PyRuntimeError::new_err("Executor shutdown has been called"));
        }
        state
            .default_executor
            .as_ref()
            .map(|value| value.clone_ref(py))
    };

    if let Some(executor) = selected_executor {
        let mut submit_items = Vec::with_capacity(args.len() + 1);
        submit_items.push(func.clone_ref(py));
        submit_items.extend(args.iter().map(|item| item.unbind()));
        let submit_args = PyTuple::new(py, submit_items)?;
        let concurrent_future = executor.call_method1(py, "submit", submit_args)?;
        let asyncio = py.import("asyncio")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("loop", PyLoop::as_py_any(py, &slf))?;
        return asyncio
            .getattr("wrap_future")?
            .call((concurrent_future,), Some(&kwargs));
    }

    let locals = PyLoop::task_locals(py, &slf)?;
    let args = args.clone().unbind();
    pyo3_async_runtimes::async_std::future_into_py_with_locals(py, locals, async move {
        crate::blocking::run("rsloop-run-in-executor", move || {
            Python::attach(|py| func.call1(py, args.clone_ref(py)))
        })
        .await
        .map_err(PyRuntimeError::new_err)?
    })
}

pub(super) fn getaddrinfo<'py>(
    slf: Py<PyLoop>,
    py: Python<'py>,
    request: AddrInfoRequest,
) -> PyResult<Bound<'py, PyAny>> {
    let socket = py.import("socket")?;
    let host = request.host.unwrap_or_else(|| py.None());
    let port = request.port.unwrap_or_else(|| py.None());
    let run_args = PyTuple::new(
        py,
        [
            py.None(),
            socket.getattr("getaddrinfo")?.unbind(),
            host,
            port,
            request.family.into_pyobject(py)?.unbind().into(),
            request.sock_type.into_pyobject(py)?.unbind().into(),
            request.proto.into_pyobject(py)?.unbind().into(),
            request.flags.into_pyobject(py)?.unbind().into(),
        ],
    )?;
    slf.call_method1(py, "run_in_executor", run_args)
        .map(|awaitable| awaitable.into_bound(py))
}

pub(super) fn getnameinfo<'py>(
    slf: Py<PyLoop>,
    py: Python<'py>,
    sockaddr: Py<PyAny>,
    flags: i32,
) -> PyResult<Bound<'py, PyAny>> {
    let socket = py.import("socket")?;
    let run_args = PyTuple::new(
        py,
        [
            py.None(),
            socket.getattr("getnameinfo")?.unbind(),
            sockaddr,
            flags.into_pyobject(py)?.unbind().into(),
        ],
    )?;
    slf.call_method1(py, "run_in_executor", run_args)
        .map(|awaitable| awaitable.into_bound(py))
}

pub(super) fn shutdown_default_executor<'py>(
    slf: Py<PyLoop>,
    py: Python<'py>,
    timeout: Option<f64>,
) -> PyResult<Bound<'py, PyAny>> {
    let executor = {
        let core = slf.borrow(py).core.clone();
        let mut state = core.state.lock().expect("poisoned loop state");
        state.executor_shutdown_called = true;
        state.default_executor.take()
    };
    let executor_nowait = if timeout.is_some() {
        executor.as_ref().map(|value| value.clone_ref(py))
    } else {
        None
    };

    let locals = PyLoop::task_locals(py, &slf)?;
    pyo3_async_runtimes::async_std::future_into_py_with_locals(py, locals, async move {
        if let Some(executor) = executor {
            let wait_forever = timeout.is_none() || timeout.is_some_and(f64::is_infinite);
            if wait_forever {
                crate::blocking::run("rsloop-shutdown-default-executor", move || {
                    Python::attach(|py| -> PyResult<()> {
                        executor.call_method1(py, "shutdown", (true,))?;
                        Ok(())
                    })
                })
                .await
                .map_err(PyRuntimeError::new_err)??;
            } else {
                shutdown_executor_with_timeout(
                    executor,
                    executor_nowait,
                    timeout.expect("timeout checked above"),
                )
                .await?;
            }
        }

        Ok(Python::attach(|py| py.None()))
    })
}

/// Shuts the executor down on a helper thread so the wait can time out; on
/// timeout it warns and falls back to a non-waiting `shutdown(False)`.
async fn shutdown_executor_with_timeout(
    executor: Py<PyAny>,
    executor_nowait: Option<Py<PyAny>>,
    timeout: f64,
) -> PyResult<()> {
    let (tx, rx) = futures::channel::oneshot::channel();
    std::thread::Builder::new()
        .name("rsloop-shutdown-default-executor".to_owned())
        .spawn(move || {
            let result = Python::attach(|py| -> PyResult<()> {
                executor.call_method1(py, "shutdown", (true,))?;
                Ok(())
            });
            let _ = tx.send(result);
        })
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;

    let timed_out = if timeout.is_finite() && timeout > 0.0 {
        match async_std::future::timeout(Duration::from_secs_f64(timeout), async move {
            rx.await
                .map_err(|_| PyRuntimeError::new_err("default executor shutdown worker dropped"))?
        })
        .await
        {
            Ok(result) => {
                result?;
                false
            }
            Err(_) => true,
        }
    } else {
        true
    };

    if timed_out {
        Python::attach(|py| warn_default_executor_timeout(py, timeout))?;
        if let Some(executor_nowait) = executor_nowait {
            crate::blocking::run("rsloop-shutdown-default-executor-nowait", move || {
                Python::attach(|py| -> PyResult<()> {
                    executor_nowait.call_method1(py, "shutdown", (false,))?;
                    Ok(())
                })
            })
            .await
            .map_err(PyRuntimeError::new_err)??;
        }
    }
    Ok(())
}
