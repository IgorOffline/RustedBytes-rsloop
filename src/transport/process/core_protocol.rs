//! Delivering subprocess lifecycle events to the Python protocol.
//!
//! Each callback runs inside the transport's `contextvars.Context` when one was
//! supplied. Text mode is applied here too: `pipe_data_received` decodes bytes
//! with the configured encoding (and translates newlines) before handing them
//! over, so the protocol sees `str` exactly as `subprocess` in text mode would.
//!
//! `connection_lost` is guarded by `connection_lost_called` and by the pipe /
//! exit bookkeeping in `ProcessState`, because both the waiter thread and the
//! last closing pipe can reach this point.

use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::{PendingProcessEvent, ProcessTransportCore, PyProcessTransport};

impl ProcessTransportCore {
    pub(super) fn connection_made(&self, transport: Py<PyProcessTransport>) -> PyResult<()> {
        self.call_in_loop_context(|py| {
            self.call_protocol_method1(py, "connection_made", transport.into_any())?;
            Ok(())
        })
    }

    pub(super) fn pipe_data_received_with_py(
        &self,
        py: Python<'_>,
        fd: i32,
        data: &[u8],
    ) -> PyResult<()> {
        let payload = if let Some(text_config) = &self.text_config {
            let decoded = pyo3::types::PyBytes::new(py, data)
                .call_method1("decode", (&text_config.encoding, &text_config.errors))?;
            if text_config.translate_newlines {
                decoded
                    .call_method1("replace", ("\r\n", "\n"))?
                    .call_method1("replace", ("\r", "\n"))?
                    .unbind()
                    .into_any()
            } else {
                decoded.unbind()
            }
        } else {
            pyo3::types::PyBytes::new(py, data).unbind().into_any()
        };
        self.call_protocol_method2(
            py,
            "pipe_data_received",
            fd.into_pyobject(py)?.unbind().into_any(),
            payload,
        )?;
        Ok(())
    }

    pub(super) fn pipe_data_received(self: &Arc<Self>, fd: i32, data: &[u8]) -> PyResult<()> {
        if !self.loop_core.on_runtime_thread() {
            self.enqueue_pending_event(PendingProcessEvent::PipeDataReceived {
                fd,
                data: Box::<[u8]>::from(data),
            });
            return Ok(());
        }

        self.call_in_loop_context(|py| self.pipe_data_received_with_py(py, fd, data))
    }

    pub(super) fn pipe_connection_lost_value_with_py(
        &self,
        py: Python<'_>,
        fd: i32,
        exc: Option<PyErr>,
    ) -> PyResult<()> {
        let exc = exc.map(|err| err.value(py).clone().unbind().into_any());
        self.call_protocol_method2(
            py,
            "pipe_connection_lost",
            fd.into_pyobject(py)?.unbind().into_any(),
            exc.unwrap_or_else(|| py.None()),
        )?;
        Ok(())
    }

    pub(super) fn pipe_connection_lost_message(
        self: &Arc<Self>,
        fd: i32,
        exc: Option<String>,
    ) -> PyResult<()> {
        let maybe_finish = {
            let mut state = self.state.lock().expect("poisoned process state");
            if !state.open_pipes.remove(&fd) {
                return Ok(());
            }
            let exited = state.exited;
            let empty = state.open_pipes.is_empty();
            (exc, exited && empty)
        };

        if !self.loop_core.on_runtime_thread() {
            self.enqueue_pending_event(PendingProcessEvent::PipeConnectionLost {
                fd,
                exc: maybe_finish.0,
            });
            if maybe_finish.1 {
                self.enqueue_pending_event(PendingProcessEvent::ConnectionLost { exc: None });
            }
            return Ok(());
        }

        if let Err(err) = self.call_in_loop_context(|py| {
            self.pipe_connection_lost_value_with_py(
                py,
                fd,
                maybe_finish.0.clone().map(PyRuntimeError::new_err),
            )
        }) {
            self.report_error(err, "subprocess pipe_connection_lost failed");
            return Err(PyRuntimeError::new_err(
                "subprocess pipe_connection_lost failed",
            ));
        }

        if maybe_finish.1 {
            self.connection_lost_message(None)?;
        }
        Ok(())
    }

    pub(super) fn pipe_connection_lost(
        self: &Arc<Self>,
        fd: i32,
        exc: Option<PyErr>,
    ) -> PyResult<()> {
        let exc = exc.map(|err| Python::attach(|py| err.value(py).to_string()));
        self.pipe_connection_lost_message(fd, exc)
    }

    pub(super) fn process_exited_with_py(&self, py: Python<'_>, returncode: i32) -> PyResult<()> {
        let _ = returncode;
        self.call_protocol_method0(py, "process_exited")?;
        Ok(())
    }

    pub(super) fn process_exited(self: &Arc<Self>, returncode: i32) -> PyResult<()> {
        let should_finish = {
            let mut state = self.state.lock().expect("poisoned process state");
            state.returncode = Some(returncode);
            state.exited = true;
            state.open_pipes.is_empty()
        };
        self.exit_notify.notify_all();

        if !self.loop_core.on_runtime_thread() {
            self.enqueue_pending_event(PendingProcessEvent::ProcessExited { returncode });
            if should_finish {
                self.enqueue_pending_event(PendingProcessEvent::ConnectionLost { exc: None });
            }
            return Ok(());
        }

        self.call_in_loop_context(|py| self.process_exited_with_py(py, returncode))?;

        if should_finish {
            self.connection_lost_message(None)?;
        }
        Ok(())
    }

    pub(super) fn connection_lost_with_py(
        &self,
        py: Python<'_>,
        exc: Option<PyErr>,
    ) -> PyResult<()> {
        let arg = exc
            .map(|err| err.value(py).clone().unbind().into_any())
            .unwrap_or_else(|| py.None());
        self.call_protocol_method1(py, "connection_lost", arg)?;
        Ok(())
    }

    pub(super) fn connection_lost_message(self: &Arc<Self>, exc: Option<String>) -> PyResult<()> {
        {
            let mut state = self.state.lock().expect("poisoned process state");
            if state.connection_lost_called {
                return Ok(());
            }
            state.connection_lost_called = true;
            state.closing = true;
        }

        if !self.loop_core.on_runtime_thread() {
            self.enqueue_pending_event(PendingProcessEvent::ConnectionLost { exc });
            return Ok(());
        }

        self.call_in_loop_context(|py| {
            self.connection_lost_with_py(py, exc.clone().map(PyRuntimeError::new_err))
        })
    }

    pub(super) fn connection_lost(self: &Arc<Self>, exc: Option<PyErr>) -> PyResult<()> {
        let exc = exc.map(|err| Python::attach(|py| err.value(py).to_string()));
        self.connection_lost_message(exc)
    }
}
