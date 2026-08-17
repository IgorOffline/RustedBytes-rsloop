//! The subprocess event queue and the protocol-call plumbing above it.
//!
//! Reader and waiter threads never touch Python. They enqueue
//! `PendingProcessEvent`s and ask the loop for a drain;
//! `drain_pending_events_with_py` is the only place the protocol is called, so
//! `pipe_data_received`, `process_exited`, and `connection_lost` always arrive
//! on the loop thread in the order the workers produced them.
//!
//! The drain re-checks ordering constraints as it goes: `connection_lost` is
//! held back until the process has exited and every pipe has closed, which is
//! the guarantee `asyncio.SubprocessProtocol` expects.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use super::{PendingProcessEvent, ProcessTransportCore};
use crate::context::{ensure_running_loop, run_in_context};
use crate::engine::{LoopCommand, LoopTransportCommand};

impl ProcessTransportCore {
    pub(super) fn enqueue_pending_event(self: &Arc<Self>, event: PendingProcessEvent) {
        profiling::scope!("ProcessTransportCore::enqueue_pending_event");
        self.pending_events
            .lock()
            .expect("poisoned process pending queue")
            .push_back(event);

        if !self.events_scheduled.swap(true, Ordering::AcqRel)
            && self
                .loop_core
                .send_command(LoopCommand::Transport(LoopTransportCommand::Process(
                    Arc::clone(self),
                )))
                .is_err()
        {
            self.events_scheduled.store(false, Ordering::Release);
        }
    }

    pub(crate) fn drain_pending_events_with_py(self: &Arc<Self>, py: Python<'_>) -> PyResult<()> {
        profiling::scope!("ProcessTransportCore::drain_pending_events_with_py");
        let mut drained = VecDeque::new();
        loop {
            {
                let mut queue = self
                    .pending_events
                    .lock()
                    .expect("poisoned process pending queue");
                if queue.is_empty() {
                    self.events_scheduled.store(false, Ordering::Release);
                    return Ok(());
                }

                std::mem::swap(&mut drained, &mut *queue);
            }

            while let Some(event) = drained.pop_front() {
                match event {
                    PendingProcessEvent::PipeDataReceived { fd, data } => {
                        profiling::scope!("process.pending.pipe_data_received");
                        if let Err(err) = self.pipe_data_received_with_py(py, fd, &data) {
                            self.report_error(err, "subprocess pipe_data_received failed");
                            let _ = self.connection_lost_with_py(py, None);
                            self.events_scheduled.store(false, Ordering::Release);
                            return Ok(());
                        }
                    }
                    PendingProcessEvent::PipeConnectionLost { fd, exc } => {
                        profiling::scope!("process.pending.pipe_connection_lost");
                        if let Err(err) = self.pipe_connection_lost_value_with_py(
                            py,
                            fd,
                            exc.map(PyRuntimeError::new_err),
                        ) {
                            self.report_error(err, "subprocess pipe_connection_lost failed");
                            let _ = self.connection_lost_with_py(py, None);
                            self.events_scheduled.store(false, Ordering::Release);
                            return Ok(());
                        }
                    }
                    PendingProcessEvent::ProcessExited { returncode } => {
                        profiling::scope!("process.pending.process_exited");
                        if let Err(err) = self.process_exited_with_py(py, returncode) {
                            self.report_error(err, "subprocess process_exited failed");
                            let _ = self.connection_lost_with_py(py, None);
                            self.events_scheduled.store(false, Ordering::Release);
                            return Ok(());
                        }
                    }
                    PendingProcessEvent::ConnectionLost { exc } => {
                        profiling::scope!("process.pending.connection_lost");
                        let _ = self.connection_lost_with_py(py, exc.map(PyRuntimeError::new_err));
                        self.events_scheduled.store(false, Ordering::Release);
                        return Ok(());
                    }
                }
            }
        }
    }

    pub(super) fn call_protocol_with_tuple(
        &self,
        py: Python<'_>,
        method: &str,
        args: &Bound<'_, PyTuple>,
    ) -> PyResult<Py<PyAny>> {
        let (protocol, context, context_needs_run) = {
            let state = self.state.lock().expect("poisoned process state");
            (
                state.protocol.clone_ref(py),
                state.context.clone_ref(py),
                state.context_needs_run,
            )
        };
        let callback = protocol.bind(py).getattr(method)?.unbind();
        let tuple = args.clone().unbind();
        run_in_context(py, &context, context_needs_run, &callback, &tuple)
    }

    pub(super) fn call_in_loop_context<T>(
        &self,
        f: impl for<'py> FnOnce(Python<'py>) -> PyResult<T>,
    ) -> PyResult<T> {
        Python::attach(|py| {
            ensure_running_loop(py, &self.loop_obj)?;
            f(py)
        })
    }

    pub(super) fn call_protocol_method0(
        &self,
        py: Python<'_>,
        method: &str,
    ) -> PyResult<Py<PyAny>> {
        let args = PyTuple::empty(py);
        self.call_protocol_with_tuple(py, method, &args)
    }

    pub(super) fn call_protocol_method1(
        &self,
        py: Python<'_>,
        method: &str,
        arg: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let args = PyTuple::new(py, [arg])?;
        self.call_protocol_with_tuple(py, method, &args)
    }

    pub(super) fn call_protocol_method2(
        &self,
        py: Python<'_>,
        method: &str,
        arg0: Py<PyAny>,
        arg1: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let args = PyTuple::new(py, [arg0, arg1])?;
        self.call_protocol_with_tuple(py, method, &args)
    }

    pub(super) fn report_error(&self, err: PyErr, message: &str) {
        let _ = Python::attach(|py| -> PyResult<()> {
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
}
