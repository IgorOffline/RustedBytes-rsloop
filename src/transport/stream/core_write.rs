//! The write path: direct writes, staging, and protocol backpressure.
//!
//! A write is attempted straight on the socket first, because most responses
//! fit in the kernel buffer and never need a writer thread. What does not fit
//! is queued to the writer worker, which is only started at that point.
//!
//! Server responses that begin with a small header get staged for one loop turn
//! (`SMALL_WRITE_COALESCE_*`) so the header and body share a syscall. Buffered
//! bytes are tracked against the high/low water marks and translated into
//! `pause_writing`/`resume_writing` on the protocol.

use std::io::{self, Write as _};
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::buffers::OwnedWriteBuffer;
use super::io_targets::{StreamKind, TaskedDirectWriter};
use super::stats::{
    TRANSPORT_DIRECT_WRITE_ATTEMPTS, TRANSPORT_STAGED_WRITES, transport_stats_enabled,
};
#[cfg(windows)]
use super::tuning::SERVER_POLL_READER_WRITE_THRESHOLD;
use super::tuning::{
    SMALL_WRITE_COALESCE_MAX_BYTES, SMALL_WRITE_COALESCE_MIN_BYTES, max_write_buffer_size,
};
#[cfg(unix)]
use super::unix_stream_from_owned_socket_fd;
use super::{
    PendingReadEvent, StreamTransportCore, TransportSpawnContext, WriterCommand,
    stop_socket_reader, tcp_stream_from_owned_socket_fd,
};
use crate::engine::{LoopCommand, LoopTransportCommand};
use crate::fd_ops;

impl StreamTransportCore {
    #[inline]
    pub(super) fn write_backpressure_active(&self) -> bool {
        self.state
            .lock()
            .expect("poisoned transport state")
            .writer_registered
    }

    #[inline]
    pub(super) fn set_write_backpressure_active(&self, active: bool) {
        self.state
            .lock()
            .expect("poisoned transport state")
            .writer_registered = active;
    }

    pub(super) fn close_on_write_eof(&self) -> bool {
        self.state
            .lock()
            .expect("poisoned transport state")
            .close_on_write_eof
    }

    pub(super) fn try_direct_tasked_write(&self, data: &[u8]) -> io::Result<usize> {
        profiling::scope!("StreamTransportCore::try_direct_tasked_write");
        if transport_stats_enabled() {
            TRANSPORT_DIRECT_WRITE_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
        }
        let Some(writer) = &self.direct_writer else {
            return Err(io::Error::other("not direct-tasked"));
        };
        let mut writer = writer.lock().expect("poisoned direct tasked writer");
        let Some(writer) = writer.as_mut() else {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "direct writer is closed",
            ));
        };
        match writer {
            TaskedDirectWriter::Tcp(stream) => {
                #[cfg(windows)]
                // A paused reader does not consume Winsock's asynchronous reset
                // notification. Surface it before another small write can appear
                // to succeed from the local send buffer.
                if !self.is_reading()
                    && let Some(err) = stream.take_error()?
                {
                    return Err(err);
                }

                stream.as_ref().write(data)
            }
            #[cfg(unix)]
            TaskedDirectWriter::Unix(stream) => stream.write(data),
        }
    }

    pub(super) fn fail_write(self: &Arc<Self>, err: Option<io::Error>) {
        if self.is_closing() {
            return;
        }

        // Queue the write error before waking a paused reader. Otherwise the
        // reader can observe `closing` and race this event with a clean loss.
        self.enqueue_pending_read_event(PendingReadEvent::ConnectionLost(
            err.map(|err| err.to_string()),
        ));
        self.set_closing();
        self.set_write_backpressure_active(false);
        self.pending_direct_write
            .lock()
            .expect("poisoned pending direct write")
            .clear();
        self.direct_write_scheduled.store(false, Ordering::Release);
        self.clear_write_buffer(false);
        let _ = self.writer_tx.send(WriterCommand::Stop);
    }

    pub(super) fn queue_write(self: &Arc<Self>, data: OwnedWriteBuffer) -> io::Result<()> {
        let should_pause = self.record_write_buffer_enqueued(data.remaining().len())?;
        self.ensure_writer_worker();
        if should_pause {
            self.notify_pause_writing();
        }
        if self.writer_tx.send(WriterCommand::Data(data)).is_err() {
            self.clear_write_buffer(false);
            self.fail_write(None);
        }
        Ok(())
    }

    pub(super) fn queue_recorded_write(self: &Arc<Self>, data: OwnedWriteBuffer) {
        self.ensure_writer_worker();
        if self.writer_tx.send(WriterCommand::Data(data)).is_err() {
            self.clear_write_buffer(false);
            self.fail_write(None);
        }
    }

    pub(super) fn stage_direct_write(self: &Arc<Self>, data: &[u8]) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        if transport_stats_enabled() {
            TRANSPORT_STAGED_WRITES.fetch_add(1, Ordering::Relaxed);
        }

        let should_pause = self.record_write_buffer_enqueued(data.len())?;
        self.pending_direct_write
            .lock()
            .expect("poisoned pending direct write")
            .extend_from_slice(data);
        if should_pause {
            self.notify_pause_writing();
        }

        if !self.direct_write_scheduled.swap(true, Ordering::AcqRel)
            && self
                .loop_core
                .send_command(LoopCommand::Transport(LoopTransportCommand::StreamWrite(
                    Arc::clone(self),
                )))
                .is_err()
        {
            self.direct_write_scheduled.store(false, Ordering::Release);
            self.fail_write(None);
        }
        Ok(())
    }

    pub(crate) fn flush_pending_direct_write(self: &Arc<Self>) {
        profiling::scope!("StreamTransportCore::flush_pending_direct_write");
        #[cfg(windows)]
        if self.poll_reader_requested() && !self.poll_reader_ready.load(Ordering::Acquire) {
            return;
        }
        self.direct_write_scheduled.store(false, Ordering::Release);
        let data = std::mem::take(
            self.pending_direct_write
                .lock()
                .expect("poisoned pending direct write")
                .deref_mut(),
        );
        if data.is_empty() {
            return;
        }
        if self.is_closing() {
            self.record_write_buffer_drained(data.len());
            return;
        }

        match self.try_direct_tasked_write(&data) {
            Ok(written) if written == data.len() => {
                self.record_write_buffer_drained(written);
            }
            Ok(written) => {
                self.record_write_buffer_drained(written);
                let mut pending = OwnedWriteBuffer::from_vec(data);
                pending.advance(written);
                self.set_write_backpressure_active(true);
                self.queue_recorded_write(pending);
            }
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                self.set_write_backpressure_active(true);
                self.queue_recorded_write(OwnedWriteBuffer::from_vec(data));
            }
            Err(err) => self.fail_write(Some(err)),
        }
    }

    pub(super) fn discard_pending_direct_write(self: &Arc<Self>) {
        self.direct_write_scheduled.store(false, Ordering::Release);
        let discarded = {
            let mut pending = self
                .pending_direct_write
                .lock()
                .expect("poisoned pending direct write");
            let len = pending.len();
            pending.clear();
            len
        };
        self.record_write_buffer_drained(discarded);
    }

    pub(super) fn try_write_bytes(self: &Arc<Self>, data: &[u8]) -> io::Result<()> {
        profiling::scope!("StreamTransportCore::try_write_bytes");
        #[cfg(windows)]
        if data.len() >= SERVER_POLL_READER_WRITE_THRESHOLD {
            if self.server_side {
                self.request_poll_reader();
            }
            if self.poll_reader_requested() && !self.poll_reader_ready.load(Ordering::Acquire) {
                return self.stage_direct_write(data);
            }
        }

        if self.direct_writer.is_some() && !self.write_backpressure_active() {
            if self.direct_write_scheduled.load(Ordering::Acquire)
                || (self.coalesce_small_server_writes
                    && data.len() > SMALL_WRITE_COALESCE_MIN_BYTES
                    && data.len() <= SMALL_WRITE_COALESCE_MAX_BYTES)
            {
                return self.stage_direct_write(data);
            }
            match self.try_direct_tasked_write(data) {
                Ok(written) if written == data.len() => return Ok(()),
                Ok(written) => {
                    let mut pending = OwnedWriteBuffer::from_slice(data);
                    pending.advance(written);
                    self.set_write_backpressure_active(true);
                    return self.queue_write(pending);
                }
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) =>
                {
                    self.set_write_backpressure_active(true);
                    return self.queue_write(OwnedWriteBuffer::from_slice(data));
                }
                Err(err) => {
                    self.fail_write(Some(err));
                    return Ok(());
                }
            }
        }

        self.queue_write(OwnedWriteBuffer::from_slice(data))
    }

    pub async fn wait_readable(self: &Arc<Self>) -> io::Result<()> {
        Err(io::Error::other(
            "transport readiness is not used in std transport mode",
        ))
    }

    pub async fn wait_writable(self: &Arc<Self>) -> io::Result<()> {
        Err(io::Error::other(
            "transport readiness is not used in std transport mode",
        ))
    }

    pub fn handle_read_ready_with_py(self: &Arc<Self>, _py: Python<'_>) {}

    pub fn handle_write_ready_with_py(self: &Arc<Self>, _py: Python<'_>) {}

    pub(super) fn upgrade_stream(
        self: &Arc<Self>,
        py: Python<'_>,
    ) -> PyResult<(TransportSpawnContext, StreamKind)> {
        self.flush_pending_direct_write();
        let protocol = self.get_protocol(py);
        let context = self
            .state
            .lock()
            .expect("poisoned transport state")
            .context
            .clone_ref(py);
        let context_needs_run = self
            .state
            .lock()
            .expect("poisoned transport state")
            .context_needs_run;
        let socket = self
            .get_extra(py, "socket")
            .ok_or_else(|| PyRuntimeError::new_err("transport does not expose a socket"))?;
        let fd = fd_ops::dup_raw_fd(socket.bind(py).call_method0("fileno")?.extract()?)
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;

        #[cfg(windows)]
        if self.runtime_socket_fd().is_some() {
            self.request_poll_reader();
            self.wait_for_poll_reader()
                .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        }

        self.detach_underlying_stream(py);
        let _ = self.writer_tx.send(WriterCommand::Stop);
        if let Some(fd) = self.runtime_socket_fd() {
            stop_socket_reader(self, fd).map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        }
        self.abort_workers();
        self.release_direct_writer();

        #[allow(unused_variables)]
        let family = socket.bind(py).getattr("family")?.extract::<i32>()?;
        #[cfg(unix)]
        let stream = if family == libc::AF_UNIX {
            StreamKind::Unix(unix_stream_from_owned_socket_fd(fd)?)
        } else {
            StreamKind::Tcp(tcp_stream_from_owned_socket_fd(fd)?)
        };
        #[cfg(not(unix))]
        let stream = StreamKind::Tcp(tcp_stream_from_owned_socket_fd(fd)?);

        Ok((
            TransportSpawnContext::new(
                py,
                Arc::clone(&self.loop_core),
                &self.loop_obj,
                protocol,
                &context,
                context_needs_run,
            ),
            stream,
        ))
    }

    pub(super) fn pause_writing_with_py(&self, py: Python<'_>) -> PyResult<()> {
        let (callback, context, context_needs_run) = {
            let state = self.state.lock().expect("poisoned transport state");
            (
                state.callbacks.pause_writing.clone_ref(py),
                state.context.clone_ref(py),
                state.context_needs_run,
            )
        };

        if let Err(err) = self.call_protocol_method0(py, &callback, &context, context_needs_run) {
            self.report_error_with_py(py, err, "protocol.pause_writing() failed")?;
        }
        Ok(())
    }

    pub(super) fn resume_writing_with_py(&self, py: Python<'_>) -> PyResult<()> {
        let (callback, context, context_needs_run) = {
            let state = self.state.lock().expect("poisoned transport state");
            (
                state.callbacks.resume_writing.clone_ref(py),
                state.context.clone_ref(py),
                state.context_needs_run,
            )
        };

        if let Err(err) = self.call_protocol_method0(py, &callback, &context, context_needs_run) {
            self.report_error_with_py(py, err, "protocol.resume_writing() failed")?;
        }
        Ok(())
    }

    pub(super) fn notify_pause_writing(self: &Arc<Self>) {
        if self.loop_core.on_runtime_thread() {
            let _ = self.call_in_loop_context(|py| self.pause_writing_with_py(py));
            return;
        }

        self.enqueue_pending_read_event(PendingReadEvent::PauseWriting);
    }

    pub(super) fn notify_resume_writing(self: &Arc<Self>) {
        if self.loop_core.on_runtime_thread() {
            let _ = self.call_in_loop_context(|py| self.resume_writing_with_py(py));
            return;
        }

        self.enqueue_pending_read_event(PendingReadEvent::ResumeWriting);
    }

    pub(super) fn record_write_buffer_enqueued(&self, len: usize) -> io::Result<bool> {
        if len == 0 {
            return Ok(false);
        }

        let mut state = self.state.lock().expect("poisoned transport state");
        if len > max_write_buffer_size().saturating_sub(state.write_buffer.size) {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!(
                    "transport write buffer exceeds {} bytes",
                    max_write_buffer_size()
                ),
            ));
        }
        state.write_buffer.size = state.write_buffer.size.saturating_add(len);
        if state.write_buffer.size > state.write_buffer.high_water
            && !state.write_buffer.protocol_paused
        {
            state.write_buffer.protocol_paused = true;
            return Ok(true);
        }

        Ok(false)
    }

    pub(super) fn record_write_buffer_drained(self: &Arc<Self>, len: usize) {
        if len == 0 {
            return;
        }

        let should_resume = {
            let mut state = self.state.lock().expect("poisoned transport state");
            state.write_buffer.size = state.write_buffer.size.saturating_sub(len);
            if state.write_buffer.protocol_paused
                && state.write_buffer.size <= state.write_buffer.low_water
            {
                state.write_buffer.protocol_paused = false;
                true
            } else {
                false
            }
        };

        if should_resume {
            self.notify_resume_writing();
        }
    }

    pub(super) fn clear_write_buffer(self: &Arc<Self>, resume_protocol: bool) {
        let should_resume = {
            let mut state = self.state.lock().expect("poisoned transport state");
            let should_resume = resume_protocol && state.write_buffer.protocol_paused;
            state.write_buffer.size = 0;
            state.write_buffer.protocol_paused = false;
            should_resume
        };

        if should_resume {
            self.notify_resume_writing();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::mpsc::{self, Receiver};

    use pyo3::prelude::*;

    use super::{OwnedWriteBuffer, StreamTransportCore};
    use crate::engine::LoopCore;
    use crate::transport::stream::builder::{
        StreamTransportStateConfig, new_stream_transport_core, stream_transport_state_parts,
    };
    use crate::transport::stream::protocol::build_protocol_callbacks;
    use crate::transport::stream::{TransportSpawnContext, WriterCommand};

    #[pyclass]
    #[derive(Default)]
    struct RecordingProtocol {
        events: Vec<&'static str>,
        pause_attempts: usize,
        panic_next_pause: bool,
    }

    #[pymethods]
    impl RecordingProtocol {
        fn connection_made(&self, _transport: Py<PyAny>) {}

        fn connection_lost(&mut self, _error: Py<PyAny>) {
            self.events.push("lost");
        }

        fn pause_writing(&mut self) {
            self.pause_attempts += 1;
            assert!(
                !std::mem::take(&mut self.panic_next_pause),
                "intentional pause_writing panic"
            );
            self.events.push("pause");
        }

        fn resume_writing(&mut self) {
            self.events.push("resume");
        }
    }

    fn build_test_core(
        py: Python<'_>,
    ) -> (
        Arc<StreamTransportCore>,
        Receiver<WriterCommand>,
        Arc<LoopCore>,
        Py<RecordingProtocol>,
    ) {
        let loop_core = LoopCore::new();
        let protocol = Py::new(py, RecordingProtocol::default()).expect("recording protocol");
        let protocol_any = protocol.clone_ref(py).into_any();
        let callbacks =
            build_protocol_callbacks(py, &protocol_any).expect("recording protocol callbacks");
        let parts = stream_transport_state_parts(
            TransportSpawnContext {
                loop_core: Arc::clone(&loop_core),
                loop_obj: py.None(),
                protocol: protocol_any,
                context: py.None(),
                context_needs_run: false,
            },
            callbacks,
            StreamTransportStateConfig {
                io_fd: None,
                runtime_socket_io: false,
                extra: HashMap::new(),
                lazy_socket_family: None,
                reading: false,
                writable: true,
                can_write_eof: false,
                close_on_write_eof: false,
                server: None,
            },
        );
        let (writer_tx, writer_rx) = mpsc::channel();
        let core = new_stream_transport_core(parts, writer_tx, None, None);
        loop_core.mark_runtime_thread();
        (core, writer_rx, loop_core, protocol)
    }

    fn shutdown_test_core(
        core: Arc<StreamTransportCore>,
        writer_rx: Receiver<WriterCommand>,
        loop_core: Arc<LoopCore>,
    ) {
        loop_core.clear_runtime_thread();
        drop(core);
        drop(writer_rx);
        loop_core.close().expect("close test loop");
    }

    #[test]
    fn write_buffer_pauses_and_resumes_only_at_watermark_crossings() {
        crate::initialize_python_for_tests();
        Python::attach(|py| {
            let (core, writer_rx, loop_core, protocol) = build_test_core(py);
            core.set_write_buffer_limits(Some(10), Some(4))
                .expect("valid write buffer limits");

            core.queue_write(OwnedWriteBuffer::from_slice(&[0; 10]))
                .expect("write at high water");
            assert!(protocol.borrow(py).events.is_empty());

            core.queue_write(OwnedWriteBuffer::from_slice(&[0; 1]))
                .expect("write above high water");
            assert_eq!(protocol.borrow(py).events, ["pause"]);

            core.queue_write(OwnedWriteBuffer::from_slice(&[0; 5]))
                .expect("write while paused");
            assert_eq!(protocol.borrow(py).events, ["pause"]);

            core.record_write_buffer_drained(11);
            assert_eq!(core.get_write_buffer_size(), 5);
            assert_eq!(protocol.borrow(py).events, ["pause"]);

            core.record_write_buffer_drained(1);
            assert_eq!(core.get_write_buffer_size(), 4);
            assert_eq!(protocol.borrow(py).events, ["pause", "resume"]);

            core.record_write_buffer_drained(4);
            assert_eq!(core.get_write_buffer_size(), 0);
            assert_eq!(protocol.borrow(py).events, ["pause", "resume"]);

            shutdown_test_core(core, writer_rx, loop_core);
        });
    }

    #[test]
    fn changing_watermarks_reconciles_the_current_buffer_state() {
        crate::initialize_python_for_tests();
        Python::attach(|py| {
            let (core, writer_rx, loop_core, protocol) = build_test_core(py);
            core.set_write_buffer_limits(Some(10), Some(4))
                .expect("valid initial limits");
            core.queue_write(OwnedWriteBuffer::from_slice(&[0; 8]))
                .expect("buffer write");

            core.set_write_buffer_limits(Some(7), Some(3))
                .expect("lower limits");
            assert_eq!(core.get_write_buffer_limits(), (3, 7));
            assert_eq!(protocol.borrow(py).events, ["pause"]);

            core.set_write_buffer_limits(Some(20), Some(10))
                .expect("raise limits");
            assert_eq!(core.get_write_buffer_limits(), (10, 20));
            assert_eq!(protocol.borrow(py).events, ["pause", "resume"]);

            core.set_write_buffer_limits(Some(20), Some(10))
                .expect("unchanged limits");
            assert_eq!(protocol.borrow(py).events, ["pause", "resume"]);

            shutdown_test_core(core, writer_rx, loop_core);
        });
    }

    #[test]
    fn invalid_limits_and_connection_loss_leave_write_state_consistent() {
        crate::initialize_python_for_tests();
        Python::attach(|py| {
            let (core, writer_rx, loop_core, protocol) = build_test_core(py);
            core.set_write_buffer_limits(Some(10), Some(4))
                .expect("valid write buffer limits");

            let error = core
                .set_write_buffer_limits(Some(3), Some(4))
                .expect_err("low water above high water should fail");
            assert!(error.is_instance_of::<pyo3::exceptions::PyValueError>(py));
            assert_eq!(core.get_write_buffer_limits(), (4, 10));

            core.queue_write(OwnedWriteBuffer::from_slice(&[0; 11]))
                .expect("write above high water");
            assert_eq!(protocol.borrow(py).events, ["pause"]);
            assert_eq!(core.get_write_buffer_size(), 11);

            core.connection_lost_with_py(py, None)
                .expect("connection loss callback");
            assert_eq!(core.get_write_buffer_size(), 0);
            assert_eq!(protocol.borrow(py).events, ["pause", "lost"]);
            assert!(core.is_closing());
            assert!(
                !core
                    .state
                    .lock()
                    .expect("transport state")
                    .write_buffer
                    .protocol_paused
            );

            core.record_write_buffer_drained(11);
            assert_eq!(protocol.borrow(py).events, ["pause", "lost"]);

            shutdown_test_core(core, writer_rx, loop_core);
        });
    }

    #[test]
    fn panicking_protocol_callback_leaves_transport_locks_unpoisoned() {
        crate::initialize_python_for_tests();
        Python::attach(|py| {
            let (core, writer_rx, loop_core, protocol) = build_test_core(py);
            protocol.borrow_mut(py).panic_next_pause = true;
            core.set_write_buffer_limits(Some(10), Some(4))
                .expect("valid write buffer limits");

            let callback_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                core.queue_write(OwnedWriteBuffer::from_slice(&[0; 11]))
                    .expect("write crossing high water");
            }));

            assert!(callback_panic.is_err());
            assert!(!core.state.is_poisoned());
            assert!(!loop_core.state.is_poisoned());
            assert_eq!(core.get_write_buffer_size(), 11);
            assert_eq!(protocol.borrow(py).pause_attempts, 1);
            assert!(protocol.borrow(py).events.is_empty());

            core.record_write_buffer_drained(11);
            core.queue_write(OwnedWriteBuffer::from_slice(&[0; 11]))
                .expect("write after callback panic");

            assert!(!core.state.is_poisoned());
            assert_eq!(protocol.borrow(py).pause_attempts, 2);
            assert_eq!(protocol.borrow(py).events, ["resume", "pause"]);

            core.clear_write_buffer(false);
            shutdown_test_core(core, writer_rx, loop_core);
        });
    }
}
