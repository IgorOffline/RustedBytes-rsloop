//! The transport's event queue and the worker threads that feed it.
//!
//! I/O happens off the loop thread, but every Python callback must run on it.
//! Reader workers and TLS sessions therefore enqueue `PendingReadEvent`s here
//! and ask the loop for a drain; `drain_pending_read_events_with_py` is what
//! actually runs under the GIL, bounded by `MAX_READ_EVENTS_PER_DRAIN` and
//! `MAX_READ_BYTES_PER_DRAIN` so one busy connection cannot starve the loop.
//!
//! On Windows a server reader alternates between an overlapped receive and a
//! readiness-mode poll; `request_poll_reader` / `wait_for_poll_reader` are the
//! handshake that lets a writer cancel the in-flight receive and wait for the
//! reader to rebind before it takes the socket.

use std::collections::VecDeque;
use std::io;
use std::ops::DerefMut;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
#[cfg(windows)]
use std::time::Duration;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
#[cfg(windows)]
use windows_sys::Win32::{Foundation::HANDLE, System::IO::CancelIoEx};

use super::buffers::PendingReadBuffer;
use super::io_targets::LazyWriterConfig;
#[cfg(windows)]
use super::stats::TRANSPORT_POLL_REBINDS;
use super::stats::{
    TRANSPORT_PYTHON_READ_DRAINS, TRANSPORT_READ_BYTES, TRANSPORT_READ_EVENTS,
    TRANSPORT_READ_WAKEUPS, transport_stats_enabled,
};
use super::tuning::{
    MAX_PENDING_READ_COALESCE_BYTES, MAX_READ_BYTES_PER_DRAIN, MAX_READ_EVENTS_PER_DRAIN,
};
use super::worker::WorkerThread;
use super::{
    PendingReadEvent, ServerCore, StreamTransportCore, WriterCommand, spawn_writer_worker,
};
use crate::context::ensure_running_loop;
use crate::engine::{LoopCommand, LoopTransportCommand};

impl StreamTransportCore {
    #[cfg(windows)]
    #[inline]
    pub(super) fn poll_reader_requested(&self) -> bool {
        self.poll_reader_requested.load(Ordering::Acquire)
    }

    #[cfg(windows)]
    pub(super) fn request_poll_reader(&self) {
        if self.poll_reader_requested.swap(true, Ordering::AcqRel) {
            return;
        }

        let fd = self.state.lock().expect("poisoned transport state").io_fd;
        if let Some(fd) = fd {
            // Wake a server reader currently blocked in WSARecv. The operation
            // still completes through vibeio's IOCP driver with
            // ERROR_OPERATION_ABORTED; the reader handles that result by
            // rebinding the same socket to readiness mode. Cancel before the
            // direct write so a pending receive cannot throttle the duplicate
            // writer socket during a bulk response.
            // SAFETY: `fd` is the live transport handle; a null OVERLAPPED requests all pending IO.
            let _ = unsafe { CancelIoEx(fd as HANDLE, std::ptr::null()) };
        }
    }

    #[cfg(windows)]
    pub(super) fn mark_poll_reader_ready(self: &Arc<Self>, rebound: bool) {
        if rebound && transport_stats_enabled() {
            TRANSPORT_POLL_REBINDS.fetch_add(1, Ordering::Relaxed);
        }
        self.poll_reader_ready.store(true, Ordering::Release);
        self.state_cv.notify_all();
        if self.server_side && self.direct_write_scheduled.load(Ordering::Acquire) {
            let _ = self.loop_core.send_command(LoopCommand::Transport(
                LoopTransportCommand::StreamWrite(Arc::clone(self)),
            ));
        }
    }

    #[cfg(windows)]
    pub(super) fn wait_for_poll_reader(&self) -> io::Result<()> {
        if self.poll_reader_ready.load(Ordering::Acquire) {
            return Ok(());
        }

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut state = self.state.lock().expect("poisoned transport state");
        while !self.poll_reader_ready.load(Ordering::Acquire) {
            let now = std::time::Instant::now();
            if now >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out rebinding completion reader for synchronous socket reclaim",
                ));
            }
            let (guard, _) = self
                .state_cv
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .expect("poisoned transport state");
            state = guard;
        }
        Ok(())
    }

    pub(super) fn close_extra_socket_with_py(&self, py: Python<'_>) {
        let socket = self
            .state
            .lock()
            .expect("poisoned transport state")
            .extra
            .get("socket")
            .map(|value| value.clone_ref(py));
        if let Some(socket) = socket {
            let _ = socket.bind(py).call_method0("close");
        }
    }

    #[inline]
    pub(super) fn register_worker(&self, worker: WorkerThread) {
        self.workers
            .lock()
            .expect("poisoned transport workers")
            .push(worker);
    }

    pub(super) fn abort_workers(&self) {
        let workers = self
            .workers
            .lock()
            .expect("poisoned transport workers")
            .drain(..)
            .collect::<Vec<_>>();
        for worker in workers {
            worker.abort();
        }
    }

    pub(super) fn ensure_writer_worker(self: &Arc<Self>) {
        let lazy = self
            .lazy_writer
            .lock()
            .expect("poisoned lazy writer")
            .take();
        let Some(LazyWriterConfig { target, writer_rx }) = lazy else {
            return;
        };
        match target.materialize() {
            Ok(target) => {
                if let Err(err) = spawn_writer_worker(Arc::clone(self), target, writer_rx) {
                    self.fail_write(Some(err));
                }
            }
            Err(err) => self.fail_write(Some(io::Error::other(err.to_string()))),
        }
    }

    #[inline]
    pub(super) fn server_ref(&self) -> Option<Weak<ServerCore>> {
        self.state
            .lock()
            .expect("poisoned transport state")
            .server
            .as_ref()
            .cloned()
    }

    pub(super) fn call_in_loop_context<T>(
        &self,
        f: impl for<'py> FnOnce(Python<'py>) -> PyResult<T>,
    ) -> PyResult<T> {
        Python::attach(|py| {
            if !self.loop_core.on_runtime_thread() {
                ensure_running_loop(py, &self.loop_obj)?;
            }
            f(py)
        })
    }

    pub(super) fn enqueue_pending_read_event(self: &Arc<Self>, event: PendingReadEvent) {
        profiling::scope!("StreamTransportCore::enqueue_pending_read_event");
        // A start_tls handoff retires this core before reusing the socket. A
        // cancelled plaintext reader may still complete once; never deliver
        // that late event to the application protocol after the handoff.
        if self.detached.load(Ordering::Acquire) {
            if let PendingReadEvent::Data(data) = event {
                self.read_buffer_pool.release(data);
            }
            return;
        }
        let data_len = match &event {
            PendingReadEvent::Data(data) => data.len(),
            _ => 0,
        };
        if data_len > 0 {
            if transport_stats_enabled() {
                TRANSPORT_READ_EVENTS.fetch_add(1, Ordering::Relaxed);
                TRANSPORT_READ_BYTES.fetch_add(data_len as u64, Ordering::Relaxed);
            }
            self.pending_read_bytes
                .fetch_add(data_len, Ordering::AcqRel);
        }
        self.pending_read_events
            .lock()
            .expect("poisoned pending read queue")
            .push_back(event);
        if data_len > 0 {
            self.apply_pending_read_backpressure();
        }

        if !self.read_events_scheduled.swap(true, Ordering::AcqRel) {
            if transport_stats_enabled() {
                TRANSPORT_READ_WAKEUPS.fetch_add(1, Ordering::Relaxed);
            }
            if self
                .loop_core
                .send_command(LoopCommand::Transport(LoopTransportCommand::StreamRead(
                    Arc::clone(self),
                )))
                .is_err()
            {
                self.read_events_scheduled.store(false, Ordering::Release);
            }
        }
    }

    pub(crate) fn drain_pending_read_events_with_py(
        self: &Arc<Self>,
        py: Python<'_>,
    ) -> PyResult<()> {
        profiling::scope!("StreamTransportCore::drain_pending_read_events_with_py");
        if transport_stats_enabled() {
            TRANSPORT_PYTHON_READ_DRAINS.fetch_add(1, Ordering::Relaxed);
        }
        // Snapshot the reader fast path once per drain instead of re-locking
        // transport state for every data event.
        let fast_path = {
            let state = self.state.lock().expect("poisoned transport state");
            state
                .callbacks
                .stream_reader_fast_path
                .as_ref()
                .map(|value| value.clone_ref(py))
        };
        let fast_path = fast_path.as_ref();
        let mut pending_data: Option<PendingReadBuffer> = None;
        let mut drained = self
            .read_event_drain
            .lock()
            .expect("poisoned read event drain queue");
        let mut drained_events = 0;
        let mut drained_bytes = 0;
        loop {
            {
                let mut queue = self
                    .pending_read_events
                    .lock()
                    .expect("poisoned pending read queue");
                if queue.is_empty() {
                    self.read_events_scheduled.store(false, Ordering::Release);
                    return Ok(());
                }

                std::mem::swap(drained.deref_mut(), queue.deref_mut());
            }

            while let Some(event) = drained.pop_front() {
                match event {
                    PendingReadEvent::Data(data) => {
                        profiling::scope!("stream.pending.data");
                        self.record_pending_read_drained(data.len());
                        drained_events += 1;
                        drained_bytes += data.len();
                        if let Some(fast_path) = fast_path.as_ref() {
                            match fast_path.feed_owned_data(py, data, &self.read_buffer_pool) {
                                Ok(()) => {}
                                Err(err) => {
                                    let _ = self.report_error_with_py(
                                        py,
                                        err,
                                        "stream data_received callback failed",
                                    );
                                    let _ = self.connection_lost_with_py(py, None);
                                    self.read_events_scheduled.store(false, Ordering::Release);
                                    return Ok(());
                                }
                            }
                        } else {
                            match &mut pending_data {
                                Some(buffer)
                                    if buffer.len() + data.len()
                                        <= MAX_PENDING_READ_COALESCE_BYTES =>
                                {
                                    buffer.extend(&data);
                                    self.read_buffer_pool.release(data);
                                }
                                Some(_) => {
                                    if let Err(err) = self.flush_pending_data_with_py(
                                        py,
                                        &mut pending_data,
                                        fast_path,
                                    ) {
                                        let _ = self.report_error_with_py(
                                            py,
                                            err,
                                            "stream data_received callback failed",
                                        );
                                        let _ = self.connection_lost_with_py(py, None);
                                        self.read_events_scheduled.store(false, Ordering::Release);
                                        return Ok(());
                                    }
                                    let mut buffer =
                                        PendingReadBuffer::new(&self.read_coalesce_buffer);
                                    buffer.extend(&data);
                                    self.read_buffer_pool.release(data);
                                    pending_data = Some(buffer);
                                }
                                None => {
                                    let mut buffer =
                                        PendingReadBuffer::new(&self.read_coalesce_buffer);
                                    buffer.extend(&data);
                                    self.read_buffer_pool.release(data);
                                    pending_data = Some(buffer);
                                }
                            }
                        }

                        if (drained_events >= MAX_READ_EVENTS_PER_DRAIN
                            || drained_bytes >= MAX_READ_BYTES_PER_DRAIN)
                            && self.reschedule_pending_read_events(&mut drained)
                        {
                            self.flush_pending_data_with_py(py, &mut pending_data, fast_path)?;
                            return Ok(());
                        }
                    }
                    PendingReadEvent::Eof => {
                        profiling::scope!("stream.pending.eof");
                        if let Err(err) =
                            self.flush_pending_data_with_py(py, &mut pending_data, fast_path)
                        {
                            let _ = self.report_error_with_py(
                                py,
                                err,
                                "stream data_received callback failed",
                            );
                            let _ = self.connection_lost_with_py(py, None);
                            self.read_events_scheduled.store(false, Ordering::Release);
                            return Ok(());
                        }
                        match self.eof_received_with_py(py) {
                            Ok(true) => {
                                self.read_events_scheduled.store(false, Ordering::Release);
                                return Ok(());
                            }
                            Ok(false) => {
                                self.set_closing();
                                let _ = self.writer_tx.send(WriterCommand::Close);
                                let _ = self.connection_lost_with_py(py, None);
                                self.read_events_scheduled.store(false, Ordering::Release);
                                return Ok(());
                            }
                            Err(err) => {
                                let _ = self.report_error_with_py(
                                    py,
                                    err,
                                    "stream eof_received callback failed",
                                );
                                let _ = self.connection_lost_with_py(py, None);
                                self.read_events_scheduled.store(false, Ordering::Release);
                                return Ok(());
                            }
                        }
                    }
                    PendingReadEvent::ConnectionLost(message) => {
                        profiling::scope!("stream.pending.connection_lost");
                        if let Err(err) =
                            self.flush_pending_data_with_py(py, &mut pending_data, fast_path)
                        {
                            let _ = self.report_error_with_py(
                                py,
                                err,
                                "stream data_received callback failed",
                            );
                            let _ = self.connection_lost_with_py(py, None);
                            self.read_events_scheduled.store(false, Ordering::Release);
                            return Ok(());
                        }
                        let err = message.map(PyRuntimeError::new_err);
                        let _ = self.connection_lost_with_py(py, err);
                        self.read_events_scheduled.store(false, Ordering::Release);
                        return Ok(());
                    }
                    PendingReadEvent::PauseWriting => {
                        profiling::scope!("stream.pending.pause_writing");
                        if let Err(err) =
                            self.flush_pending_data_with_py(py, &mut pending_data, fast_path)
                        {
                            let _ = self.report_error_with_py(
                                py,
                                err,
                                "stream data_received callback failed",
                            );
                            let _ = self.connection_lost_with_py(py, None);
                            self.read_events_scheduled.store(false, Ordering::Release);
                            return Ok(());
                        }
                        self.pause_writing_with_py(py)?;
                    }
                    PendingReadEvent::ResumeWriting => {
                        profiling::scope!("stream.pending.resume_writing");
                        if let Err(err) =
                            self.flush_pending_data_with_py(py, &mut pending_data, fast_path)
                        {
                            let _ = self.report_error_with_py(
                                py,
                                err,
                                "stream data_received callback failed",
                            );
                            let _ = self.connection_lost_with_py(py, None);
                            self.read_events_scheduled.store(false, Ordering::Release);
                            return Ok(());
                        }
                        self.resume_writing_with_py(py)?;
                    }
                }
            }

            if let Err(err) = self.flush_pending_data_with_py(py, &mut pending_data, fast_path) {
                let _ = self.report_error_with_py(py, err, "stream data_received callback failed");
                let _ = self.connection_lost_with_py(py, None);
                self.read_events_scheduled.store(false, Ordering::Release);
                return Ok(());
            }
        }
    }

    pub(super) fn reschedule_pending_read_events(
        self: &Arc<Self>,
        drained: &mut VecDeque<PendingReadEvent>,
    ) -> bool {
        let mut queue = self
            .pending_read_events
            .lock()
            .expect("poisoned pending read queue");
        if drained.is_empty() && queue.is_empty() {
            return false;
        }

        drained.append(&mut queue);
        std::mem::swap(drained, queue.deref_mut());
        drop(queue);
        let _ =
            self.loop_core
                .send_command(LoopCommand::Transport(LoopTransportCommand::StreamRead(
                    Arc::clone(self),
                )));
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use pyo3::Python;

    use super::PendingReadEvent;
    use crate::transport::stream::test_support::{
        build_test_core, install_exception_handler, shutdown_test_core,
    };
    use crate::transport::stream::tuning::{
        MAX_READ_BYTES_PER_DRAIN, MAX_READ_EVENTS_PER_DRAIN, PENDING_READ_HIGH_WATER,
        PENDING_READ_LOW_WATER,
    };

    #[test]
    fn pending_read_backpressure_uses_exact_high_and_low_boundaries() {
        crate::initialize_python_for_tests();
        Python::attach(|py| {
            let (core, writer_rx, loop_core, _protocol) = build_test_core(py);
            {
                let mut state = core.state.lock().expect("transport state");
                state.reading = true;
            }
            core.reading.store(true, Ordering::Release);
            core.pending_read_bytes
                .store(PENDING_READ_HIGH_WATER - 1, Ordering::Release);
            core.apply_pending_read_backpressure();
            assert!(core.is_reading());

            core.pending_read_bytes
                .store(PENDING_READ_HIGH_WATER, Ordering::Release);
            core.apply_pending_read_backpressure();
            assert!(!core.is_reading());
            assert!(
                core.state
                    .lock()
                    .expect("transport state")
                    .read_backpressured
            );

            core.record_pending_read_drained(PENDING_READ_HIGH_WATER - PENDING_READ_LOW_WATER - 1);
            assert!(!core.is_reading());
            core.record_pending_read_drained(1);
            assert!(core.is_reading());
            assert_eq!(
                core.pending_read_bytes.load(Ordering::Acquire),
                PENDING_READ_LOW_WATER
            );

            shutdown_test_core(core, writer_rx, loop_core);
        });
    }

    #[test]
    fn automatic_read_resume_respects_user_pause_and_closing() {
        crate::initialize_python_for_tests();
        Python::attach(|py| {
            let (core, writer_rx, loop_core, _protocol) = build_test_core(py);
            {
                let mut state = core.state.lock().expect("transport state");
                state.reading = true;
                state.read_paused = true;
            }
            core.pending_read_bytes
                .store(PENDING_READ_HIGH_WATER, Ordering::Release);
            core.apply_pending_read_backpressure();
            core.record_pending_read_drained(PENDING_READ_HIGH_WATER - PENDING_READ_LOW_WATER);
            assert!(!core.is_reading());
            assert!(core.state.lock().expect("transport state").read_paused);

            {
                let mut state = core.state.lock().expect("transport state");
                state.read_paused = false;
                state.closing = true;
                state.read_backpressured = false;
            }
            core.pending_read_bytes
                .store(PENDING_READ_HIGH_WATER, Ordering::Release);
            core.apply_pending_read_backpressure();
            assert!(
                !core
                    .state
                    .lock()
                    .expect("transport state")
                    .read_backpressured
            );

            shutdown_test_core(core, writer_rx, loop_core);
        });
    }

    #[test]
    fn drain_coalesces_data_and_preserves_eof_ordering() {
        crate::initialize_python_for_tests();
        Python::attach(|py| {
            let (core, writer_rx, loop_core, protocol) = build_test_core(py);
            core.read_events_scheduled.store(true, Ordering::Release);
            core.enqueue_pending_read_event(PendingReadEvent::Data(b"hello ".to_vec()));
            core.enqueue_pending_read_event(PendingReadEvent::Data(b"world".to_vec()));
            core.enqueue_pending_read_event(PendingReadEvent::Eof);

            core.drain_pending_read_events_with_py(py)
                .expect("drain read events");

            assert_eq!(protocol.borrow(py).received, [b"hello world".to_vec()]);
            assert_eq!(protocol.borrow(py).events, ["data", "eof", "lost"]);
            assert_eq!(core.pending_read_bytes.load(Ordering::Acquire), 0);
            assert!(!core.read_events_scheduled.load(Ordering::Acquire));

            shutdown_test_core(core, writer_rx, loop_core);
        });
    }

    #[test]
    fn drain_budget_reschedules_remaining_events_without_reordering() {
        crate::initialize_python_for_tests();
        Python::attach(|py| {
            let (core, writer_rx, loop_core, protocol) = build_test_core(py);
            core.read_events_scheduled.store(true, Ordering::Release);
            let event_count = MAX_READ_EVENTS_PER_DRAIN + 2;
            for value in 0..event_count {
                core.enqueue_pending_read_event(PendingReadEvent::Data(vec![
                    u8::try_from(value).expect("small event index"),
                ]));
            }

            core.drain_pending_read_events_with_py(py)
                .expect("first bounded drain");
            assert_eq!(protocol.borrow(py).received.len(), 1);
            assert_eq!(
                protocol.borrow(py).received[0],
                (0..MAX_READ_EVENTS_PER_DRAIN)
                    .map(|value| u8::try_from(value).expect("small event index"))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                core.pending_read_events
                    .lock()
                    .expect("pending read events")
                    .len(),
                2
            );

            core.drain_pending_read_events_with_py(py)
                .expect("second bounded drain");
            assert_eq!(protocol.borrow(py).received.len(), 2);
            assert_eq!(
                protocol.borrow(py).received[1],
                vec![
                    u8::try_from(MAX_READ_EVENTS_PER_DRAIN).expect("small event index"),
                    u8::try_from(MAX_READ_EVENTS_PER_DRAIN + 1).expect("small event index"),
                ]
            );
            assert!(!core.read_events_scheduled.load(Ordering::Acquire));

            shutdown_test_core(core, writer_rx, loop_core);
        });
    }

    #[test]
    fn byte_budget_takes_precedence_over_the_coalescing_limit() {
        crate::initialize_python_for_tests();
        Python::attach(|py| {
            let (core, writer_rx, loop_core, protocol) = build_test_core(py);
            core.read_events_scheduled.store(true, Ordering::Release);
            let half = MAX_READ_BYTES_PER_DRAIN / 2;
            core.enqueue_pending_read_event(PendingReadEvent::Data(vec![1; half]));
            core.enqueue_pending_read_event(PendingReadEvent::Data(vec![2; half]));
            core.enqueue_pending_read_event(PendingReadEvent::Data(vec![3]));

            core.drain_pending_read_events_with_py(py)
                .expect("drain coalesced reads");

            assert_eq!(protocol.borrow(py).received.len(), 1);
            assert_eq!(
                protocol.borrow(py).received[0].len(),
                MAX_READ_BYTES_PER_DRAIN
            );

            core.drain_pending_read_events_with_py(py)
                .expect("drain event deferred by the byte budget");
            assert_eq!(protocol.borrow(py).received.len(), 2);
            assert_eq!(protocol.borrow(py).received[1], [3]);

            shutdown_test_core(core, writer_rx, loop_core);
        });
    }

    #[test]
    fn data_callback_failure_is_reported_once_and_closes_transport() {
        crate::initialize_python_for_tests();
        Python::attach(|py| {
            let (core, writer_rx, loop_core, protocol) = build_test_core(py);
            protocol.borrow_mut(py).fail_next_data = true;
            let exception_handler = install_exception_handler(py, &loop_core);
            core.read_events_scheduled.store(true, Ordering::Release);
            core.enqueue_pending_read_event(PendingReadEvent::Data(b"boom".to_vec()));

            core.drain_pending_read_events_with_py(py)
                .expect("callback errors are contained by the drain");

            assert_eq!(
                exception_handler.borrow(py).messages,
                ["stream data_received callback failed"]
            );
            assert_eq!(
                exception_handler.borrow(py).exception_types,
                ["RuntimeError"]
            );
            assert_eq!(protocol.borrow(py).events, ["lost"]);
            assert!(core.is_closing());
            assert_eq!(core.pending_read_bytes.load(Ordering::Acquire), 0);
            assert!(!core.read_events_scheduled.load(Ordering::Acquire));

            shutdown_test_core(core, writer_rx, loop_core);
        });
    }

    #[test]
    fn detached_transport_ignores_late_reader_events() {
        crate::initialize_python_for_tests();
        Python::attach(|py| {
            let (core, writer_rx, loop_core, _protocol) = build_test_core(py);
            core.detached.store(true, Ordering::Release);

            core.enqueue_pending_read_event(PendingReadEvent::Data(b"late".to_vec()));

            assert_eq!(core.pending_read_bytes.load(Ordering::Acquire), 0);
            assert!(
                core.pending_read_events
                    .lock()
                    .expect("pending read events")
                    .is_empty()
            );
            shutdown_test_core(core, writer_rx, loop_core);
        });
    }
}
