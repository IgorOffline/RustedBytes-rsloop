//! Building a subprocess transport around an already-spawned `Child`.
//!
//! The child arrives from `loop_api::process_spawn` with its pipes still
//! attached; `ProcessPipes` takes them off it and the open-descriptor set that
//! comes out of that seeds the bookkeeping `connection_lost` depends on.
//!
//! stdin becomes a real write-pipe stream transport, so subprocess writes reuse
//! the stream writer rather than a second implementation. Its descriptor is
//! duplicated into a Python file object first, which is what lets the transport
//! own its copy while the caller's `Child` keeps its own.
//!
//! Pipe transports are registered and `connection_made` is delivered *before*
//! the worker threads start, so no event can reach the protocol before it has
//! been given the transport.

use std::collections::{HashMap, HashSet, VecDeque};
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;
use std::process::{Child, ChildStdin};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::params::{BoxedProcessReader, ProcessTextConfig, ProcessTransportParams};
use super::worker::{run_process_reader, run_process_waiter};
use super::{
    ProcessCommand, ProcessPipeTransportCore, ProcessState, ProcessTransportCore,
    PyProcessPipeTransport, PyProcessStdinProtocol, PyProcessTransport,
};
use crate::async_event::AsyncEvent;
use crate::fd_ops;
use crate::transport::stream::{TransportSpawnContext, spawn_write_pipe_transport};

pub(super) struct ProcessPipes {
    stdin: Option<ChildStdin>,
    stdout: Option<BoxedProcessReader>,
    stderr: Option<BoxedProcessReader>,
}

impl ProcessPipes {
    pub(super) fn take_from(
        child: &mut Child,
        stdout_override: Option<BoxedProcessReader>,
        stderr_override: Option<BoxedProcessReader>,
    ) -> Self {
        Self {
            stdin: child.stdin.take(),
            stdout: stdout_override.or_else(|| {
                child
                    .stdout
                    .take()
                    .map(|value| Box::new(value) as BoxedProcessReader)
            }),
            stderr: stderr_override.or_else(|| {
                child
                    .stderr
                    .take()
                    .map(|value| Box::new(value) as BoxedProcessReader)
            }),
        }
    }

    pub(super) fn open_pipes(&self) -> HashSet<i32> {
        let mut open_pipes = HashSet::with_capacity(3);
        if self.stdin.is_some() {
            open_pipes.insert(0);
        }
        if self.stdout.is_some() {
            open_pipes.insert(1);
        }
        if self.stderr.is_some() {
            open_pipes.insert(2);
        }
        open_pipes
    }
}

pub(super) fn new_process_pipe_transport(py: Python<'_>, fd: i32) -> PyResult<Py<PyAny>> {
    Ok(Py::new(
        py,
        PyProcessPipeTransport {
            core: Arc::new(ProcessPipeTransportCore {
                fd,
                closing: AtomicBool::new(false),
            }),
        },
    )?
    .into_any())
}

pub(super) fn process_text_extra_entries(
    py: Python<'_>,
    text_config: Option<&ProcessTextConfig>,
) -> Option<HashMap<String, Py<PyAny>>> {
    text_config.map(|text_config| {
        let mut extra = HashMap::with_capacity(2);
        extra.insert(
            "text_encoding".to_owned(),
            pyo3::types::PyString::new(py, &text_config.encoding)
                .unbind()
                .into_any(),
        );
        extra.insert(
            "text_errors".to_owned(),
            pyo3::types::PyString::new(py, &text_config.errors)
                .unbind()
                .into_any(),
        );
        extra
    })
}

pub(super) fn spawn_stdin_pipe_transport(
    py: Python<'_>,
    core: &Arc<ProcessTransportCore>,
    stdin: ChildStdin,
    extra_entries: Option<HashMap<String, Py<PyAny>>>,
) -> PyResult<Py<PyAny>> {
    #[cfg(unix)]
    let file_obj: Py<PyAny> = make_python_pipe_file(py, i64::from(stdin.as_raw_fd()), "wb")?;
    #[cfg(windows)]
    let file_obj: Py<PyAny> = make_python_pipe_file_from_handle(py, stdin.as_raw_handle(), "wb")?;
    let stdin_protocol = Py::new(py, PyProcessStdinProtocol { core: core.clone() })?.into_any();
    let stdin_context = py
        .import("contextvars")?
        .getattr("Context")?
        .call0()?
        .unbind();
    let transport = spawn_write_pipe_transport(
        py,
        TransportSpawnContext::new(
            py,
            core.loop_core.clone(),
            &core.loop_obj,
            stdin_protocol,
            &stdin_context,
            false,
        ),
        file_obj.clone_ref(py),
        extra_entries,
    )?;
    if let Err(err) = file_obj.call_method0(py, "close") {
        core.report_error(err, "subprocess stdin pipe close failed");
    }
    Ok(transport.into_any())
}

pub(super) fn register_initial_pipe_transports(
    py: Python<'_>,
    core: &Arc<ProcessTransportCore>,
    stdin: Option<ChildStdin>,
    has_stdout: bool,
    has_stderr: bool,
) -> PyResult<()> {
    let mut pipe_transport_entries = Vec::with_capacity(3);
    if let Some(stdin) = stdin {
        let extra_entries = process_text_extra_entries(py, core.text_config.as_ref());
        let transport = spawn_stdin_pipe_transport(py, core, stdin, extra_entries)?;
        pipe_transport_entries.push((0, transport));
    }
    if has_stdout {
        pipe_transport_entries.push((1, new_process_pipe_transport(py, 1)?));
    }
    if has_stderr {
        pipe_transport_entries.push((2, new_process_pipe_transport(py, 2)?));
    }
    core.register_pipe_transports(pipe_transport_entries);
    Ok(())
}

pub(super) fn spawn_process_reader_thread(
    name: &str,
    core: Arc<ProcessTransportCore>,
    fd: i32,
    reader: BoxedProcessReader,
) -> PyResult<()> {
    thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || run_process_reader(core, fd, reader))
        .map(|_| ())
        .map_err(|err| PyRuntimeError::new_err(format!("failed to spawn {name}: {err}")))
}

pub(super) fn spawn_process_waiter_thread(
    core: Arc<ProcessTransportCore>,
    child: Child,
    control_rx: Receiver<ProcessCommand>,
) -> PyResult<()> {
    thread::Builder::new()
        .name("rsloop-process-waiter".to_owned())
        .spawn(move || run_process_waiter(core, child, control_rx))
        .map(|_| ())
        .map_err(|err| PyRuntimeError::new_err(format!("failed to spawn process waiter: {err}")))
}

pub(super) fn spawn_process_workers(
    core: Arc<ProcessTransportCore>,
    stdout: Option<BoxedProcessReader>,
    stderr: Option<BoxedProcessReader>,
    child: Child,
    control_rx: Receiver<ProcessCommand>,
) -> PyResult<()> {
    if let Some(stdout) = stdout {
        spawn_process_reader_thread("rsloop-process-stdout", core.clone(), 1, stdout)?;
    }
    if let Some(stderr) = stderr {
        spawn_process_reader_thread("rsloop-process-stderr", core.clone(), 2, stderr)?;
    }
    spawn_process_waiter_thread(core, child, control_rx)
}

pub fn spawn_process_transport(
    py: Python<'_>,
    params: ProcessTransportParams,
) -> PyResult<Py<PyProcessTransport>> {
    let ProcessTransportParams {
        loop_core,
        loop_obj,
        protocol,
        context,
        context_needs_run,
        text_config,
        mut child,
        stdout_override,
        stderr_override,
    } = params;
    let pid = child.id();
    let mut pipes = ProcessPipes::take_from(&mut child, stdout_override, stderr_override);
    let (control_tx, control_rx) = mpsc::channel();

    let core = Arc::new(ProcessTransportCore {
        loop_core,
        loop_obj,
        state: Mutex::new(ProcessState {
            protocol,
            context,
            context_needs_run,
            pid,
            returncode: None,
            closing: false,
            exited: false,
            connection_lost_called: false,
            open_pipes: pipes.open_pipes(),
            pipe_transports: HashMap::with_capacity(3),
        }),
        text_config,
        control_tx,
        exit_notify: AsyncEvent::new(),
        pending_events: Mutex::new(VecDeque::new()),
        events_scheduled: AtomicBool::new(false),
    });

    let stdout_open = pipes.stdout.is_some();
    let stderr_open = pipes.stderr.is_some();
    register_initial_pipe_transports(py, &core, pipes.stdin.take(), stdout_open, stderr_open)?;

    let transport = Py::new(py, PyProcessTransport { core: core.clone() })?;
    core.connection_made(transport.clone_ref(py))?;

    spawn_process_workers(core.clone(), pipes.stdout, pipes.stderr, child, control_rx)?;

    Ok(transport)
}

#[cfg(unix)]
pub(super) fn make_python_pipe_file(
    py: Python<'_>,
    fd: fd_ops::RawFd,
    mode: &str,
) -> PyResult<Py<PyAny>> {
    let os = py.import("os")?;
    let dup = fd_ops::dup_raw_fd(fd).map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
    Ok(os.getattr("fdopen")?.call1((dup, mode, 0))?.unbind())
}

#[cfg(windows)]
pub(super) fn make_python_pipe_file_from_handle(
    py: Python<'_>,
    handle: std::os::windows::io::RawHandle,
    mode: &str,
) -> PyResult<Py<PyAny>> {
    let duplicated =
        fd_ops::duplicate_handle(handle).map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
    let msvcrt = py.import("msvcrt")?;
    let os = py.import("os")?;
    let flags = if mode.starts_with('r') {
        libc::O_RDONLY
    } else {
        libc::O_WRONLY
    } | libc::O_BINARY;
    let fd = msvcrt
        .getattr("open_osfhandle")?
        .call1((duplicated as isize, flags))?
        .extract::<i64>()?;
    Ok(os.getattr("fdopen")?.call1((fd, mode, 0))?.unbind())
}
