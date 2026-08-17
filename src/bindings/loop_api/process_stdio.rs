//! Translation of `asyncio`'s `stdin`/`stdout`/`stderr` arguments into `Stdio`.
//!
//! Callers pass either one of `asyncio.subprocess`'s marker constants
//! (`PIPE`/`DEVNULL`/`STDOUT`) or something with a `fileno()`. `STDOUT` is only
//! legal for `stderr`, and the `stdout=PIPE, stderr=STDOUT` combination cannot be
//! expressed with `std::process::Stdio` alone — it needs one pipe handed to both
//! child descriptors, which is why that case builds the pipe itself.

use std::process::Command;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;

use super::process_handles;
use crate::fd_ops;
use crate::transport::process::BoxedProcessReader;

#[derive(Clone, Copy)]
pub(super) enum ProcessStdioSpec {
    Inherit,
    Pipe,
    DevNull,
    Fd(fd_ops::RawFd),
    Stdout,
}

/// The three resolved child descriptors for one spawn.
#[derive(Clone, Copy)]
pub(super) struct ProcessStdioSpecs {
    pub(super) stdin: ProcessStdioSpec,
    pub(super) stdout: ProcessStdioSpec,
    pub(super) stderr: ProcessStdioSpec,
}

impl ProcessStdioSpecs {
    pub(super) fn parse(
        py: Python<'_>,
        stdin: &Py<PyAny>,
        stdout: &Py<PyAny>,
        stderr: &Py<PyAny>,
    ) -> PyResult<Self> {
        Ok(Self {
            stdin: parse_process_stdio(py, stdin, false)?,
            stdout: parse_process_stdio(py, stdout, false)?,
            stderr: parse_process_stdio(py, stderr, true)?,
        })
    }
}

static PIPE_CELL: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// Default for an omitted `stdin`/`stdout`/`stderr`: `subprocess.PIPE` (== -1),
/// matching `CPython`'s loop methods. An explicit `None` arrives as `Option::None`
/// instead and is honored as "inherit the parent's fd" by `parse_process_stdio`.
pub(super) fn default_stdio_pipe() -> Py<PyAny> {
    Python::attach(|py| {
        PIPE_CELL
            .get_or_try_init(py, || {
                py.import("asyncio.subprocess")?
                    .getattr("PIPE")
                    .map(Bound::unbind)
            })
            .expect("asyncio.subprocess is always importable")
            .clone_ref(py)
    })
}

fn parse_process_stdio(
    py: Python<'_>,
    value: &Py<PyAny>,
    allow_stdout_redirect: bool,
) -> PyResult<ProcessStdioSpec> {
    let bound = value.bind(py);
    if bound.is_none() {
        return Ok(ProcessStdioSpec::Inherit);
    }
    parse_subprocess_stdio_marker(py, bound, allow_stdout_redirect)?.map_or_else(
        || Ok(ProcessStdioSpec::Fd(fd_ops::fileobj_to_fd(py, bound)?)),
        Ok,
    )
}

fn parse_subprocess_stdio_marker(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    allow_stdout_redirect: bool,
) -> PyResult<Option<ProcessStdioSpec>> {
    let subprocess = py.import("asyncio.subprocess")?;
    if value.eq(default_stdio_pipe())? {
        return Ok(Some(ProcessStdioSpec::Pipe));
    }
    if value.eq(&subprocess.getattr("DEVNULL")?)? {
        return Ok(Some(ProcessStdioSpec::DevNull));
    }
    let is_stdout = value.eq(&subprocess.getattr("STDOUT")?)?;
    match (allow_stdout_redirect, is_stdout) {
        (true, true) => Ok(Some(ProcessStdioSpec::Stdout)),
        (false, true) => Err(PyValueError::new_err("STDOUT can only be used for stderr")),
        (_, false) => Ok(None),
    }
}

fn stdio_from_fd(fd: fd_ops::RawFd) -> PyResult<std::process::Stdio> {
    process_handles::file_from_fd(fd).map(std::process::Stdio::from)
}

pub(super) fn apply_stdio(
    command: &mut Command,
    specs: ProcessStdioSpecs,
) -> PyResult<(Option<BoxedProcessReader>, Option<BoxedProcessReader>)> {
    use std::process::Stdio;

    let ProcessStdioSpecs {
        stdin,
        stdout,
        stderr,
    } = specs;
    command.stdin(stdin_stdio(stdin)?);

    let mut stdout_override = None;
    let stderr_override = None;

    match (stdout, stderr) {
        (ProcessStdioSpec::Pipe, ProcessStdioSpec::Stdout) => {
            let (read_end, write_end) = process_handles::new_pipe()?;
            let stderr_end = write_end
                .try_clone()
                .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
            command.stdout(Stdio::from(write_end));
            command.stderr(Stdio::from(stderr_end));
            stdout_override = Some(Box::new(read_end) as BoxedProcessReader);
        }
        (stdout, stderr) => {
            command.stdout(output_stdio(stdout)?);
            command.stderr(stderr_stdio(stderr, stdout)?);
        }
    }

    Ok((stdout_override, stderr_override))
}

fn stdin_stdio(spec: ProcessStdioSpec) -> PyResult<std::process::Stdio> {
    match spec {
        ProcessStdioSpec::Stdout => {
            Err(PyValueError::new_err("STDOUT can only be used for stderr"))
        }
        spec => output_stdio(spec),
    }
}

fn output_stdio(spec: ProcessStdioSpec) -> PyResult<std::process::Stdio> {
    use std::process::Stdio;

    match spec {
        ProcessStdioSpec::Inherit => Ok(Stdio::inherit()),
        ProcessStdioSpec::Pipe => Ok(Stdio::piped()),
        ProcessStdioSpec::DevNull => Ok(Stdio::null()),
        ProcessStdioSpec::Fd(fd) => stdio_from_fd(fd),
        ProcessStdioSpec::Stdout => {
            Err(PyValueError::new_err("STDOUT can only be used for stderr"))
        }
    }
}

fn stderr_stdio(
    stderr: ProcessStdioSpec,
    stdout: ProcessStdioSpec,
) -> PyResult<std::process::Stdio> {
    if matches!(stderr, ProcessStdioSpec::Stdout) {
        return stderr_stdout_stdio(stdout);
    }
    output_stdio(stderr)
}

fn stderr_stdout_stdio(stdout: ProcessStdioSpec) -> PyResult<std::process::Stdio> {
    use std::process::Stdio;

    match stdout {
        ProcessStdioSpec::Inherit => Ok(Stdio::inherit()),
        ProcessStdioSpec::DevNull => Ok(Stdio::null()),
        ProcessStdioSpec::Fd(fd) => stdio_from_fd(fd),
        ProcessStdioSpec::Pipe | ProcessStdioSpec::Stdout => Err(PyRuntimeError::new_err(
            "invalid stderr=STDOUT configuration",
        )),
    }
}
