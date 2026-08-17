//! Subprocess spawning: `Popen`-style keyword translation and the shared
//! `subprocess_shell` / `subprocess_exec` pipeline.
//!
//! `asyncio` forwards arbitrary `subprocess.Popen` keywords through to the loop,
//! so the keyword handling below mirrors `Popen`'s semantics (including
//! `umask=-1` meaning "leave unchanged" and `preexec_fn` being rejected rather
//! than emulated). The Unix-only keywords are collected into
//! [`UnixPreExecConfig`] and applied between fork and exec by [`super::pre_exec`].

#[cfg(unix)]
use std::os::unix::process::CommandExt;
#[cfg(windows)]
use std::os::windows::process::CommandExt;
use std::process::Command;

use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use super::PyLoop;
use super::pre_exec;
use super::process_stdio::{ProcessStdioSpecs, apply_stdio};
use super::spawn_env::{
    LoopSpawnEnv, is_asyncio_subprocess_stream_protocol, transport_protocol_pair,
};
use crate::transport::process::{
    ProcessTextConfig, ProcessTransportParams, spawn_process_transport,
};

const PROCESS_UMASK_MAX: i64 = 0o777;

#[derive(Clone)]
pub(super) struct UnixPreExecConfig {
    pub(super) restore_signals: bool,
    pub(super) start_new_session: bool,
    pub(super) process_group: Option<i32>,
    pub(super) pass_fds: Vec<i32>,
    pub(super) gid: Option<u32>,
    pub(super) extra_groups: Option<Vec<u32>>,
    pub(super) uid: Option<u32>,
    pub(super) umask: Option<u32>,
}

#[derive(Clone)]
struct ProcessSpawnConfig {
    text: Option<ProcessTextConfig>,
    unix: UnixPreExecConfig,
}

impl Default for UnixPreExecConfig {
    fn default() -> Self {
        Self {
            restore_signals: true,
            start_new_session: false,
            process_group: None,
            pass_fds: Vec::new(),
            gid: None,
            extra_groups: None,
            uid: None,
            umask: None,
        }
    }
}

/// Everything `subprocess_shell` and `subprocess_exec` resolve on the calling
/// thread before the spawn future starts.
pub(super) struct SubprocessParams {
    pub(super) protocol_factory: Py<PyAny>,
    pub(super) specs: ProcessStdioSpecs,
    pub(super) text_config: Option<ProcessTextConfig>,
    pub(super) kwargs: Option<Py<PyDict>>,
    /// `asyncio` helper this call came from, used for the text-mode message.
    pub(super) api_name: &'static str,
}

/// Shared tail of both subprocess methods: build the protocol, spawn the child,
/// and wrap it in a transport. Only the `Command` construction differs, so it
/// arrives as a closure that runs under the GIL on the spawning thread.
pub(super) fn spawn_subprocess<'py, F>(
    slf: &Py<PyLoop>,
    py: Python<'py>,
    params: SubprocessParams,
    build_command: F,
) -> PyResult<Bound<'py, PyAny>>
where
    F: FnOnce(Python<'_>) -> PyResult<Command> + Send + 'static,
{
    let locals = PyLoop::task_locals(py, slf)?;
    let env = LoopSpawnEnv::capture(py, slf)?;
    let SubprocessParams {
        protocol_factory,
        specs,
        text_config,
        kwargs,
        api_name,
    } = params;

    pyo3_async_runtimes::async_std::future_into_py_with_locals(py, locals, async move {
        let protocol = Python::attach(|py| env.call_protocol_factory(py, &protocol_factory))?;
        if text_config.is_some()
            && Python::attach(|py| is_asyncio_subprocess_stream_protocol(py, &protocol))?
        {
            return Err(PyValueError::new_err(format!(
                "text mode is not supported with asyncio.{api_name}() in rust-impl yet"
            )));
        }

        let (child, spawn_config, stdout_override, stderr_override) =
            Python::attach(|py| -> PyResult<_> {
                let mut command = build_command(py)?;
                let (stdout_override, stderr_override) = apply_stdio(&mut command, specs)?;
                let spawn_config = apply_common_process_kwargs(
                    py,
                    &mut command,
                    kwargs.as_ref().map(|kwargs| kwargs.bind(py)),
                )?;
                let child = command
                    .spawn()
                    .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
                Ok((child, spawn_config, stdout_override, stderr_override))
            })?;

        let transport = Python::attach(|py| {
            spawn_process_transport(
                py,
                ProcessTransportParams::new(env.spawn_context(py, &protocol), child)
                    .with_text_config(text_config.or(spawn_config.text))
                    .with_stdio_overrides(stdout_override, stderr_override),
            )
        })?;

        Python::attach(|py| transport_protocol_pair(py, transport.into_any(), &protocol))
    })
}

/// `/bin/sh -c <cmd>` on Unix, `%COMSPEC% /c "<cmd>"` on Windows.
pub(super) fn shell_command(py: Python<'_>, cmd: &Py<PyAny>) -> PyResult<Command> {
    let shell_cmd = cmd.bind(py).extract::<String>()?;
    #[cfg(unix)]
    {
        let mut command = Command::new("/bin/sh");
        command.arg("-c");
        command.arg(&shell_cmd);
        Ok(command)
    }
    #[cfg(windows)]
    {
        let mut command =
            Command::new(std::env::var_os("COMSPEC").unwrap_or_else(|| "cmd.exe".into()));
        command.raw_arg(format!(" /c \"{shell_cmd}\""));
        Ok(command)
    }
}

pub(super) fn exec_command(
    py: Python<'_>,
    program: &Py<PyAny>,
    argv: &Py<PyTuple>,
) -> PyResult<Command> {
    let mut command = Command::new(program.bind(py).extract::<String>()?);
    for arg in argv.bind(py).iter() {
        command.arg(arg.extract::<String>()?);
    }
    Ok(command)
}

fn resolve_numeric_id(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    module_name: &str,
    lookup: &str,
    field: &str,
    label: &str,
) -> PyResult<u32> {
    if let Ok(id) = value.extract::<u32>() {
        return Ok(id);
    }
    if let Ok(name) = value.extract::<String>() {
        let module = py.import(module_name)?;
        let entry = module.getattr(lookup)?.call1((name,))?;
        return entry.getattr(field)?.extract::<u32>();
    }
    Err(PyTypeError::new_err(format!(
        "{label} must be an int or str"
    )))
}

fn resolve_extra_groups(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Vec<u32>> {
    let mut groups = Vec::new();
    for item in value.try_iter()? {
        let item = item?;
        groups.push(resolve_numeric_id(
            py,
            &item,
            "grp",
            "getgrnam",
            "gr_gid",
            "extra_groups entries",
        )?);
    }
    Ok(groups)
}

pub(super) fn parse_process_text_config(
    py: Python<'_>,
    universal_newlines: bool,
    encoding: Option<Py<PyAny>>,
    errors: Option<Py<PyAny>>,
    text: Option<bool>,
) -> PyResult<Option<ProcessTextConfig>> {
    if text == Some(false) && universal_newlines {
        return Err(PyValueError::new_err(
            "text and universal_newlines have different values",
        ));
    }
    let text_enabled =
        universal_newlines || text == Some(true) || encoding.is_some() || errors.is_some();
    if !text_enabled {
        return Ok(None);
    }
    let encoding = if let Some(encoding) = encoding {
        encoding.bind(py).extract::<String>()?
    } else {
        py.import("locale")?
            .getattr("getpreferredencoding")?
            .call1((false,))?
            .extract::<String>()?
    };
    let errors = if let Some(errors) = errors {
        errors.bind(py).extract::<String>()?
    } else {
        "strict".to_owned()
    };
    Ok(Some(ProcessTextConfig {
        encoding,
        errors,
        translate_newlines: true,
    }))
}

fn apply_process_basic_kw(
    py: Python<'_>,
    command: &mut Command,
    key: &str,
    value: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    match key {
        "cwd" => apply_process_cwd(py, command, value).map(|()| true),
        "env" => apply_process_env(command, value).map(|()| true),
        "executable" => apply_process_executable(py, command, value).map(|()| true),
        _ => Ok(false),
    }
}

fn process_fspath(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<String> {
    py.import("os")?
        .getattr("fspath")?
        .call1((value.clone(),))?
        .extract::<String>()
}

fn apply_process_cwd(
    py: Python<'_>,
    command: &mut Command,
    value: &Bound<'_, PyAny>,
) -> PyResult<()> {
    if !value.is_none() {
        command.current_dir(process_fspath(py, value)?);
    }
    Ok(())
}

fn apply_process_env(command: &mut Command, value: &Bound<'_, PyAny>) -> PyResult<()> {
    if !value.is_none() {
        for (env_key, env_value) in value.cast::<PyDict>()?.iter() {
            command.env(env_key.extract::<String>()?, env_value.extract::<String>()?);
        }
    }
    Ok(())
}

fn apply_process_executable(
    py: Python<'_>,
    command: &mut Command,
    value: &Bound<'_, PyAny>,
) -> PyResult<()> {
    if !value.is_none() {
        let executable = process_fspath(py, value)?;
        #[cfg(unix)]
        command.arg0(executable);
        #[cfg(windows)]
        {
            let _ = command;
            drop(executable);
            return Err(PyNotImplementedError::new_err(
                "subprocess executable override is not implemented on Windows",
            ));
        }
    }
    Ok(())
}

struct UnixProcessKw<'a, 'py> {
    unix: &'a mut UnixPreExecConfig,
    key: &'a str,
    value: &'a Bound<'py, PyAny>,
}

fn apply_unix_process_kw(
    py: Python<'_>,
    unix: &mut UnixPreExecConfig,
    key: &str,
    value: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    let mut kw = UnixProcessKw { unix, key, value };
    if apply_unix_bool_process_kw(&mut kw)? {
        return Ok(true);
    }
    if kw.value.is_none() {
        return Ok(is_known_unix_process_kw(key));
    }
    if apply_unix_fd_process_kw(&mut kw)? {
        return Ok(true);
    }
    if apply_unix_identity_process_kw(py, &mut kw)? {
        return Ok(true);
    }
    apply_unix_misc_process_kw(&mut kw)
}

fn is_known_unix_process_kw(key: &str) -> bool {
    matches!(
        key,
        "process_group" | "pass_fds" | "group" | "extra_groups" | "user" | "umask" | "preexec_fn"
    )
}

fn apply_unix_fd_process_kw(kw: &mut UnixProcessKw<'_, '_>) -> PyResult<bool> {
    match kw.key {
        "process_group" => kw.unix.process_group = Some(kw.value.extract::<i32>()?),
        "pass_fds" => {
            kw.unix.pass_fds = kw
                .value
                .try_iter()?
                .map(|item| item.and_then(|value| value.extract::<i32>()))
                .collect::<PyResult<Vec<_>>>()?;
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn apply_unix_identity_process_kw(
    py: Python<'_>,
    kw: &mut UnixProcessKw<'_, '_>,
) -> PyResult<bool> {
    match kw.key {
        "group" => {
            kw.unix.gid = Some(resolve_numeric_id(
                py, kw.value, "grp", "getgrnam", "gr_gid", "group",
            )?);
        }
        "extra_groups" => {
            kw.unix.extra_groups = Some(resolve_extra_groups(py, kw.value)?);
        }
        "user" => {
            kw.unix.uid = Some(resolve_numeric_id(
                py, kw.value, "pwd", "getpwnam", "pw_uid", "user",
            )?);
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn apply_unix_misc_process_kw(kw: &mut UnixProcessKw<'_, '_>) -> PyResult<bool> {
    match kw.key {
        "umask" => {
            let mask = kw.value.extract::<i64>()?;
            // Popen umask=-1 is default for "no change".
            if mask != -1 {
                if !(0..=PROCESS_UMASK_MAX).contains(&mask) {
                    return Err(PyValueError::new_err("umask must be between 0 and 0o777"));
                }
                kw.unix.umask =
                    Some(u32::try_from(mask).expect("validated umask is nonnegative and fits u32"));
            }
        }
        "preexec_fn" => {
            return Err(PyNotImplementedError::new_err(
                "preexec_fn remains unsupported in rust-impl because it is unsafe in this runtime model",
            ));
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn apply_unix_bool_process_kw(kw: &mut UnixProcessKw<'_, '_>) -> PyResult<bool> {
    match kw.key {
        "restore_signals" => kw.unix.restore_signals = kw.value.is_truthy()?,
        "start_new_session" => kw.unix.start_new_session = kw.value.is_truthy()?,
        _ => return Ok(false),
    }
    Ok(true)
}

fn apply_common_process_kwargs(
    py: Python<'_>,
    command: &mut Command,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<ProcessSpawnConfig> {
    let mut spawn_config = ProcessSpawnConfig {
        text: None,
        unix: UnixPreExecConfig::default(),
    };
    let Some(kwargs) = kwargs else {
        return Ok(spawn_config);
    };

    for (key, value) in kwargs.iter() {
        let key = key.extract::<String>()?;
        if apply_process_basic_kw(py, command, &key, &value)? {
            continue;
        }
        apply_unix_process_kw(py, &mut spawn_config.unix, &key, &value)?;
    }

    pre_exec::apply(command, spawn_config.unix.clone());
    Ok(spawn_config)
}
