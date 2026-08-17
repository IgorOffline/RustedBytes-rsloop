//! Cached lookups into the `asyncio` module.
//!
//! `Task`, `Future`, and `_get_running_loop` are resolved once per process and
//! reused, because the loop touches them on the hottest paths (`create_task` runs
//! for every coroutine step). The keyword-name tuples exist for the same reason:
//! `PyObject_Vectorcall` needs a `kwnames` tuple, and rebuilding it per call
//! would allocate on every task creation.

use std::sync::OnceLock;

use pyo3::prelude::*;
use pyo3::types::PyModule;

#[cfg(any(Py_3_12, all(Py_3_11, not(Py_LIMITED_API))))]
use pyo3::exceptions::PyRuntimeError;
#[cfg(any(Py_3_12, all(Py_3_11, not(Py_LIMITED_API))))]
use pyo3::types::PyTuple;

use super::ffi_helpers;
#[cfg(any(Py_3_12, all(Py_3_11, not(Py_LIMITED_API))))]
use crate::python_names;

struct PythonApiCaches {
    asyncio_task_cls: OnceLock<Py<PyAny>>,
    asyncio_future_cls: OnceLock<Py<PyAny>>,
    asyncio_get_running_loop_fn: OnceLock<Py<PyAny>>,
    asyncio_task_kwarg_support: OnceLock<TaskKwargSupport>,
    #[cfg(any(Py_3_12, all(Py_3_11, not(Py_LIMITED_API))))]
    asyncio_future_loop_kwnames: OnceLock<Py<PyTuple>>,
    #[cfg(any(Py_3_12, all(Py_3_11, not(Py_LIMITED_API))))]
    asyncio_task_loop_kwnames: OnceLock<Py<PyTuple>>,
    #[cfg(any(Py_3_12, all(Py_3_11, not(Py_LIMITED_API))))]
    asyncio_task_loop_name_kwnames: OnceLock<Py<PyTuple>>,
    #[cfg(any(Py_3_12, all(Py_3_11, not(Py_LIMITED_API))))]
    asyncio_task_loop_context_kwnames: OnceLock<Py<PyTuple>>,
    #[cfg(any(Py_3_12, all(Py_3_11, not(Py_LIMITED_API))))]
    asyncio_task_loop_name_context_kwnames: OnceLock<Py<PyTuple>>,
}

impl PythonApiCaches {
    const fn new() -> Self {
        Self {
            asyncio_task_cls: OnceLock::new(),
            asyncio_future_cls: OnceLock::new(),
            asyncio_get_running_loop_fn: OnceLock::new(),
            asyncio_task_kwarg_support: OnceLock::new(),
            #[cfg(any(Py_3_12, all(Py_3_11, not(Py_LIMITED_API))))]
            asyncio_future_loop_kwnames: OnceLock::new(),
            #[cfg(any(Py_3_12, all(Py_3_11, not(Py_LIMITED_API))))]
            asyncio_task_loop_kwnames: OnceLock::new(),
            #[cfg(any(Py_3_12, all(Py_3_11, not(Py_LIMITED_API))))]
            asyncio_task_loop_name_kwnames: OnceLock::new(),
            #[cfg(any(Py_3_12, all(Py_3_11, not(Py_LIMITED_API))))]
            asyncio_task_loop_context_kwnames: OnceLock::new(),
            #[cfg(any(Py_3_12, all(Py_3_11, not(Py_LIMITED_API))))]
            asyncio_task_loop_name_context_kwnames: OnceLock::new(),
        }
    }
}

static PYTHON_API_CACHES: PythonApiCaches = PythonApiCaches::new();

/// Which keyword-only parameters this interpreter's `asyncio.Task` accepts.
pub(super) struct TaskKwargSupport {
    pub(super) name: bool,
    pub(super) context: bool,
    pub(super) eager_start: bool,
}

pub(super) fn asyncio_task_cls(py: Python<'_>) -> PyResult<&Py<PyAny>> {
    if let Some(cached) = PYTHON_API_CACHES.asyncio_task_cls.get() {
        return Ok(cached);
    }

    let loaded = py.import("asyncio")?.getattr("Task")?.unbind();
    Ok(PYTHON_API_CACHES.asyncio_task_cls.get_or_init(|| loaded))
}

pub(super) fn asyncio_future_cls(py: Python<'_>) -> PyResult<&Py<PyAny>> {
    if let Some(cached) = PYTHON_API_CACHES.asyncio_future_cls.get() {
        return Ok(cached);
    }

    let loaded = py.import("asyncio")?.getattr("Future")?.unbind();
    Ok(PYTHON_API_CACHES.asyncio_future_cls.get_or_init(|| loaded))
}

pub(super) fn asyncio_get_running_loop_fn(py: Python<'_>) -> PyResult<&Py<PyAny>> {
    if let Some(cached) = PYTHON_API_CACHES.asyncio_get_running_loop_fn.get() {
        return Ok(cached);
    }

    let loaded = py
        .import("asyncio.events")?
        .getattr("_get_running_loop")?
        .unbind();
    Ok(PYTHON_API_CACHES
        .asyncio_get_running_loop_fn
        .get_or_init(|| loaded))
}

pub(super) fn asyncio_task_kwarg_support(py: Python<'_>) -> PyResult<&'static TaskKwargSupport> {
    if let Some(cached) = PYTHON_API_CACHES.asyncio_task_kwarg_support.get() {
        return Ok(cached);
    }

    let support = detect_asyncio_task_kwarg_support(py)?;
    Ok(PYTHON_API_CACHES
        .asyncio_task_kwarg_support
        .get_or_init(|| support))
}

fn detect_asyncio_task_kwarg_support(py: Python<'_>) -> PyResult<TaskKwargSupport> {
    let inspect = py.import("inspect")?;
    let Some(signature) = asyncio_task_signature(py, &inspect)? else {
        return Ok(TaskKwargSupport {
            name: true,
            context: false,
            eager_start: false,
        });
    };
    let parameters = signature.getattr("parameters")?;
    let keyword_only = inspect.getattr("Parameter")?.getattr("KEYWORD_ONLY")?;
    let mut support = TaskKwargSupport {
        name: false,
        context: false,
        eager_start: false,
    };

    for kwarg_name in ["name", "context", "eager_start"] {
        if has_keyword_only_parameter(&parameters, &keyword_only, kwarg_name)? {
            mark_task_kwarg_supported(&mut support, kwarg_name);
        }
    }

    Ok(support)
}

fn asyncio_task_signature<'py>(
    py: Python<'py>,
    inspect: &Bound<'py, PyModule>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    match inspect
        .getattr("signature")?
        .call1((asyncio_task_cls(py)?.clone_ref(py),))
    {
        Ok(signature) => Ok(Some(signature)),
        Err(_) => Ok(None),
    }
}

fn has_keyword_only_parameter(
    parameters: &Bound<'_, PyAny>,
    keyword_only: &Bound<'_, PyAny>,
    kwarg_name: &str,
) -> PyResult<bool> {
    let Ok(parameter) = parameters.get_item(kwarg_name) else {
        return Ok(false);
    };
    parameter.getattr("kind")?.eq(keyword_only)
}

fn mark_task_kwarg_supported(support: &mut TaskKwargSupport, kwarg_name: &str) {
    match kwarg_name {
        "name" => support.name = true,
        "context" => support.context = true,
        "eager_start" => support.eager_start = true,
        _ => {}
    }
}

#[inline]
pub(super) fn call_callable_noargs(py: Python<'_>, callable: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    ffi_helpers::call_noargs(py, callable)
}

#[inline]
pub(super) fn call_callable_onearg(
    py: Python<'_>,
    callable: &Py<PyAny>,
    arg: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    ffi_helpers::call_onearg(py, callable, arg)
}

#[cfg(any(Py_3_12, all(Py_3_11, not(Py_LIMITED_API))))]
fn keyword_tuple<const N: usize>(
    slot: &'static OnceLock<Py<PyTuple>>,
    py: Python<'_>,
    names: [&Bound<'_, pyo3::types::PyString>; N],
) -> PyResult<&'static Py<PyTuple>> {
    if let Some(tuple) = slot.get() {
        return Ok(tuple);
    }

    let tuple = PyTuple::new(py, names)?.unbind();
    match slot.set(tuple) {
        Ok(()) => {}
        Err(_already_initialized) => {}
    }
    slot.get()
        .ok_or_else(|| PyRuntimeError::new_err("failed to initialize keyword tuple cache"))
}

#[cfg(any(Py_3_12, all(Py_3_11, not(Py_LIMITED_API))))]
pub(super) fn asyncio_future_loop_kwnames(py: Python<'_>) -> PyResult<&Py<PyTuple>> {
    keyword_tuple(
        &PYTHON_API_CACHES.asyncio_future_loop_kwnames,
        py,
        [python_names::loop_kw(py)],
    )
}

#[cfg(any(Py_3_12, all(Py_3_11, not(Py_LIMITED_API))))]
pub(super) fn asyncio_task_kwnames_for_options(
    py: Python<'_>,
    include_name: bool,
    include_context: bool,
) -> PyResult<&Py<PyTuple>> {
    match (include_name, include_context) {
        (false, false) => keyword_tuple(
            &PYTHON_API_CACHES.asyncio_task_loop_kwnames,
            py,
            [python_names::loop_kw(py)],
        ),
        (true, false) => keyword_tuple(
            &PYTHON_API_CACHES.asyncio_task_loop_name_kwnames,
            py,
            [python_names::loop_kw(py), python_names::name_kw(py)],
        ),
        (false, true) => keyword_tuple(
            &PYTHON_API_CACHES.asyncio_task_loop_context_kwnames,
            py,
            [python_names::loop_kw(py), python_names::context_kw(py)],
        ),
        (true, true) => keyword_tuple(
            &PYTHON_API_CACHES.asyncio_task_loop_name_context_kwnames,
            py,
            [
                python_names::loop_kw(py),
                python_names::name_kw(py),
                python_names::context_kw(py),
            ],
        ),
    }
}
