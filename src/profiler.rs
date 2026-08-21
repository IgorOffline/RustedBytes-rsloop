//! Optional Tracy profiler lifecycle exposed to Python.

/// Opens a named profiling scope when Tracy support is compiled in.
///
/// Default builds intentionally expand this macro to nothing, matching the
/// previous `profiling` facade without retaining it as a dependency.
macro_rules! profile_scope {
    ($name:literal) => {
        #[cfg(feature = "profiler")]
        let _rsloop_tracy_span = tracy_client::span!($name, 0);
    };
}

/// Opens a scope named after the enclosing function in profiler builds.
macro_rules! profile_function {
    () => {
        #[cfg(feature = "profiler")]
        let _rsloop_tracy_span = tracy_client::span!();
    };
}

pub(crate) use profile_function;
pub(crate) use profile_scope;

use pyo3::prelude::*;

#[cfg(feature = "profiler")]
mod imp {
    use std::cell::RefCell;
    use std::sync::{Mutex, OnceLock};

    use pyo3::exceptions::PyRuntimeError;
    use pyo3::prelude::*;
    use tracy_client::{Client, Span};

    struct ActiveProfiler {
        client: Client,
    }

    thread_local! {
        static SESSION_SPAN: RefCell<Option<Span>> = const { RefCell::new(None) };
    }

    static ACTIVE_PROFILER: OnceLock<Mutex<Option<ActiveProfiler>>> = OnceLock::new();

    fn active_profiler() -> &'static Mutex<Option<ActiveProfiler>> {
        ACTIVE_PROFILER.get_or_init(|| Mutex::new(None))
    }

    #[pyfunction]
    /// Starts a process-wide Tracy profiling session.
    ///
    /// Returns an error if a session is already active.
    pub fn start_profiler() -> PyResult<()> {
        let mut active = active_profiler()
            .lock()
            .map_err(|_| PyRuntimeError::new_err("profiler state mutex is poisoned"))?;
        if active.is_some() {
            return Err(PyRuntimeError::new_err("profiler is already running"));
        }

        let client = Client::start();
        client.set_thread_name("python-main");
        let session_span = client.clone().span_alloc(
            Some("rsloop.profile_session"),
            "start_profiler",
            file!(),
            line!(),
            0,
        );
        SESSION_SPAN.with(|slot| {
            *slot.borrow_mut() = Some(session_span);
        });
        *active = Some(ActiveProfiler { client });
        Ok(())
    }

    #[pyfunction]
    /// Reports whether a Tracy profiling session is currently active.
    pub fn profiler_running() -> bool {
        active_profiler()
            .lock()
            .map(|active| active.is_some())
            .unwrap_or(false)
    }

    #[pyfunction]
    /// Stops the active Tracy profiling session.
    ///
    /// Returns an error if no session is active.
    pub fn stop_profiler() -> PyResult<()> {
        let active = active_profiler()
            .lock()
            .map_err(|_| PyRuntimeError::new_err("profiler state mutex is poisoned"))?
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("profiler is not running"))?;
        SESSION_SPAN.with(|slot| {
            slot.borrow_mut().take();
        });
        let ActiveProfiler { client: _client } = active;
        Ok(())
    }
}

#[cfg(not(feature = "profiler"))]
mod imp {
    use pyo3::exceptions::PyRuntimeError;
    use pyo3::prelude::*;

    const PROFILER_DISABLED_MESSAGE: &str =
        "profiler support is disabled; rebuild with `--features profiler`";

    #[pyfunction]
    /// Always returns `false` when profiler support was not compiled in.
    pub fn profiler_running() -> bool {
        false
    }

    #[pyfunction]
    /// Returns an error explaining that profiler support is disabled.
    pub fn start_profiler() -> PyResult<()> {
        Err(PyRuntimeError::new_err(PROFILER_DISABLED_MESSAGE))
    }

    #[pyfunction]
    /// Returns an error explaining that profiler support is disabled.
    pub fn stop_profiler() -> PyResult<()> {
        Err(PyRuntimeError::new_err(PROFILER_DISABLED_MESSAGE))
    }
}

pub use imp::{profiler_running, start_profiler, stop_profiler};

#[pyfunction]
/// Reports whether this build includes optional Tracy profiler support.
///
/// Unlike [`profiler_running`], this describes a compile-time capability and
/// therefore never changes while the process is running.
pub const fn profiler_compiled() -> bool {
    cfg!(feature = "profiler")
}
