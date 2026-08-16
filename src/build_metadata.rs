//! Runtime-visible metadata about the native extension build.

use pyo3::prelude::*;
use pyo3::types::PyDict;

#[cfg(windows)]
const REACTOR: &str = "iocp";
#[cfg(target_os = "linux")]
const REACTOR: &str = "io_uring";
#[cfg(not(any(windows, target_os = "linux")))]
const REACTOR: &str = "mio";

/// Return stable diagnostics that help identify the installed native build.
#[pyfunction]
pub(crate) fn build_info(py: Python<'_>) -> PyResult<Py<PyDict>> {
    let info = PyDict::new(py);
    info.set_item("version", env!("CARGO_PKG_VERSION"))?;
    info.set_item(
        "profile",
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
    )?;
    info.set_item("target_os", std::env::consts::OS)?;
    info.set_item("target_arch", std::env::consts::ARCH)?;
    info.set_item("free_threaded", cfg!(Py_GIL_DISABLED))?;
    info.set_item("reactor", REACTOR)?;
    info.set_item("tls_backend", "rustls")?;
    info.set_item("profiler", cfg!(feature = "profiler"))?;
    Ok(info.unbind())
}
