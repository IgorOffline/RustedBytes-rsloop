//! Native extension entry point and the top-level map of the Rust implementation.
//!
//! `bindings` exposes the Python API, `engine` schedules work, and `transport`
//! owns network, TLS, and subprocess I/O. Platform details stay behind
//! `platform` so the higher layers can share one event-loop model.

mod async_event;
mod bindings;
mod blocking;
mod build_metadata;
mod context;
mod engine;
mod errors;
mod module_init;
mod platform;
mod profiler;
mod python_names;
pub mod rust_async;
mod transport;

pub(crate) use platform::fd as fd_ops;
#[cfg(windows)]
pub(crate) use platform::windows_vibeio;

// Compatibility re-exports for the crate's existing Rust API. Internal module
// registration imports from the owning modules directly, so these can be
// deprecated or versioned independently in a future breaking release.
pub use bindings::{
    PyLoop, asyncgen_finalizer_hook, asyncgen_firstiter_hook, future_done_stop, new_event_loop,
    signal_bridge,
};
pub use engine::{
    LoopCommand, LoopCore, LoopFutureCommand, LoopIoCommand, LoopRunCommand, LoopSignalCommand,
    LoopTransportCommand, PyHandle, PyTimerHandle, ReadyCallback,
};
pub use profiler::{profiler_compiled, profiler_running, start_profiler, stop_profiler};
pub use transport::process::{PyProcessPipeTransport, PyProcessTransport};
pub use transport::stream::{
    PyFastStreamReader, PyFastStreamWriter, open_connection, start_server,
};
pub use transport::stream::{PyServer, PyStreamTransport};

use pyo3::prelude::*;

#[cfg(test)]
pub(crate) fn initialize_python_for_tests() {
    static INITIALIZE: std::sync::Once = std::sync::Once::new();
    INITIALIZE.call_once(Python::initialize);
}

#[pymodule(gil_used = false)]
fn _loop(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module_init::add_module_contents(m)
}
