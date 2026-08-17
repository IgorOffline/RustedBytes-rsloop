//! Shared handling of the `ssl` / `server_hostname` / timeout keyword group.
//!
//! Every `asyncio` loop method that accepts TLS options repeats the same three
//! "only meaningful with ssl" checks before converting the Python `SSLContext`
//! into Rust TLS settings. Collecting the group here keeps those validations —
//! and their order, which callers observe through the raised `ValueError` — in
//! one place.

use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::transport::tls::{
    ClientTlsSettings, ServerTlsSettings, client_tls_settings, server_tls_settings,
};

/// The TLS keyword group as it arrives from Python.
pub(super) struct TlsParams {
    pub(super) ssl: Option<Py<PyAny>>,
    pub(super) server_hostname: Option<Py<PyAny>>,
    pub(super) handshake_timeout: Option<f64>,
    pub(super) shutdown_timeout: Option<f64>,
}

impl TlsParams {
    /// Options for a method that has no `server_hostname` parameter.
    pub(super) fn without_hostname(
        ssl: Option<Py<PyAny>>,
        handshake_timeout: Option<f64>,
        shutdown_timeout: Option<f64>,
    ) -> Self {
        Self {
            ssl,
            server_hostname: None,
            handshake_timeout,
            shutdown_timeout,
        }
    }

    pub(super) fn is_enabled(&self) -> bool {
        self.ssl.is_some()
    }

    /// Rejects TLS-only keywords passed without `ssl`. The check order matches
    /// what the individual methods used to do, so the reported error for a call
    /// that misuses several keywords at once does not change.
    pub(super) fn validate(&self) -> PyResult<()> {
        if self.ssl.is_some() {
            return Ok(());
        }
        if self.server_hostname.is_some() {
            return Err(PyValueError::new_err(
                "server_hostname is only meaningful with ssl",
            ));
        }
        if self.handshake_timeout.is_some() {
            return Err(PyValueError::new_err(
                "ssl_handshake_timeout is only meaningful with ssl",
            ));
        }
        if self.shutdown_timeout.is_some() {
            return Err(PyValueError::new_err(
                "ssl_shutdown_timeout is only meaningful with ssl",
            ));
        }
        Ok(())
    }

    /// Client-side settings, or `None` when the caller passed no `ssl`.
    pub(super) fn client_settings(&self, py: Python<'_>) -> PyResult<Option<ClientTlsSettings>> {
        let Some(ssl) = self.ssl.as_ref() else {
            return Ok(None);
        };
        client_tls_settings(
            py,
            ssl.bind(py),
            self.server_hostname.as_ref().map(|value| value.bind(py)),
            self.handshake_timeout,
            self.shutdown_timeout,
        )
        .map(Some)
    }

    /// Server-side settings, or `None` when the caller passed no `ssl`.
    pub(super) fn server_settings(&self, py: Python<'_>) -> PyResult<Option<ServerTlsSettings>> {
        let Some(ssl) = self.ssl.as_ref() else {
            return Ok(None);
        };
        server_tls_settings(
            py,
            ssl.bind(py),
            self.handshake_timeout,
            self.shutdown_timeout,
        )
        .map(Some)
    }

    /// Server settings shared with the accept tasks that outlive this call.
    pub(super) fn shared_server_settings(
        &self,
        py: Python<'_>,
    ) -> PyResult<Option<Arc<ServerTlsSettings>>> {
        Ok(self.server_settings(py)?.map(Arc::new))
    }
}
