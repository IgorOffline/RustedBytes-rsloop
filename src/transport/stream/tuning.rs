//! Sizing constants and their environment-variable overrides.
//!
//! The compile-time defaults are what every transport uses in practice; the
//! `RSLOOP_*` overrides exist so a deployment can retune buffering, TLS
//! admission, and reader spinning without a rebuild. Each override is resolved
//! once into a `OnceLock` so the hot paths stay a plain load.

use std::time::Duration;

pub(super) const DEFAULT_WRITE_BUFFER_HIGH_WATER: usize = 64 * 1024;
pub(super) const DEFAULT_WRITE_BUFFER_LOW_WATER: usize = DEFAULT_WRITE_BUFFER_HIGH_WATER / 4;
const DEFAULT_MAX_WRITE_BUFFER_SIZE: usize = 64 * 1024 * 1024;
pub(super) const MAX_PENDING_READ_COALESCE_BYTES: usize = 256 * 1024;
pub(super) const MAX_READ_EVENTS_PER_DRAIN: usize = 16;
pub(super) const MAX_READ_BYTES_PER_DRAIN: usize = 128 * 1024;
pub(super) const PENDING_READ_HIGH_WATER: usize = 1024 * 1024;
pub(super) const PENDING_READ_LOW_WATER: usize = PENDING_READ_HIGH_WATER / 4;
// Servers commonly emit a small protocol header followed immediately by a
// body. Defer only that header-sized first write for one loop turn so adjacent
// writes share a syscall; keep tiny control frames and larger payloads direct.
pub(super) const SMALL_WRITE_COALESCE_MIN_BYTES: usize = 64;
pub(super) const SMALL_WRITE_COALESCE_MAX_BYTES: usize = 512;
pub(super) const BLOCKING_POLL_INTERVAL_MS: i32 = 50;
pub(super) const WORKER_JOIN_TIMEOUT: Duration = Duration::from_secs(5);
// Keep the per-connection allocation modest. Large reads are delivered in
// several owned chunks, while the pending-event drain can still coalesce the
// slow protocol path up to MAX_PENDING_READ_COALESCE_BYTES.
pub(super) const STREAM_READ_BUFFER_SIZE: usize = 16 * 1024;
pub(super) const MAX_STREAM_READ_BUFFER_SIZE: usize = 64 * 1024;
#[cfg(windows)]
pub(super) const SERVER_POLL_READER_WRITE_THRESHOLD: usize = STREAM_READ_BUFFER_SIZE;
#[cfg(windows)]
pub(super) const SERVER_POLL_READER_TINY_TRIGGER_MAX_BYTES: usize = 16;
pub(super) const OWNED_READ_HANDOFF_MIN_BYTES: usize = 1024;
pub(super) const READ_BUFFER_POOL_LIMIT: usize = 1;
pub(super) const TLS_WORKER_STACK_SIZE: usize = 256 * 1024;
const DEFAULT_MAX_PENDING_TLS_HANDSHAKES: usize = 256;

pub(super) fn max_pending_tls_handshakes() -> usize {
    static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("RSLOOP_MAX_PENDING_TLS_HANDSHAKES")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_MAX_PENDING_TLS_HANDSHAKES)
    })
}

pub(super) fn max_write_buffer_size() -> usize {
    static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("RSLOOP_MAX_WRITE_BUFFER_BYTES")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_MAX_WRITE_BUFFER_SIZE)
    })
}

// After a successful read on a blocking reader worker, retry non-blocking
// reads for this long before falling back to poll(). Request/response peers
// usually answer within microseconds, and skipping the poll() sleep/wake
// halves per-roundtrip latency on actively chatting connections.
pub(super) fn reader_spin_window() -> Duration {
    static WINDOW: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *WINDOW.get_or_init(|| {
        let micros = std::env::var("RSLOOP_READER_SPIN_US")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(30);
        Duration::from_micros(micros.min(1_000))
    })
}
