//! Shared bounded-input helpers for Kani proof harnesses.
//!
//! This module is absent from normal and test builds. Bounds are intentionally
//! small: each proof records the exact domain it establishes, and ordinary
//! property tests continue to exercise much larger examples.

pub(crate) const MAX_BYTES: usize = 8;
pub(crate) const MAX_POOL_OPERATIONS: usize = 6;

/// Produce an arbitrary byte vector whose length is at most `N`.
pub(crate) fn any_bytes<const N: usize>() -> Vec<u8> {
    let bytes: [u8; N] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= N);
    bytes[..len].to_vec()
}
