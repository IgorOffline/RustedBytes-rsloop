//! Operating-system integration kept outside loop and transport policy.

pub(crate) mod fd;

#[cfg(windows)]
pub(crate) mod windows_vibeio;
