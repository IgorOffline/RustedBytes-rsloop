//! Child-process setup that must run between `fork` and `exec` on Unix.

use std::process::Command;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use super::UnixPreExecConfig;

#[cfg(unix)]
pub(super) fn apply(command: &mut Command, config: UnixPreExecConfig) {
    // SAFETY: `pre_exec` installs a closure that runs in the child process after fork and before
    // exec. The closure only invokes async-signal-safe libc operations and returns OS errors.
    unsafe {
        command.pre_exec(move || apply_in_child(&config));
    }
}

#[cfg(not(unix))]
pub(super) fn apply(_command: &mut Command, _config: UnixPreExecConfig) {}

#[cfg(unix)]
fn apply_in_child(config: &UnixPreExecConfig) -> std::io::Result<()> {
    restore_child_signals(config.restore_signals)?;
    clear_pass_fds_cloexec(&config.pass_fds)?;
    apply_child_attributes(config)
}

#[cfg(unix)]
fn restore_child_signals(restore_signals: bool) -> std::io::Result<()> {
    if !restore_signals {
        return Ok(());
    }

    // SAFETY: this runs in the `pre_exec` child and installs a valid signal disposition.
    let result = unsafe { libc::signal(libc::SIGPIPE, libc::SIG_DFL) };
    if result == libc::SIG_ERR {
        return Err(std::io::Error::last_os_error());
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // SAFETY: this runs in the `pre_exec` child and installs a valid signal disposition.
        let result = unsafe { libc::signal(libc::SIGXFSZ, libc::SIG_DFL) };
        if result == libc::SIG_ERR {
            return Err(std::io::Error::last_os_error());
        }
    }

    Ok(())
}

#[cfg(unix)]
fn clear_pass_fds_cloexec(pass_fds: &[i32]) -> std::io::Result<()> {
    for fd in pass_fds {
        // SAFETY: `fd` is supplied to `pre_exec`; F_GETFD neither dereferences pointers nor allocates.
        let flags = unsafe { libc::fcntl(*fd, libc::F_GETFD) };
        if flags == -1 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: `fd` and the flags returned above are valid inputs for F_SETFD.
        let result = unsafe { libc::fcntl(*fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) };
        if result == -1 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(unix)]
fn apply_child_attributes(config: &UnixPreExecConfig) -> std::io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let ngroups = config.extra_groups.as_ref().map(Vec::len);
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let ngroups = config
        .extra_groups
        .as_ref()
        .map(|groups| {
            groups.len().try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "extra_groups length exceeds platform limit",
                )
            })
        })
        .transpose()?;

    if config.start_new_session {
        // SAFETY: called only inside the `pre_exec` child; `setsid` takes no pointers.
        let result = unsafe { libc::setsid() };
        if result == -1 {
            return Err(std::io::Error::last_os_error());
        }
    }
    if let Some(process_group) = config.process_group
        && !(config.start_new_session && process_group == 0)
    {
        // SAFETY: called only inside the `pre_exec` child with numeric process IDs.
        let result = unsafe { libc::setpgid(0, process_group) };
        if result == -1 {
            return Err(std::io::Error::last_os_error());
        }
    }
    if let Some(groups) = &config.extra_groups {
        // SAFETY: `groups.as_ptr()` is valid for the supplied `ngroups` length during the call.
        let result =
            unsafe { libc::setgroups(ngroups.expect("extra_groups present"), groups.as_ptr()) };
        if result == -1 {
            return Err(std::io::Error::last_os_error());
        }
    }
    if let Some(gid) = config.gid {
        // SAFETY: called only inside the `pre_exec` child with a numeric group ID.
        let result = unsafe { libc::setgid(gid) };
        if result == -1 {
            return Err(std::io::Error::last_os_error());
        }
    }
    if let Some(uid) = config.uid {
        // SAFETY: called only inside the `pre_exec` child with a numeric user ID.
        let result = unsafe { libc::setuid(uid) };
        if result == -1 {
            return Err(std::io::Error::last_os_error());
        }
    }
    if let Some(umask) = config.umask {
        // SAFETY: called only inside the `pre_exec` child with a validated mode value.
        unsafe { libc::umask(umask as libc::mode_t) };
    }

    Ok(())
}
