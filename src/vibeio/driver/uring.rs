use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{self, ErrorKind};
use std::os::fd::RawFd;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use io_uring::types::{SubmitArgs, Timespec};
use io_uring::{IoUring, cqueue, opcode, squeue, types};
use mio::{Interest, Token};
use slab::Slab;

use crate::vibeio::driver::{CompletionIoResult, Interruptor};
use crate::vibeio::{
    driver::{Driver, RegistrationMode},
    fd_inner::InnerRawHandle,
};

const KEY_KIND_BITS: u64 = 2;
const KEY_KIND_MASK: u64 = (1u64 << KEY_KIND_BITS) - 1;
const POLL_KEY_KIND: u8 = 0;
const COMPLETION_KEY_KIND: u8 = 1;
const ACCEPT_KEY_KIND: u8 = 2;
const MEMORY_FALLBACK_ENTRIES: [u32; 2] = [256, 64];

fn build_with_memory_fallback<T>(
    entries: u32,
    mut build: impl FnMut(u32) -> io::Result<T>,
) -> io::Result<(T, u32)> {
    let mut previous = None;
    let mut last_memory_error = None;

    for candidate in std::iter::once(entries).chain(
        MEMORY_FALLBACK_ENTRIES
            .into_iter()
            .map(|fallback| entries.min(fallback)),
    ) {
        if previous == Some(candidate) {
            continue;
        }
        previous = Some(candidate);

        match build(candidate) {
            Ok(value) => return Ok((value, candidate)),
            Err(err) if err.raw_os_error() == Some(libc::ENOMEM) => {
                last_memory_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }

    Err(last_memory_error.expect("at least one io_uring build was attempted"))
}

pub struct UringInterruptor {
    eventfd: std::sync::Weak<RawFd>,
}

impl Interruptor for UringInterruptor {
    #[inline]
    fn interrupt(&self) {
        if let Some(eventfd) = self.eventfd.upgrade() {
            // Write to the eventfd to wake up the driver
            let value: u64 = 1;
            let _ = unsafe {
                libc::write(
                    *eventfd,
                    &value as *const u64 as *const std::ffi::c_void,
                    std::mem::size_of::<u64>(),
                )
            };
        }
    }
}

struct PollRegistration {
    fd: RawFd,
    poll_mask: u32,
    waiter: Option<Waker>,
    poll_armed: bool,
    generation: u32,
}

struct AcceptRegistration {
    results: VecDeque<i32>,
    waiter: Option<Waker>,
    armed: bool,
}

impl Drop for AcceptRegistration {
    fn drop(&mut self) {
        for result in self.results.drain(..) {
            if result >= 0 {
                unsafe {
                    libc::close(result);
                }
            }
        }
    }
}

struct CompletionRegistration {
    fd: RawFd,
    generation: u32,
    accept: Option<AcceptRegistration>,
}

enum HandleRegistration {
    Completion(CompletionRegistration),
    Poll(PollRegistration),
}

struct Completion {
    waiter: Option<Waker>,
    completed: Option<i32>,
    ignored_data: Option<Box<dyn std::any::Any>>,
}

struct DriverState {
    registrations: Slab<HandleRegistration>,
    completions: Slab<Completion>,
    next_registration_generation: u32,
}

pub struct UringDriver {
    ring: RefCell<IoUring>,
    state: RefCell<DriverState>,
    interrupt_eventfd: Option<StdArc<RawFd>>,
    interrupt_buffer: RefCell<Box<[u8; 8]>>,
    pending_submissions: AtomicBool,
}

impl Drop for UringDriver {
    fn drop(&mut self) {
        if let Some(eventfd) = self.interrupt_eventfd.take() {
            // Close eventfd
            unsafe { libc::close(*eventfd) };
        }
    }
}

impl UringDriver {
    #[inline]
    pub(crate) fn new(entries: u32, builder: io_uring::Builder) -> Result<Self, io::Error> {
        // Ring teardown is deferred by the kernel. Rapid runtime churn can
        // therefore hit ENOMEM even though earlier rings have been dropped;
        // smaller queues keep initialization reliable while cleanup catches up.
        let (ring, ring_entries) =
            build_with_memory_fallback(entries, |candidate| builder.build(candidate))?;
        if !ring.params().is_feature_ext_arg() {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "rsloop requires Linux 6.1+ with io_uring extended arguments",
            ));
        }

        // Create eventfd only after ring initialization succeeds so failed
        // attempts cannot leak descriptors.
        let eventfd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if eventfd < 0 {
            return Err(io::Error::last_os_error());
        }
        let driver = Self {
            ring: RefCell::new(ring),
            state: RefCell::new(DriverState {
                registrations: Slab::with_capacity(ring_entries as usize),
                completions: Slab::with_capacity(ring_entries as usize),
                next_registration_generation: 0,
            }),
            interrupt_eventfd: Some(StdArc::new(eventfd)),
            interrupt_buffer: RefCell::new(Box::new([0; 8])),
            pending_submissions: AtomicBool::new(false),
        };

        driver.submit_interrupt();

        Ok(driver)
    }

    #[inline]
    fn update_waiter(waiter_slot: &mut Option<Waker>, waker: Waker) {
        if !waiter_slot
            .as_ref()
            .is_some_and(|waiter| waiter.will_wake(&waker))
        {
            *waiter_slot = Some(waker);
        }
    }

    #[inline]
    fn encode_completion_key(token: usize) -> u64 {
        ((token as u64) << KEY_KIND_BITS) | COMPLETION_KEY_KIND as u64
    }

    #[inline]
    fn encode_poll_key(token: Token, generation: u32) -> u64 {
        ((u64::from(generation) & 0x3fff_ffff) << 34)
            | ((token.0 as u64 & u64::from(u32::MAX)) << KEY_KIND_BITS)
            | POLL_KEY_KIND as u64
    }

    #[inline]
    fn decode_token(key: u64) -> Token {
        Token(((key >> KEY_KIND_BITS) & u64::from(u32::MAX)) as usize)
    }

    #[inline]
    fn decode_poll_generation(key: u64) -> u32 {
        (key >> 34) as u32
    }

    #[inline]
    fn encode_accept_key(token: Token, generation: u32) -> u64 {
        ((u64::from(generation) & 0x3fff_ffff) << 34)
            | ((token.0 as u64 & u64::from(u32::MAX)) << KEY_KIND_BITS)
            | ACCEPT_KEY_KIND as u64
    }

    #[inline]
    fn decode_key_kind(key: u64) -> u8 {
        (key & KEY_KIND_MASK) as u8
    }

    #[inline]
    fn interest_to_poll_mask(interest: Interest) -> u32 {
        let mut mask = 0;
        if interest.is_readable() {
            mask |= libc::POLLIN as u32;
        }
        if interest.is_writable() {
            mask |= libc::POLLOUT as u32;
        }
        mask
    }

    #[inline]
    fn submitter_call_result(result: Result<usize, io::Error>) -> Result<(), io::Error> {
        match result {
            Ok(_) => Ok(()),
            Err(err) if err.raw_os_error() == Some(libc::EBUSY) => Ok(()),
            Err(err) if err.raw_os_error() == Some(libc::ETIME) => Ok(()), // io_uring Timeout
            Err(err) => Err(err),
        }
    }

    #[inline]
    fn push_entry(&self, entry: squeue::Entry) -> Result<(), io::Error> {
        let mut ring = self.ring.borrow_mut();

        if ring.submission().is_full() {
            Self::submitter_call_result(ring.submit())?;
        }

        let mut sq = ring.submission();
        unsafe {
            sq.push(&entry)
                .map_err(|_| io::Error::other("io_uring submission queue is full"))?;
        }

        self.pending_submissions.store(true, Ordering::Release);

        Ok(())
    }

    #[inline]
    fn push_poll_add(
        &self,
        token: Token,
        generation: u32,
        fd: RawFd,
        poll_mask: u32,
    ) -> Result<(), io::Error> {
        let entry = opcode::PollAdd::new(types::Fd(fd), poll_mask)
            .multi(true)
            .build()
            .user_data(Self::encode_poll_key(token, generation));
        self.push_entry(entry)
    }

    #[inline]
    fn collect_completions(
        &self,
        wait_for_one: bool,
        timeout: Option<Duration>,
    ) -> Result<(), io::Error> {
        {
            let mut ring = self.ring.borrow_mut();
            let should_submit = if wait_for_one {
                true
            } else {
                !ring.submission().is_empty()
            };

            if should_submit {
                let submit_result = if wait_for_one {
                    if let Some(timeout) = timeout {
                        let timespec = Timespec::from(timeout);
                        ring.submitter()
                            .submit_with_args(1, &SubmitArgs::new().timespec(&timespec))
                    } else {
                        ring.submit_and_wait(1)
                    }
                } else {
                    ring.submit()
                };
                Self::submitter_call_result(submit_result)?;
                self.pending_submissions
                    .store(!ring.submission().is_empty(), Ordering::Release);
            } else {
                self.pending_submissions.store(false, Ordering::Release);
            }
        }

        // Drain any new completions produced by the submit above.
        let need_interrupt = {
            let mut ring = self.ring.borrow_mut();
            let mut state = self.state.borrow_mut();
            Self::drain_cq(&mut ring, &mut state)
        };
        if need_interrupt {
            self.submit_interrupt();
        }

        Ok(())
    }

    /// Drain the completion queue and wake any registered waiters.
    #[inline]
    fn drain_cq(ring: &mut IoUring, state: &mut DriverState) -> bool {
        let mut interrupt = false;

        // Collect wakers in a small inline array to avoid heap allocation
        // in the common case (0-8 completions per collect_completions call).
        // Most flush/wait calls produce very few completions.
        let mut fast_wakers: [Option<Waker>; 8] = Default::default();
        let mut fast_count = 0;
        let mut overflow_wakers: Vec<Waker> = Vec::new();

        {
            let cq = ring.completion();

            for cqe in cq {
                let key = cqe.user_data();
                let result = cqe.result();

                if key == u64::MAX {
                    // Task interrupted
                    interrupt = true;
                    continue;
                }

                let token = Self::decode_token(key);
                let key_kind = Self::decode_key_kind(key);

                if key_kind == POLL_KEY_KIND {
                    let generation = Self::decode_poll_generation(key);
                    let waiter = match state.registrations.get_mut(token.0) {
                        Some(HandleRegistration::Poll(registration))
                            if registration.generation == generation =>
                        {
                            registration.poll_armed = cqueue::more(cqe.flags());
                            registration.waiter.take()
                        }
                        _ => None,
                    };
                    if let Some(waiter) = waiter {
                        if fast_count < fast_wakers.len() {
                            fast_wakers[fast_count] = Some(waiter);
                        } else {
                            overflow_wakers.push(waiter);
                        }
                        fast_count += 1;
                    }
                    continue;
                }

                if key_kind == ACCEPT_KEY_KIND {
                    let generation = Self::decode_poll_generation(key);
                    let mut delivered = false;
                    if let Some(HandleRegistration::Completion(registration)) =
                        state.registrations.get_mut(token.0)
                    {
                        if registration.generation == generation {
                            if let Some(accept) = registration.accept.as_mut() {
                                accept.armed = cqueue::more(cqe.flags());
                                accept.results.push_back(result);
                                if let Some(waiter) = accept.waiter.take() {
                                    if fast_count < fast_wakers.len() {
                                        fast_wakers[fast_count] = Some(waiter);
                                    } else {
                                        overflow_wakers.push(waiter);
                                    }
                                    fast_count += 1;
                                }
                                delivered = true;
                            }
                        }
                    }
                    if !delivered && result >= 0 {
                        unsafe {
                            libc::close(result);
                        }
                    }
                    continue;
                }

                let mut remove_completion = false;
                let waiter = match state.completions.get_mut(token.0) {
                    Some(completion) => {
                        completion.completed = Some(result);
                        remove_completion = completion.ignored_data.is_some();
                        completion.waiter.take()
                    }
                    None => None,
                };
                if remove_completion {
                    state.completions.remove(token.0);
                }
                if let Some(waiter) = waiter {
                    if fast_count < fast_wakers.len() {
                        fast_wakers[fast_count] = Some(waiter);
                    } else {
                        overflow_wakers.push(waiter);
                    }
                    fast_count += 1;
                }
            }
        }

        for waker in fast_wakers.iter_mut().take(fast_count) {
            if let Some(w) = waker.take() {
                w.wake();
            }
        }
        for waker in overflow_wakers {
            waker.wake();
        }

        interrupt
    }

    #[inline]
    fn submit_interrupt(&self) {
        use io_uring::{opcode, types};
        // Submit a read operation to the eventfd to wake up the driver
        let mut buffer = self.interrupt_buffer.borrow_mut();
        let entry = opcode::Read::new(
            types::Fd(
                *self
                    .interrupt_eventfd
                    .as_ref()
                    .expect("interrupt_eventfd is not initialized")
                    .as_ref(),
            ),
            buffer.as_mut_ptr(),
            buffer.len() as u32,
        )
        .build()
        .user_data(u64::MAX);

        // We use push_entry here. It handles submission if full.
        // We panic if it fails because we cannot recover (we won't be able to wake up).
        if let Err(err) = self.push_entry(entry) {
            panic!("io_uring: failed to submit interrupt task: {}", err);
        }
    }
}

#[cfg(test)]
mod memory_fallback_tests {
    use super::*;

    #[test]
    fn retries_smaller_rings_after_out_of_memory() {
        let mut attempts = Vec::new();
        let selected = build_with_memory_fallback(1024, |entries| {
            attempts.push(entries);
            if entries > 64 {
                Err(io::Error::from_raw_os_error(libc::ENOMEM))
            } else {
                Ok(entries)
            }
        })
        .expect("small ring should initialize");

        assert_eq!(selected, (64, 64));
        assert_eq!(attempts, [1024, 256, 64]);
    }

    #[test]
    fn does_not_retry_non_memory_errors() {
        let mut attempts = Vec::new();
        let err = build_with_memory_fallback(1024, |entries| -> io::Result<()> {
            attempts.push(entries);
            Err(io::Error::from_raw_os_error(libc::EINVAL))
        })
        .expect_err("invalid configuration should be preserved");

        assert_eq!(err.raw_os_error(), Some(libc::EINVAL));
        assert_eq!(attempts, [1024]);
    }
}

impl Driver for UringDriver {
    type Interruptor = UringInterruptor;

    #[inline]
    fn flush(&self) {
        match self.collect_completions(false, None) {
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => panic!("io_uring submit failed while processing I/O completions: {err}"),
        }
    }

    #[inline]
    fn should_flush(&self) -> bool {
        self.pending_submissions.load(Ordering::Acquire)
    }

    #[inline]
    fn wait(&self, timeout: Option<Duration>) {
        match self.collect_completions(true, timeout) {
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => panic!("io_uring submit_and_wait failed while waiting for I/O: {err}"),
        }
    }

    #[inline]
    fn get_interruptor(&self) -> Self::Interruptor {
        UringInterruptor {
            eventfd: StdArc::downgrade(
                self.interrupt_eventfd
                    .as_ref()
                    .expect("interrupt_eventfd is not initialized"),
            ),
        }
    }

    #[inline]
    fn register_handle(
        &self,
        handle: &InnerRawHandle,
        interest: Interest,
    ) -> Result<Token, io::Error> {
        self.register_handle_with_mode(handle, interest, RegistrationMode::Completion)
    }

    #[inline]
    fn register_handle_with_mode(
        &self,
        handle: &InnerRawHandle,
        interest: Interest,
        mode: RegistrationMode,
    ) -> Result<Token, io::Error> {
        let mut state = self.state.borrow_mut();
        state.next_registration_generation =
            state.next_registration_generation.wrapping_add(1) & 0x3fff_ffff;
        if state.next_registration_generation == 0 {
            state.next_registration_generation = 1;
        }
        let generation = state.next_registration_generation;
        let entry = state.registrations.vacant_entry();
        let token = Token(entry.key());

        match mode {
            RegistrationMode::Completion => {
                entry.insert(HandleRegistration::Completion(CompletionRegistration {
                    fd: handle.handle,
                    generation,
                    accept: None,
                }));
            }
            RegistrationMode::Poll => {
                entry.insert(HandleRegistration::Poll(PollRegistration {
                    fd: handle.handle,
                    poll_mask: Self::interest_to_poll_mask(interest),
                    waiter: None,
                    poll_armed: false,
                    generation,
                }));
            }
        }

        Ok(token)
    }

    #[inline]
    fn reregister_handle(
        &self,
        handle: &InnerRawHandle,
        interest: Interest,
    ) -> Result<(), io::Error> {
        let mut state = self.state.borrow_mut();
        match state.registrations.get_mut(handle.token.0) {
            Some(HandleRegistration::Completion(_)) => Ok(()),
            Some(HandleRegistration::Poll(registration)) => {
                registration.poll_mask = Self::interest_to_poll_mask(interest);
                Ok(())
            }
            None => Err(io::Error::new(
                ErrorKind::NotFound,
                format!(
                    "I/O token {} is not registered with this driver",
                    handle.token.0
                ),
            )),
        }
    }

    #[inline]
    fn deregister_handle(&self, handle: &InnerRawHandle) -> Result<(), io::Error> {
        {
            // Cancel any pending io_uring operations for this handle
            let ring = self.ring.borrow_mut();
            let _ = ring.submitter().register_sync_cancel(
                Some(Timespec::new().nsec(0).sec(0)),
                types::CancelBuilder::fd(types::Fd(handle.handle)),
            );
        }

        let mut state = self.state.borrow_mut();
        if state.registrations.try_remove(handle.token.0).is_none() {
            return Err(io::Error::new(
                ErrorKind::NotFound,
                format!(
                    "I/O token {} is not registered with this driver",
                    handle.token.0
                ),
            ));
        }

        Ok(())
    }

    #[inline]
    fn supports_completion(&self) -> bool {
        true
    }

    #[inline]
    fn submit_poll(
        &self,
        handle: &InnerRawHandle,
        waker: Waker,
        interest: Interest,
    ) -> Result<(), io::Error> {
        let token = handle.token();
        let poll_spec = {
            let mut state = self.state.borrow_mut();
            let registration = match state.registrations.get_mut(token.0) {
                Some(HandleRegistration::Poll(registration)) => registration,
                Some(HandleRegistration::Completion(_)) => {
                    return Err(io::Error::new(
                        ErrorKind::Unsupported,
                        format!(
                            "I/O token {} is registered for completion mode, not poll mode",
                            token.0
                        ),
                    ));
                }
                None => {
                    return Err(io::Error::new(
                        ErrorKind::NotFound,
                        format!("I/O token {} is not registered with this driver", token.0),
                    ));
                }
            };

            Self::update_waiter(&mut registration.waiter, waker);
            let desired_mask = Self::interest_to_poll_mask(interest);
            registration.poll_mask = desired_mask;

            if registration.poll_armed {
                None
            } else {
                registration.poll_armed = true;
                Some((registration.generation, registration.fd, desired_mask))
            }
        };

        if let Some((generation, fd, poll_mask)) = poll_spec {
            if let Err(submit_err) = self.push_poll_add(token, generation, fd, poll_mask) {
                let mut state = self.state.borrow_mut();
                if let Some(HandleRegistration::Poll(registration)) =
                    state.registrations.get_mut(token.0)
                {
                    registration.poll_armed = false;
                    registration.waiter = None;
                }
                return Err(submit_err);
            }
        }

        Ok(())
    }

    #[inline]
    fn submit_completion<O>(&self, op: &mut O, waker: Waker) -> super::CompletionIoResult
    where
        O: crate::vibeio::op::Op,
    {
        let mut state = self.state.borrow_mut();
        let vacant_completion = state.completions.vacant_entry();
        let token = vacant_completion.key();

        // Build the SQE. If this fails, return the error.
        let entry = match op.build_completion_entry(Self::encode_completion_key(token)) {
            Ok(entry) => entry,
            Err(err) => return CompletionIoResult::SubmitErr(err),
        };

        // Push the SQE into the submission queue. If this fails, undo the inflight
        // flag and clear waiters on the registration.
        if let Err(err) = self.push_entry(entry) {
            return CompletionIoResult::SubmitErr(err);
        }

        // Store the operation in the completions slab.
        vacant_completion.insert(Completion {
            waiter: Some(waker),
            completed: None,
            ignored_data: None,
        });

        CompletionIoResult::Retry(token)
    }

    #[inline]
    fn get_completion_result(&self, token: usize) -> Option<i32> {
        let mut state = self.state.borrow_mut();
        let completed = state.completions.get(token).and_then(|c| c.completed);
        if completed.is_some() {
            state.completions.remove(token);
        }
        completed
    }

    fn poll_multishot_accept(
        &self,
        handle: &InnerRawHandle,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<i32>> {
        let submission = {
            let mut state = self.state.borrow_mut();
            let registration = match state.registrations.get_mut(handle.token.0) {
                Some(HandleRegistration::Completion(registration)) => registration,
                Some(HandleRegistration::Poll(_)) => {
                    return Poll::Ready(Err(io::Error::new(
                        ErrorKind::Unsupported,
                        "multishot accept requires completion registration",
                    )));
                }
                None => {
                    return Poll::Ready(Err(io::Error::new(
                        ErrorKind::NotFound,
                        format!("I/O token {} is not registered", handle.token.0),
                    )));
                }
            };
            let accept = registration
                .accept
                .get_or_insert_with(|| AcceptRegistration {
                    results: VecDeque::new(),
                    waiter: None,
                    armed: false,
                });
            if let Some(result) = accept.results.pop_front() {
                return if result >= 0 {
                    Poll::Ready(Ok(result))
                } else {
                    Poll::Ready(Err(io::Error::from_raw_os_error(-result)))
                };
            }
            Self::update_waiter(&mut accept.waiter, cx.waker().clone());
            if accept.armed {
                None
            } else {
                accept.armed = true;
                Some((registration.fd, registration.generation))
            }
        };

        if let Some((fd, generation)) = submission {
            let entry = opcode::AcceptMulti::new(types::Fd(fd))
                .flags(libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK)
                .build()
                .user_data(Self::encode_accept_key(handle.token, generation));
            if let Err(err) = self.push_entry(entry) {
                if let Some(HandleRegistration::Completion(registration)) = self
                    .state
                    .borrow_mut()
                    .registrations
                    .get_mut(handle.token.0)
                {
                    if let Some(accept) = registration.accept.as_mut() {
                        accept.armed = false;
                        accept.waiter = None;
                    }
                }
                return Poll::Ready(Err(err));
            }
        }
        Poll::Pending
    }

    #[inline]
    fn set_completion_waker(&self, token: usize, waker: Waker) {
        let mut state = self.state.borrow_mut();
        if let Some(c) = state.completions.get_mut(token) {
            Self::update_waiter(&mut c.waiter, waker);
        }
    }

    #[inline]
    fn ignore_completion(&self, token: usize, data: Box<dyn std::any::Any>) {
        let mut state = self.state.borrow_mut();
        if let Some(c) = state.completions.get_mut(token) {
            c.ignored_data = Some(data);
        }
    }
}
