use std::cell::RefCell;
use std::io::{self, ErrorKind};
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixDatagram;
use std::sync::Arc;
use std::task::Waker;
use std::time::Duration;

use mio::{Interest, Token};
use slab::Slab;

use crate::vibeio::driver::{Driver, Interruptor};
use crate::vibeio::fd_inner::InnerRawHandle;

const EVENT_CAPACITY: usize = 1024;
const WAKE_KEY: usize = usize::MAX;
const MAX_WAIT: Duration = Duration::from_secs(24 * 60 * 60);

pub struct KqueueInterruptor {
    waker: std::sync::Weak<DriverWaker>,
}

impl Interruptor for KqueueInterruptor {
    #[inline]
    fn interrupt(&self) {
        if let Some(waker) = self.waker.upgrade() {
            let _ = waker.wake();
        }
    }
}

struct DriverWaker {
    sender: UnixDatagram,
    receiver: UnixDatagram,
}

impl DriverWaker {
    fn new() -> io::Result<Self> {
        let (sender, receiver) = UnixDatagram::pair()?;
        sender.set_nonblocking(true)?;
        receiver.set_nonblocking(true)?;
        Ok(Self { sender, receiver })
    }

    #[inline]
    fn wake(&self) -> io::Result<()> {
        match self.sender.send(&[1]) {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == ErrorKind::WouldBlock => Ok(()),
            Err(err) if err.kind() == ErrorKind::Interrupted => self.wake(),
            Err(err) => Err(err),
        }
    }

    fn acknowledge(&self) {
        let mut buffer = [0_u8; 256];
        loop {
            match self.receiver.recv(&mut buffer) {
                Ok(_) => {}
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(_) => return,
            }
        }
    }
}

struct Registration {
    fd: RawFd,
    read_waiter: Option<Waker>,
    write_waiter: Option<Waker>,
    read_ready: bool,
    write_ready: bool,
    interest: Interest,
    registered_read: bool,
    registered_write: bool,
    generation: u32,
}

struct DriverState {
    registrations: Slab<Registration>,
    next_generation: u32,
}

pub struct KqueueDriver {
    kqueue: RawFd,
    state: RefCell<DriverState>,
    waker: Arc<DriverWaker>,
    ready_wakers: RefCell<Vec<Waker>>,
}

impl Drop for KqueueDriver {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.kqueue);
        }
    }
}

impl KqueueDriver {
    pub(crate) fn new() -> io::Result<Self> {
        let kqueue = unsafe { libc::kqueue() };
        if kqueue < 0 {
            return Err(io::Error::last_os_error());
        }
        let waker = match DriverWaker::new() {
            Ok(waker) => Arc::new(waker),
            Err(err) => {
                unsafe {
                    libc::close(kqueue);
                }
                return Err(err);
            }
        };
        let driver = Self {
            kqueue,
            state: RefCell::new(DriverState {
                registrations: Slab::with_capacity(1024),
                next_generation: 0,
            }),
            waker,
            ready_wakers: RefCell::new(Vec::with_capacity(64)),
        };
        driver.apply_change(Self::change(
            driver.waker.receiver.as_raw_fd(),
            libc::EVFILT_READ,
            libc::EV_ADD | libc::EV_ENABLE | libc::EV_CLEAR,
            WAKE_KEY,
        ))?;
        Ok(driver)
    }

    #[inline]
    fn change(fd: RawFd, filter: i16, flags: u16, key: usize) -> libc::kevent {
        libc::kevent {
            ident: fd as usize,
            filter,
            flags,
            fflags: 0,
            data: 0,
            udata: key as *mut libc::c_void,
        }
    }

    #[inline]
    fn encode_key(token: Token, generation: u32) -> usize {
        ((generation as usize) << 32) | (token.0 & u32::MAX as usize)
    }

    #[inline]
    fn decode_key(key: usize) -> (Token, u32) {
        (Token(key & u32::MAX as usize), (key >> 32) as u32)
    }

    fn apply_change(&self, change: libc::kevent) -> io::Result<()> {
        self.apply_changes(std::slice::from_ref(&change))
    }

    fn apply_changes(&self, changes: &[libc::kevent]) -> io::Result<()> {
        let result = unsafe {
            libc::kevent(
                self.kqueue,
                changes.as_ptr(),
                changes.len() as i32,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn delete_filter(&self, fd: RawFd, filter: i16) -> io::Result<()> {
        match self.apply_change(Self::change(fd, filter, libc::EV_DELETE, 0)) {
            Err(err)
                if matches!(
                    err.raw_os_error(),
                    Some(libc::ENOENT) | Some(libc::EBADF) | Some(libc::EINVAL)
                ) =>
            {
                Ok(())
            }
            result => result,
        }
    }

    fn wait_events(&self, timeout: Option<Duration>) -> io::Result<()> {
        let timeout = timeout.map(|duration| duration.min(MAX_WAIT));
        let timespec = timeout.map(|duration| libc::timespec {
            tv_sec: duration.as_secs().try_into().unwrap_or(i64::MAX),
            tv_nsec: duration.subsec_nanos().into(),
        });
        let timeout_ptr = timespec
            .as_ref()
            .map_or(std::ptr::null(), |value| value as *const libc::timespec);
        let mut events: [MaybeUninit<libc::kevent>; EVENT_CAPACITY] =
            [const { MaybeUninit::uninit() }; EVENT_CAPACITY];

        let count = unsafe {
            libc::kevent(
                self.kqueue,
                std::ptr::null(),
                0,
                events.as_mut_ptr().cast(),
                events.len() as i32,
                timeout_ptr,
            )
        };
        if count < 0 {
            let err = io::Error::last_os_error();
            return if err.kind() == ErrorKind::Interrupted {
                Ok(())
            } else {
                Err(err)
            };
        }

        let mut wakers = self.ready_wakers.borrow_mut();
        wakers.clear();
        let mut state = self.state.borrow_mut();
        for event in &events[..count as usize] {
            let event = unsafe { event.assume_init_ref() };
            let key = event.udata as usize;
            if key == WAKE_KEY {
                self.waker.acknowledge();
                continue;
            }
            let (token, generation) = Self::decode_key(key);
            let Some(registration) = state.registrations.get_mut(token.0) else {
                continue;
            };
            if registration.generation != generation {
                continue;
            }
            let (waiter, ready) = if event.filter == libc::EVFILT_READ {
                (&mut registration.read_waiter, &mut registration.read_ready)
            } else if event.filter == libc::EVFILT_WRITE {
                (
                    &mut registration.write_waiter,
                    &mut registration.write_ready,
                )
            } else {
                continue;
            };
            if let Some(waker) = waiter.take() {
                wakers.push(waker);
            } else {
                *ready = true;
            }
        }
        drop(state);
        for waker in wakers.drain(..) {
            waker.wake();
        }
        Ok(())
    }

    #[inline]
    fn filter(interest: Interest) -> i16 {
        if interest.is_readable() {
            libc::EVFILT_READ
        } else {
            libc::EVFILT_WRITE
        }
    }
}

impl Driver for KqueueDriver {
    type Interruptor = KqueueInterruptor;

    #[inline]
    fn should_flush(&self) -> bool {
        false
    }

    #[inline]
    fn wait(&self, timeout: Option<Duration>) {
        if let Err(err) = self.wait_events(timeout) {
            panic!("kqueue wait failed: {err}");
        }
    }

    fn register_handle(&self, handle: &InnerRawHandle, interest: Interest) -> io::Result<Token> {
        let (token, generation) = {
            let mut state = self.state.borrow_mut();
            state.next_generation = state.next_generation.wrapping_add(1);
            if state.next_generation == 0 {
                state.next_generation = 1;
            }
            let generation = state.next_generation;
            let entry = state.registrations.vacant_entry();
            let token = Token(entry.key());
            entry.insert(Registration {
                fd: handle.handle,
                read_waiter: None,
                write_waiter: None,
                read_ready: false,
                write_ready: false,
                interest,
                registered_read: interest.is_readable(),
                registered_write: interest.is_writable(),
                generation,
            });
            (token, generation)
        };
        let key = Self::encode_key(token, generation);
        let mut changes = Vec::with_capacity(2);
        if interest.is_readable() {
            changes.push(Self::change(
                handle.handle,
                libc::EVFILT_READ,
                libc::EV_ADD | libc::EV_ENABLE | libc::EV_CLEAR,
                key,
            ));
        }
        if interest.is_writable() {
            changes.push(Self::change(
                handle.handle,
                libc::EVFILT_WRITE,
                libc::EV_ADD | libc::EV_ENABLE | libc::EV_CLEAR,
                key,
            ));
        }
        if let Err(err) = self.apply_changes(&changes) {
            let _ = self.state.borrow_mut().registrations.try_remove(token.0);
            return Err(err);
        }
        Ok(token)
    }

    fn reregister_handle(&self, handle: &InnerRawHandle, interest: Interest) -> io::Result<()> {
        let (fd, generation, old_read, old_write) = {
            let mut state = self.state.borrow_mut();
            let registration = state.registrations.get_mut(handle.token.0).ok_or_else(|| {
                io::Error::new(
                    ErrorKind::NotFound,
                    format!("I/O token {} is not registered", handle.token.0),
                )
            })?;
            let old_read = registration.registered_read;
            let old_write = registration.registered_write;
            registration.interest = interest;
            registration.registered_read = interest.is_readable();
            registration.registered_write = interest.is_writable();
            if !interest.is_readable() {
                registration.read_waiter = None;
                registration.read_ready = false;
            }
            if !interest.is_writable() {
                registration.write_waiter = None;
                registration.write_ready = false;
            }
            (
                registration.fd,
                registration.generation,
                old_read,
                old_write,
            )
        };
        if old_read && !interest.is_readable() {
            self.delete_filter(fd, libc::EVFILT_READ)?;
        }
        if old_write && !interest.is_writable() {
            self.delete_filter(fd, libc::EVFILT_WRITE)?;
        }
        let key = Self::encode_key(handle.token, generation);
        let mut additions = Vec::with_capacity(2);
        if !old_read && interest.is_readable() {
            additions.push(Self::change(
                fd,
                libc::EVFILT_READ,
                libc::EV_ADD | libc::EV_ENABLE | libc::EV_CLEAR,
                key,
            ));
        }
        if !old_write && interest.is_writable() {
            additions.push(Self::change(
                fd,
                libc::EVFILT_WRITE,
                libc::EV_ADD | libc::EV_ENABLE | libc::EV_CLEAR,
                key,
            ));
        }
        if !additions.is_empty() {
            self.apply_changes(&additions)?;
        }
        Ok(())
    }

    fn deregister_handle(&self, handle: &InnerRawHandle) -> io::Result<()> {
        let registration = self
            .state
            .borrow_mut()
            .registrations
            .try_remove(handle.token.0)
            .ok_or_else(|| {
                io::Error::new(
                    ErrorKind::NotFound,
                    format!("I/O token {} is not registered", handle.token.0),
                )
            })?;
        if registration.registered_read {
            self.delete_filter(registration.fd, libc::EVFILT_READ)?;
        }
        if registration.registered_write {
            self.delete_filter(registration.fd, libc::EVFILT_WRITE)?;
        }
        Ok(())
    }

    fn submit_poll(
        &self,
        handle: &InnerRawHandle,
        waker: Waker,
        interest: Interest,
    ) -> io::Result<()> {
        let filter = Self::filter(interest);
        let wake_now = {
            let mut state = self.state.borrow_mut();
            let registration = state.registrations.get_mut(handle.token.0).ok_or_else(|| {
                io::Error::new(
                    ErrorKind::NotFound,
                    format!("I/O token {} is not registered", handle.token.0),
                )
            })?;
            let (waiter, ready) = if filter == libc::EVFILT_READ {
                (&mut registration.read_waiter, &mut registration.read_ready)
            } else {
                (
                    &mut registration.write_waiter,
                    &mut registration.write_ready,
                )
            };
            if *ready {
                *ready = false;
                true
            } else {
                if !waiter
                    .as_ref()
                    .is_some_and(|current| current.will_wake(&waker))
                {
                    *waiter = Some(waker.clone());
                }
                false
            }
        };
        if wake_now {
            waker.wake();
        }
        Ok(())
    }

    #[inline]
    fn get_interruptor(&self) -> Self::Interruptor {
        KqueueInterruptor {
            waker: Arc::downgrade(&self.waker),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::vibeio::driver::{AnyDriver, RegistrationMode};

    struct WakeCount(AtomicUsize);

    impl std::task::Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn readiness_arriving_before_waiter_is_latched() {
        let driver = Rc::new(AnyDriver::Kqueue(
            KqueueDriver::new().expect("kqueue driver should initialize"),
        ));
        let (reader, mut writer) =
            std::os::unix::net::UnixStream::pair().expect("socket pair should initialize");
        reader
            .set_nonblocking(true)
            .expect("reader should become nonblocking");
        let handle = InnerRawHandle::new_with_driver_and_mode(
            &driver,
            reader.as_raw_fd(),
            Interest::READABLE,
            RegistrationMode::Poll,
        )
        .expect("reader should register");

        writer.write_all(b"!").expect("socket write should succeed");
        driver.wait(Some(Duration::from_millis(100)));

        let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
        driver
            .submit_poll(&handle, Waker::from(Arc::clone(&wakes)), Interest::READABLE)
            .expect("latched readiness should submit");
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    }
}
