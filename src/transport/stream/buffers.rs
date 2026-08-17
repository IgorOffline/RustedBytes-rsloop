//! Owned buffers shared by stream reader and writer paths.

use std::ops::{Deref, DerefMut};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use futures::future::poll_fn;
use futures::task::AtomicWaker;

use super::tuning::{
    MAX_STREAM_READ_BUFFER_SIZE, READ_BUFFER_POOL_LIMIT, WRITE_BUFFER_BLOCK_SIZE,
    WRITE_BUFFER_POOL_LIMIT,
};

pub(super) struct OwnedWriteBuffer {
    bytes: Vec<u8>,
    offset: usize,
    pool: Option<Arc<WriteBufferPool>>,
}

/// A socket-read allocation whose pool slot follows the bytes into the
/// consumer.  Native stream readers can retain this buffer directly instead
/// of copying it and still return the allocation to the bounded transport
/// pool when they replace or drop it.
pub(super) struct OwnedReadBuffer {
    bytes: Vec<u8>,
    pool: Option<Arc<ReadBufferPool>>,
}

impl OwnedReadBuffer {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
            pool: None,
        }
    }

    pub(super) fn from_pooled(bytes: Vec<u8>, pool: &Arc<ReadBufferPool>) -> Self {
        Self {
            bytes,
            pool: Some(Arc::clone(pool)),
        }
    }

    #[cfg(test)]
    pub(super) fn from_vec(bytes: Vec<u8>) -> Self {
        Self { bytes, pool: None }
    }
}

impl Deref for OwnedReadBuffer {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl DerefMut for OwnedReadBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bytes
    }
}

impl Drop for OwnedReadBuffer {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            pool.release(std::mem::take(&mut self.bytes));
        }
    }
}

pub(super) struct PendingReadBuffer<'a> {
    bytes: Vec<u8>,
    home: &'a Mutex<Vec<u8>>,
}

impl<'a> PendingReadBuffer<'a> {
    pub(super) fn new(home: &'a Mutex<Vec<u8>>) -> Self {
        let mut bytes = std::mem::take(&mut *home.lock().expect("poisoned read coalesce buffer"));
        bytes.clear();
        Self { bytes, home }
    }

    #[inline]
    pub(super) fn len(&self) -> usize {
        self.bytes.len()
    }

    #[inline]
    pub(super) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub(super) fn extend(&mut self, data: &[u8]) {
        self.bytes.extend_from_slice(data);
    }
}

impl Drop for PendingReadBuffer<'_> {
    fn drop(&mut self) {
        self.bytes.clear();
        *self.home.lock().expect("poisoned read coalesce buffer") = std::mem::take(&mut self.bytes);
    }
}

impl OwnedWriteBuffer {
    #[inline]
    #[cfg(test)]
    pub(super) fn from_slice(data: &[u8]) -> Self {
        Self {
            bytes: data.to_vec(),
            offset: 0,
            pool: None,
        }
    }

    pub(super) fn from_pooled_slice(data: &[u8], pool: &Arc<WriteBufferPool>) -> Self {
        let (mut bytes, pooled) = pool.acquire(data.len());
        bytes.extend_from_slice(data);
        Self {
            bytes,
            offset: 0,
            pool: pooled.then(|| Arc::clone(pool)),
        }
    }

    pub(super) fn with_pooled_capacity(capacity: usize, pool: &Arc<WriteBufferPool>) -> Self {
        let (bytes, pooled) = pool.acquire(capacity);
        Self {
            bytes,
            offset: 0,
            pool: pooled.then(|| Arc::clone(pool)),
        }
    }

    pub(super) fn extend_from_slice(&mut self, data: &[u8]) {
        self.bytes.extend_from_slice(data);
    }

    pub(super) fn try_append(&mut self, data: &[u8]) -> bool {
        if self.offset != 0
            || data.len()
                > super::tuning::DEFAULT_WRITE_BUFFER_HIGH_WATER.saturating_sub(self.bytes.len())
        {
            return false;
        }
        self.bytes.extend_from_slice(data);
        true
    }

    #[inline]
    #[cfg(test)]
    pub(super) fn from_vec(data: Vec<u8>) -> Self {
        Self {
            bytes: data,
            offset: 0,
            pool: None,
        }
    }

    #[inline]
    pub(super) fn remaining(&self) -> &[u8] {
        &self.bytes[self.offset..]
    }

    #[inline]
    pub(super) fn len(&self) -> usize {
        self.remaining().len()
    }

    #[inline]
    pub(super) fn advance(&mut self, written: usize) {
        self.offset += written;
    }

    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

impl Drop for OwnedWriteBuffer {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            pool.release(std::mem::take(&mut self.bytes));
        }
    }
}

/// Lazily populated storage for writes that cannot complete directly.
pub(super) struct WriteBufferPool {
    state: Mutex<WriteBufferPoolState>,
    #[cfg(test)]
    allocations: AtomicUsize,
    #[cfg(test)]
    fallbacks: AtomicUsize,
}

struct WriteBufferPoolState {
    buffers: Vec<Vec<u8>>,
    allocated: usize,
}

impl WriteBufferPool {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(WriteBufferPoolState {
                buffers: Vec::with_capacity(WRITE_BUFFER_POOL_LIMIT),
                allocated: 0,
            }),
            #[cfg(test)]
            allocations: AtomicUsize::new(0),
            #[cfg(test)]
            fallbacks: AtomicUsize::new(0),
        }
    }

    fn acquire(&self, capacity: usize) -> (Vec<u8>, bool) {
        let mut state = self.state.lock().expect("poisoned write buffer pool");
        let (mut buffer, pooled) = if let Some(buffer) = state.buffers.pop() {
            (buffer, true)
        } else if state.allocated < WRITE_BUFFER_POOL_LIMIT {
            state.allocated += 1;
            #[cfg(test)]
            self.allocations.fetch_add(1, Ordering::Relaxed);
            (
                Vec::with_capacity(capacity.max(WRITE_BUFFER_BLOCK_SIZE)),
                true,
            )
        } else {
            #[cfg(test)]
            self.fallbacks.fetch_add(1, Ordering::Relaxed);
            (Vec::with_capacity(capacity), false)
        };
        drop(state);
        buffer.clear();
        if buffer.capacity() < capacity {
            #[cfg(test)]
            self.allocations.fetch_add(1, Ordering::Relaxed);
            buffer.reserve(capacity);
        }
        (buffer, pooled)
    }

    pub(super) fn release(&self, mut buffer: Vec<u8>) {
        let mut state = self.state.lock().expect("poisoned write buffer pool");
        if buffer.capacity() > super::tuning::DEFAULT_WRITE_BUFFER_HIGH_WATER {
            state.allocated = state.allocated.saturating_sub(1);
            return;
        }
        buffer.clear();
        if state.buffers.len() < WRITE_BUFFER_POOL_LIMIT {
            state.buffers.push(buffer);
        } else {
            state.allocated = state.allocated.saturating_sub(1);
        }
    }
}

/// Small transport-local pool shared by the runtime reader and Python-loop
/// delivery path. Framed protocols repeatedly exchange similarly sized
/// buffers; recycling avoids allocating a fresh Vec after every owned handoff.
pub(super) struct ReadBufferPool {
    state: Mutex<ReadBufferPoolState>,
    available: Condvar,
    async_available: AtomicWaker,
    closed: AtomicBool,
    #[cfg(test)]
    allocations: AtomicUsize,
}

struct ReadBufferPoolState {
    buffers: Vec<Vec<u8>>,
    allocated: usize,
}

impl ReadBufferPool {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(ReadBufferPoolState {
                buffers: Vec::with_capacity(READ_BUFFER_POOL_LIMIT),
                allocated: 0,
            }),
            available: Condvar::new(),
            async_available: AtomicWaker::new(),
            closed: AtomicBool::new(false),
            #[cfg(test)]
            allocations: AtomicUsize::new(0),
        }
    }

    pub(super) fn try_acquire(&self, capacity: usize) -> Option<Vec<u8>> {
        let mut state = self.state.lock().expect("poisoned stream read buffer pool");
        let mut buffer = if let Some(buffer) = state.buffers.pop() {
            buffer
        } else if state.allocated < READ_BUFFER_POOL_LIMIT {
            state.allocated += 1;
            #[cfg(test)]
            self.allocations.fetch_add(1, Ordering::Relaxed);
            Vec::with_capacity(capacity)
        } else {
            return None;
        };
        drop(state);
        buffer.clear();
        if buffer.capacity() < capacity {
            #[cfg(test)]
            self.allocations.fetch_add(1, Ordering::Relaxed);
            buffer.reserve(capacity);
        }
        Some(buffer)
    }

    fn has_available(&self) -> bool {
        let state = self.state.lock().expect("poisoned stream read buffer pool");
        !state.buffers.is_empty() || state.allocated < READ_BUFFER_POOL_LIMIT
    }

    pub(super) async fn wait_async(&self) {
        poll_fn(|context| {
            if self.closed.load(Ordering::Acquire) || self.has_available() {
                return std::task::Poll::Ready(());
            }
            self.async_available.register(context.waker());
            if self.closed.load(Ordering::Acquire) || self.has_available() {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await;
    }

    pub(super) fn wait_timeout(&self, timeout: Duration) {
        let state = self.state.lock().expect("poisoned stream read buffer pool");
        if state.buffers.is_empty() && state.allocated >= READ_BUFFER_POOL_LIMIT {
            let _ = self
                .available
                .wait_timeout(state, timeout)
                .expect("poisoned stream read buffer pool");
        }
    }

    pub(super) fn notify_all(&self) {
        self.available.notify_all();
        self.async_available.wake();
    }

    pub(super) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify_all();
    }

    pub(super) fn release(&self, mut buffer: Vec<u8>) {
        let mut state = self.state.lock().expect("poisoned stream read buffer pool");
        if buffer.capacity() > MAX_STREAM_READ_BUFFER_SIZE {
            state.allocated = state.allocated.saturating_sub(1);
            drop(state);
            self.notify_all();
            return;
        }
        buffer.clear();
        if state.buffers.len() < READ_BUFFER_POOL_LIMIT {
            state.buffers.push(buffer);
        } else {
            state.allocated = state.allocated.saturating_sub(1);
        }
        drop(state);
        self.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};

    use super::{
        MAX_STREAM_READ_BUFFER_SIZE, OwnedReadBuffer, OwnedWriteBuffer, PendingReadBuffer,
        READ_BUFFER_POOL_LIMIT, ReadBufferPool, WRITE_BUFFER_POOL_LIMIT, WriteBufferPool,
    };

    #[test]
    fn owned_write_buffer_tracks_remaining_bytes() {
        let mut buffer = OwnedWriteBuffer::from_slice(b"abcdef");
        buffer.advance(2);
        assert_eq!(buffer.remaining(), b"cdef");
        assert!(!buffer.is_empty());
        buffer.advance(4);
        assert!(buffer.is_empty());
    }

    #[test]
    fn owned_write_buffer_preserves_invariants_across_all_split_points() {
        for len in 0..=128 {
            let data = (0_u8..=127).take(len).collect::<Vec<_>>();
            for split in 0..=len {
                let mut buffer = OwnedWriteBuffer::from_vec(data.clone());
                buffer.advance(split);
                assert_eq!(buffer.remaining(), &data[split..]);
                assert_eq!(buffer.is_empty(), split == len);

                buffer.advance(len - split);
                assert!(buffer.remaining().is_empty());
                assert!(buffer.is_empty());
            }
        }
    }

    #[test]
    fn pending_read_buffer_coalesces_and_returns_storage_home() {
        let home = Mutex::new(vec![1, 2]);
        let mut pending = PendingReadBuffer::new(&home);
        let input = vec![3, 4, 5];

        pending.extend(&input);

        assert_eq!(pending.len(), 3);
        assert_eq!(pending.as_slice(), &[3, 4, 5]);
        drop(pending);
        assert!(home.lock().expect("coalesce buffer").capacity() >= 3);
    }

    #[test]
    fn read_buffer_pool_clears_and_reuses_released_buffers() {
        let pool = ReadBufferPool::new();
        let mut buffer = pool.try_acquire(128).expect("initial buffer");
        buffer.extend_from_slice(b"stale data");
        let pointer = buffer.as_ptr();
        pool.release(buffer);

        let acquired = pool.try_acquire(64).expect("recycled buffer");

        assert!(acquired.is_empty());
        assert_eq!(acquired.as_ptr(), pointer);
        assert!(acquired.capacity() >= 128);
        pool.release(acquired);
        let allocation_count = pool.allocations.load(Ordering::Relaxed);
        let grown = pool.try_acquire(4096).expect("grown buffer");
        pool.release(grown);
        assert!(pool.allocations.load(Ordering::Relaxed) > allocation_count);
        let warmed_count = pool.allocations.load(Ordering::Relaxed);
        let warmed = pool.try_acquire(4096).expect("warmed buffer");
        pool.release(warmed);
        assert_eq!(pool.allocations.load(Ordering::Relaxed), warmed_count);
    }

    #[test]
    fn read_buffer_pool_enforces_count_and_capacity_limits() {
        let pool = ReadBufferPool::new();
        let mut held = Vec::new();
        for capacity in 1..=READ_BUFFER_POOL_LIMIT {
            held.push(pool.try_acquire(capacity * 32).expect("pool slot"));
        }
        for buffer in held {
            pool.release(buffer);
        }
        assert_eq!(
            pool.state.lock().expect("read buffer pool").buffers.len(),
            READ_BUFFER_POOL_LIMIT
        );

        let mut oversized = pool
            .try_acquire(MAX_STREAM_READ_BUFFER_SIZE)
            .expect("oversized pool slot");
        oversized.resize(MAX_STREAM_READ_BUFFER_SIZE, 0);
        oversized.reserve(1);
        pool.release(oversized);
        let state = pool.state.lock().expect("read buffer pool");
        assert_eq!(state.buffers.len(), READ_BUFFER_POOL_LIMIT - 1);
        assert_eq!(state.allocated, READ_BUFFER_POOL_LIMIT - 1);
    }

    #[test]
    fn read_buffer_pool_stops_allocating_at_the_slot_limit() {
        let pool = ReadBufferPool::new();
        let held = (0..READ_BUFFER_POOL_LIMIT)
            .map(|_| pool.try_acquire(64).expect("pool slot"))
            .collect::<Vec<_>>();

        assert!(pool.try_acquire(64).is_none());
        pool.release(held.into_iter().next().expect("held buffer"));
        assert!(pool.try_acquire(64).is_some());
        assert_eq!(
            pool.allocations.load(Ordering::Relaxed),
            READ_BUFFER_POOL_LIMIT
        );
    }

    #[test]
    fn read_buffer_release_makes_a_slot_available() {
        let pool = ReadBufferPool::new();
        let held = (0..READ_BUFFER_POOL_LIMIT)
            .map(|_| pool.try_acquire(64).expect("pool slot"))
            .collect::<Vec<_>>();
        assert!(!pool.has_available());

        pool.release(held.into_iter().next().expect("held buffer"));

        assert!(pool.has_available());
    }

    #[test]
    fn owned_read_buffer_returns_its_allocation_to_the_pool() {
        let pool = Arc::new(ReadBufferPool::new());
        let mut bytes = pool.try_acquire(128).expect("pool slot");
        bytes.extend_from_slice(b"framed message");
        let pointer = bytes.as_ptr();

        let owned = OwnedReadBuffer::from_pooled(bytes, &pool);
        assert_eq!(owned.as_slice(), b"framed message");
        drop(owned);

        let reused = pool.try_acquire(64).expect("returned pool slot");
        assert_eq!(reused.as_ptr(), pointer);
        pool.release(reused);
    }

    #[test]
    fn write_buffer_pool_reuses_storage_and_bounds_normal_allocations() {
        let pool = Arc::new(WriteBufferPool::new());
        let first = OwnedWriteBuffer::from_pooled_slice(b"first", &pool);
        let pointer = first.bytes.as_ptr();
        drop(first);

        let reused = OwnedWriteBuffer::from_pooled_slice(b"second", &pool);
        assert_eq!(reused.bytes.as_ptr(), pointer);
        assert_eq!(pool.allocations.load(Ordering::Relaxed), 1);
        drop(reused);

        let held = (0..WRITE_BUFFER_POOL_LIMIT)
            .map(|_| OwnedWriteBuffer::from_pooled_slice(b"held", &pool))
            .collect::<Vec<_>>();
        let fallback = OwnedWriteBuffer::from_pooled_slice(b"fallback", &pool);
        assert!(fallback.pool.is_none());
        assert_eq!(pool.fallbacks.load(Ordering::Relaxed), 1);
        drop(held);
    }
}
