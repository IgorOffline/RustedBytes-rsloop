//! Owned buffers shared by stream reader and writer paths.

use std::sync::Mutex;

use super::tuning::{MAX_STREAM_READ_BUFFER_SIZE, READ_BUFFER_POOL_LIMIT};

pub(super) struct OwnedWriteBuffer {
    bytes: Box<[u8]>,
    offset: usize,
}

pub(super) struct PendingReadBuffer(pub(super) Vec<u8>);

impl PendingReadBuffer {
    #[inline]
    pub(super) fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub(super) fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub(super) fn extend(&mut self, data: Vec<u8>) -> Vec<u8> {
        self.0.extend_from_slice(&data);
        data
    }
}

impl OwnedWriteBuffer {
    #[inline]
    pub(super) fn from_slice(data: &[u8]) -> Self {
        Self {
            bytes: Box::<[u8]>::from(data),
            offset: 0,
        }
    }

    #[inline]
    pub(super) fn from_vec(data: Vec<u8>) -> Self {
        Self {
            bytes: data.into_boxed_slice(),
            offset: 0,
        }
    }

    #[inline]
    pub(super) fn remaining(&self) -> &[u8] {
        &self.bytes[self.offset..]
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

/// Small transport-local pool shared by the runtime reader and Python-loop
/// delivery path. Framed protocols repeatedly exchange similarly sized
/// buffers; recycling avoids allocating a fresh Vec after every owned handoff.
pub(super) struct ReadBufferPool {
    buffers: Mutex<Vec<Vec<u8>>>,
}

impl ReadBufferPool {
    pub(super) fn new() -> Self {
        Self {
            buffers: Mutex::new(Vec::with_capacity(READ_BUFFER_POOL_LIMIT)),
        }
    }

    pub(super) fn acquire(&self, capacity: usize) -> Vec<u8> {
        let mut buffer = self
            .buffers
            .lock()
            .expect("poisoned stream read buffer pool")
            .pop()
            .unwrap_or_default();
        buffer.clear();
        if buffer.capacity() < capacity {
            buffer.reserve(capacity - buffer.capacity());
        }
        buffer
    }

    pub(super) fn release(&self, mut buffer: Vec<u8>) {
        if buffer.capacity() > MAX_STREAM_READ_BUFFER_SIZE {
            return;
        }
        buffer.clear();
        let mut buffers = self
            .buffers
            .lock()
            .expect("poisoned stream read buffer pool");
        if buffers.len() < READ_BUFFER_POOL_LIMIT {
            buffers.push(buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_STREAM_READ_BUFFER_SIZE, OwnedWriteBuffer, PendingReadBuffer, READ_BUFFER_POOL_LIMIT,
        ReadBufferPool,
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
    fn pending_read_buffer_coalesces_and_returns_recyclable_input() {
        let mut pending = PendingReadBuffer(vec![1, 2]);
        let input = vec![3, 4, 5];

        let recycled = pending.extend(input);

        assert_eq!(pending.len(), 5);
        assert_eq!(pending.as_slice(), &[1, 2, 3, 4, 5]);
        assert_eq!(recycled, vec![3, 4, 5]);
    }

    #[test]
    fn read_buffer_pool_clears_and_reuses_released_buffers() {
        let pool = ReadBufferPool::new();
        let mut buffer = Vec::with_capacity(128);
        buffer.extend_from_slice(b"stale data");
        let pointer = buffer.as_ptr();
        pool.release(buffer);

        let acquired = pool.acquire(64);

        assert!(acquired.is_empty());
        assert_eq!(acquired.as_ptr(), pointer);
        assert!(acquired.capacity() >= 128);
    }

    #[test]
    fn read_buffer_pool_enforces_count_and_capacity_limits() {
        let pool = ReadBufferPool::new();
        for capacity in [32, 64] {
            pool.release(Vec::with_capacity(capacity));
        }
        assert_eq!(
            pool.buffers.lock().expect("read buffer pool").len(),
            READ_BUFFER_POOL_LIMIT
        );

        let oversized = Vec::with_capacity(MAX_STREAM_READ_BUFFER_SIZE + 1);
        pool.acquire(0);
        pool.release(oversized);
        assert!(pool.buffers.lock().expect("read buffer pool").is_empty());
    }
}
