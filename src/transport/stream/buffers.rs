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
    use super::OwnedWriteBuffer;

    #[test]
    fn owned_write_buffer_tracks_remaining_bytes() {
        let mut buffer = OwnedWriteBuffer::from_slice(b"abcdef");
        buffer.advance(2);
        assert_eq!(buffer.remaining(), b"cdef");
        assert!(!buffer.is_empty());
        buffer.advance(4);
        assert!(buffer.is_empty());
    }
}
