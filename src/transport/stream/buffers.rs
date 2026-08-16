//! Owned buffers shared by stream reader and writer paths.

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
