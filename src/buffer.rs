use std::io::{self, Write};

/// Expandable in-memory buffer (gumdrop `ByteBufferChannel`).
#[derive(Debug, Default)]
pub struct Buffer {
    inner: Vec<u8>,
}

impl Buffer {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Vec::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.inner
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.inner
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
