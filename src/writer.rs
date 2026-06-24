use crate::buffer::Buffer;
use crate::error::{WriteError, WriteResult};
use crate::wire::{WIRETYPE_I32, WIRETYPE_I64, WIRETYPE_LEN, WIRETYPE_VARINT};
use std::io::Write;

/// Zero-dependency protobuf binary encoder.
pub struct Writer<W: Write> {
    inner: W,
    scratch: [u8; 16],
    bytes_written: u64,
}

impl Writer<Buffer> {
    pub fn buffer(capacity: usize) -> Self {
        Self::new(Buffer::with_capacity(capacity))
    }

    pub fn finish(self) -> Vec<u8> {
        self.inner.into_vec()
    }
}

impl<W: Write> Writer<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            scratch: [0u8; 16],
            bytes_written: 0,
        }
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub fn into_inner(self) -> W {
        self.inner
    }

    fn write_all(&mut self, data: &[u8]) -> WriteResult<()> {
        self.inner.write_all(data).map_err(WriteError::new)?;
        self.bytes_written += data.len() as u64;
        Ok(())
    }

    pub fn write_tag(&mut self, field_number: u32, wire_type: u8) -> WriteResult<()> {
        self.write_varint(((field_number << 3) | u32::from(wire_type)) as u64)
    }

    pub fn write_varint(&mut self, mut value: u64) -> WriteResult<()> {
        let mut len = 0usize;
        loop {
            if (value & !0x7F) == 0 {
                self.scratch[len] = value as u8;
                len += 1;
                break;
            }
            self.scratch[len] = ((value & 0x7F) as u8) | 0x80;
            len += 1;
            value >>= 7;
        }
        let chunk = self.scratch[..len].to_vec();
        self.write_all(&chunk)
    }

    pub fn write_svarint(&mut self, value: i64) -> WriteResult<()> {
        let encoded = ((value << 1) ^ (value >> 63)) as u64;
        self.write_varint(encoded)
    }

    pub fn write_svarint32(&mut self, value: i32) -> WriteResult<()> {
        let encoded = ((value << 1) ^ (value >> 31)) as u32 as u64;
        self.write_varint(encoded)
    }

    pub fn write_fixed64(&mut self, value: u64) -> WriteResult<()> {
        let bytes = value.to_le_bytes();
        self.write_all(&bytes)
    }

    pub fn write_fixed32(&mut self, value: u32) -> WriteResult<()> {
        let bytes = value.to_le_bytes();
        self.write_all(&bytes)
    }

    pub fn write_double(&mut self, value: f64) -> WriteResult<()> {
        self.write_fixed64(value.to_bits())
    }

    pub fn write_float(&mut self, value: f32) -> WriteResult<()> {
        self.write_fixed32(value.to_bits())
    }

    pub fn write_varint_field(&mut self, field_number: u32, value: u64) -> WriteResult<()> {
        self.write_tag(field_number, WIRETYPE_VARINT)?;
        self.write_varint(value)
    }

    pub fn write_svarint_field(&mut self, field_number: u32, value: i64) -> WriteResult<()> {
        self.write_tag(field_number, WIRETYPE_VARINT)?;
        self.write_svarint(value)
    }

    pub fn write_bool_field(&mut self, field_number: u32, value: bool) -> WriteResult<()> {
        self.write_tag(field_number, WIRETYPE_VARINT)?;
        self.write_varint(u64::from(value))
    }

    pub fn write_fixed64_field(&mut self, field_number: u32, value: u64) -> WriteResult<()> {
        self.write_tag(field_number, WIRETYPE_I64)?;
        self.write_fixed64(value)
    }

    pub fn write_fixed32_field(&mut self, field_number: u32, value: u32) -> WriteResult<()> {
        self.write_tag(field_number, WIRETYPE_I32)?;
        self.write_fixed32(value)
    }

    pub fn write_double_field(&mut self, field_number: u32, value: f64) -> WriteResult<()> {
        self.write_tag(field_number, WIRETYPE_I64)?;
        self.write_double(value)
    }

    pub fn write_float_field(&mut self, field_number: u32, value: f32) -> WriteResult<()> {
        self.write_tag(field_number, WIRETYPE_I32)?;
        self.write_float(value)
    }

    pub fn write_bytes_field(&mut self, field_number: u32, data: &[u8]) -> WriteResult<()> {
        self.write_tag(field_number, WIRETYPE_LEN)?;
        self.write_varint(data.len() as u64)?;
        self.write_all(data)
    }

    pub fn write_string_field(&mut self, field_number: u32, value: &str) -> WriteResult<()> {
        self.write_bytes_field(field_number, value.as_bytes())
    }

    pub fn write_encoded_message_field(
        &mut self,
        field_number: u32,
        encoded_message: &[u8],
    ) -> WriteResult<()> {
        if encoded_message.is_empty() {
            return Ok(());
        }
        self.write_tag(field_number, WIRETYPE_LEN)?;
        self.write_varint(encoded_message.len() as u64)?;
        self.write_all(encoded_message)
    }

    pub fn write_message_field<F>(&mut self, field_number: u32, content: F) -> WriteResult<()>
    where
        F: FnOnce(&mut Writer<Buffer>) -> WriteResult<()>,
    {
        let mut temp = Writer::buffer(64 * 1024);
        content(&mut temp)?;
        let encoded = temp.finish();
        self.write_encoded_message_field(field_number, &encoded)
    }

    pub fn varint_size(mut value: u64) -> usize {
        let mut size = 0usize;
        loop {
            size += 1;
            if (value & !0x7F) == 0 {
                break;
            }
            value >>= 7;
        }
        size
    }
}
