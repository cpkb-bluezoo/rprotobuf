use std::io;

use loopbuf::LoopBuf;

use crate::error::{WriteError, WriteResult};
use crate::wire::{WIRETYPE_I32, WIRETYPE_I64, WIRETYPE_LEN, WIRETYPE_VARINT};

/// Protobuf binary encoder that writes straight into a caller-owned [`LoopBuf`].
///
/// The writer keeps no buffer of its own. Every `write_*` method takes the
/// caller's ring and is **atomic**: it appends the whole field (tag and value
/// together) or appends nothing and returns an error.
///
/// - [`WriteError::is_would_block`] means the ring has no room for it *right
///   now*. Drain the ring (to a socket, say) and repeat the same call.
/// - [`std::io::ErrorKind::InvalidInput`] means it can never succeed as it
///   stands: the field is larger than the ring's whole capacity. A bytes or
///   string field is written whole, as the parser reads it whole, so the ring
///   must be larger than the largest.
///
/// ```
/// use loopbuf::LoopBuf;
/// use rprotobuf::Writer;
///
/// let mut ring = LoopBuf::with_capacity(4096);
/// let mut w = Writer::new();
/// w.write_varint_field(&mut ring, 1, 150).unwrap();
/// assert_eq!(ring.spans().to_vec(), [0x08, 0x96, 0x01]);
/// ```
#[derive(Debug, Default)]
pub struct Writer {
    bytes_written: u64,
}

impl Writer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bytes this writer has appended since it was created or reset.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub fn reset(&mut self) {
        self.bytes_written = 0;
    }

    pub fn write_tag(&mut self, buf: &mut LoopBuf, field_number: u32, wire_type: u8) -> WriteResult<()> {
        let (scratch, len) = tag_bytes(field_number, wire_type);
        self.put(buf, &scratch[..len], &[], "tag")
    }

    pub fn write_varint(&mut self, buf: &mut LoopBuf, value: u64) -> WriteResult<()> {
        let (scratch, len) = varint_bytes(value);
        self.put(buf, &scratch[..len], &[], "varint")
    }

    pub fn write_svarint(&mut self, buf: &mut LoopBuf, value: i64) -> WriteResult<()> {
        self.write_varint(buf, zigzag64(value))
    }

    pub fn write_svarint32(&mut self, buf: &mut LoopBuf, value: i32) -> WriteResult<()> {
        self.write_varint(buf, zigzag32(value))
    }

    pub fn write_fixed64(&mut self, buf: &mut LoopBuf, value: u64) -> WriteResult<()> {
        self.put(buf, &value.to_le_bytes(), &[], "fixed64")
    }

    pub fn write_fixed32(&mut self, buf: &mut LoopBuf, value: u32) -> WriteResult<()> {
        self.put(buf, &value.to_le_bytes(), &[], "fixed32")
    }

    pub fn write_double(&mut self, buf: &mut LoopBuf, value: f64) -> WriteResult<()> {
        self.write_fixed64(buf, value.to_bits())
    }

    pub fn write_float(&mut self, buf: &mut LoopBuf, value: f32) -> WriteResult<()> {
        self.write_fixed32(buf, value.to_bits())
    }

    pub fn write_varint_field(&mut self, buf: &mut LoopBuf, field_number: u32, value: u64) -> WriteResult<()> {
        self.put_field(buf, field_number, WIRETYPE_VARINT, varint_bytes(value), &[])
    }

    pub fn write_svarint_field(&mut self, buf: &mut LoopBuf, field_number: u32, value: i64) -> WriteResult<()> {
        self.write_varint_field(buf, field_number, zigzag64(value))
    }

    pub fn write_bool_field(&mut self, buf: &mut LoopBuf, field_number: u32, value: bool) -> WriteResult<()> {
        self.write_varint_field(buf, field_number, u64::from(value))
    }

    pub fn write_fixed64_field(&mut self, buf: &mut LoopBuf, field_number: u32, value: u64) -> WriteResult<()> {
        let mut scratch = [0u8; 10];
        scratch[..8].copy_from_slice(&value.to_le_bytes());
        self.put_field(buf, field_number, WIRETYPE_I64, (scratch, 8), &[])
    }

    pub fn write_fixed32_field(&mut self, buf: &mut LoopBuf, field_number: u32, value: u32) -> WriteResult<()> {
        let mut scratch = [0u8; 10];
        scratch[..4].copy_from_slice(&value.to_le_bytes());
        self.put_field(buf, field_number, WIRETYPE_I32, (scratch, 4), &[])
    }

    pub fn write_double_field(&mut self, buf: &mut LoopBuf, field_number: u32, value: f64) -> WriteResult<()> {
        self.write_fixed64_field(buf, field_number, value.to_bits())
    }

    pub fn write_float_field(&mut self, buf: &mut LoopBuf, field_number: u32, value: f32) -> WriteResult<()> {
        self.write_fixed32_field(buf, field_number, value.to_bits())
    }

    /// Writes a bytes field. The whole field must fit the ring.
    pub fn write_bytes_field(&mut self, buf: &mut LoopBuf, field_number: u32, data: &[u8]) -> WriteResult<()> {
        self.put_field(
            buf,
            field_number,
            WIRETYPE_LEN,
            varint_bytes(data.len() as u64),
            data,
        )
    }

    pub fn write_string_field(&mut self, buf: &mut LoopBuf, field_number: u32, value: &str) -> WriteResult<()> {
        self.write_bytes_field(buf, field_number, value.as_bytes())
    }

    /// Writes an already-encoded embedded message with its length prefix.
    /// Writes nothing if it is empty.
    pub fn write_encoded_message_field(
        &mut self,
        buf: &mut LoopBuf,
        field_number: u32,
        encoded_message: &[u8],
    ) -> WriteResult<()> {
        if encoded_message.is_empty() {
            return Ok(());
        }
        self.write_bytes_field(buf, field_number, encoded_message)
    }

    /// Writes an embedded message field. `content` writes the message's fields
    /// (with the same writer and ring) straight into the ring, after the tag
    /// and a reserved length; once it returns, the length is filled in. There
    /// is no scratch buffer and no copy.
    ///
    /// The whole message is atomic: if `content` fails, for instance because
    /// the ring filled up, everything written since the field began is taken
    /// back and the error returned, so the caller can drain the ring and repeat
    /// the same call. This means the message must fit the ring as a whole, and
    /// `content` must only append to it, never consume from it.
    ///
    /// The length is the shortest varint, as any other encoder would write it,
    /// though it is not known until the content has been. One byte is reserved
    /// for it, which is all a message under 128 bytes needs. A longer one has
    /// its content moved on within the ring by the extra bytes the length
    /// takes (a memmove of just that message, once, however deeply it is
    /// nested), so the ring needs room for those too.
    pub fn write_message_field<F>(
        &mut self,
        buf: &mut LoopBuf,
        field_number: u32,
        content: F,
    ) -> WriteResult<()>
    where
        F: FnOnce(&mut Writer, &mut LoopBuf) -> WriteResult<()>,
    {
        let (tag, tag_len) = tag_bytes(field_number, WIRETYPE_LEN);
        // The tag and a one byte length, which is widened below if need be.
        let header_len = tag_len + 1;
        ensure_room(buf, header_len, "embedded message")?;

        let start = buf.len();
        let written_before = self.bytes_written;
        buf.extend_from_slice(&tag[..tag_len]);
        buf.extend_from_slice(&[0x00]);
        self.bytes_written += header_len as u64;

        if let Err(e) = content(self, buf) {
            buf.truncate(start);
            self.bytes_written = written_before;
            return Err(e);
        }

        let content_start = start + header_len;
        let content_len = buf
            .len()
            .checked_sub(content_start)
            .expect("the content callback must only append to the ring, not consume from it");
        if content_len > i32::MAX as usize {
            buf.truncate(start);
            self.bytes_written = written_before;
            return Err(invalid_input("embedded message is longer than a length prefix can express"));
        }
        let (length, length_len) = varint_bytes(content_len as u64);
        if length_len > 1 {
            // Open a gap for the rest of the length. If the ring has no room
            // for it the message as a whole does not fit (yet).
            let extra = length_len - 1;
            if buf.spare_len() < extra {
                let total = tag_len + length_len + content_len;
                buf.truncate(start);
                self.bytes_written = written_before;
                return Err(not_enough_room(buf, total, "embedded message"));
            }
            buf.extend_from_slice(&[0u8; 4][..extra]);
            buf.copy_within(content_start..content_start + content_len, content_start + extra);
            self.bytes_written += extra as u64;
        }
        buf.overwrite(start + tag_len, &length[..length_len]);
        Ok(())
    }

    /// Number of bytes a varint takes.
    pub fn varint_size(value: u64) -> usize {
        varint_bytes(value).1
    }

    /// Appends `first` then `second` as one atomic write.
    fn put(&mut self, buf: &mut LoopBuf, first: &[u8], second: &[u8], what: &str) -> WriteResult<()> {
        ensure_room(buf, first.len() + second.len(), what)?;
        buf.extend_from_slice(first);
        buf.extend_from_slice(second);
        self.bytes_written += (first.len() + second.len()) as u64;
        Ok(())
    }

    /// Appends a tag, then `value` (already encoded), then `payload`, as one
    /// atomic write.
    fn put_field(
        &mut self,
        buf: &mut LoopBuf,
        field_number: u32,
        wire_type: u8,
        value: ([u8; 10], usize),
        payload: &[u8],
    ) -> WriteResult<()> {
        let (tag, tag_len) = tag_bytes(field_number, wire_type);
        let (value, value_len) = value;
        let need = tag_len + value_len + payload.len();
        let what = if wire_type == WIRETYPE_LEN { "bytes field" } else { "field" };
        ensure_room(buf, need, what)?;
        buf.extend_from_slice(&tag[..tag_len]);
        buf.extend_from_slice(&value[..value_len]);
        buf.extend_from_slice(payload);
        self.bytes_written += need as u64;
        Ok(())
    }
}

fn invalid_input(message: &str) -> WriteError {
    WriteError::new(io::Error::new(io::ErrorKind::InvalidInput, message.to_owned()))
}

/// Succeeds if `need` bytes can be appended now. Otherwise nothing has been
/// done: `WouldBlock` if draining the ring would make room, `InvalidInput` if
/// the ring's whole capacity is too small and it never will.
fn ensure_room(buf: &LoopBuf, need: usize, what: &str) -> WriteResult<()> {
    if need <= buf.spare_len() {
        return Ok(());
    }
    Err(not_enough_room(buf, need, what))
}

/// The error for a write that needs `need` bytes and found too few: it can
/// never succeed if the ring is too small for it, otherwise it can once the
/// ring has been drained.
fn not_enough_room(buf: &LoopBuf, need: usize, what: &str) -> WriteError {
    if need > buf.capacity() {
        return WriteError::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{what} needs {need} bytes but the LoopBuf's whole capacity is {}",
                buf.capacity()
            ),
        ));
    }
    WriteError::new(io::Error::new(
        io::ErrorKind::WouldBlock,
        "LoopBuf has no room for the whole write: drain it and repeat the call",
    ))
}

fn zigzag64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn zigzag32(value: i32) -> u64 {
    ((value << 1) ^ (value >> 31)) as u32 as u64
}

/// The varint encoding of `value` and its length (at most 10 bytes).
fn varint_bytes(mut value: u64) -> ([u8; 10], usize) {
    let mut out = [0u8; 10];
    let mut len = 0;
    loop {
        if (value & !0x7F) == 0 {
            out[len] = value as u8;
            return (out, len + 1);
        }
        out[len] = ((value & 0x7F) as u8) | 0x80;
        len += 1;
        value >>= 7;
    }
}

fn tag_bytes(field_number: u32, wire_type: u8) -> ([u8; 10], usize) {
    varint_bytes(u64::from((field_number << 3) | u32::from(wire_type)))
}

