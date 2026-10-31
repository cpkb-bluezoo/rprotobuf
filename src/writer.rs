use std::collections::VecDeque;
use std::io;

use loopbuf::LoopBuf;

use crate::error::{WriteError, WriteResult};
use crate::wire::{WIRETYPE_I32, WIRETYPE_I64, WIRETYPE_LEN, WIRETYPE_VARINT};

/// Protobuf binary encoder that writes straight into a caller-owned [`LoopBuf`].
///
/// The writer keeps no copy of the output. Every `write_*` method takes the
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
/// # Embedded messages
///
/// The length of an embedded message goes before its content but is known only
/// after it, so it is not written into the ring at all. The ring holds the
/// message's content; the writer remembers each length prefix and where it
/// goes, and [`Self::drain_to`] writes the prefix to the sink and then the
/// content from the ring, in order. The prefix is always the shortest varint,
/// nothing is moved, and a prefix takes no room in the ring.
///
/// So a ring that has had embedded messages written to it is drained with
/// [`Self::drain_to`], not read directly: [`Self::has_pending_prefixes`] says
/// whether it matters. A ring that has had only scalar and bytes fields
/// written to it holds exactly the output, and can be read directly.
///
/// ```
/// use loopbuf::LoopBuf;
/// use rprotobuf::Writer;
///
/// let mut ring = LoopBuf::with_capacity(4096);
/// let mut w = Writer::new();
/// w.write_varint_field(&mut ring, 1, 150).unwrap();
/// w.write_message_field(&mut ring, 2, |w, ring| w.write_string_field(ring, 1, "hi")).unwrap();
///
/// let mut out = Vec::new();
/// w.drain_to(&mut ring, &mut out).unwrap();
/// assert_eq!(out, [0x08, 0x96, 0x01, 0x12, 0x04, 0x0A, 0x02, b'h', b'i']);
/// ```
#[derive(Debug, Default)]
pub struct Writer {
    bytes_written: u64,
    /// Length prefixes of embedded messages, in the order they go, each with
    /// where in the stream of ring bytes it is to be put.
    prefixes: VecDeque<Prefix>,
    /// How much of the prefix at the front has already gone to a sink.
    prefix_done: usize,
    /// The position, in the stream of every byte the writer has put in the
    /// ring, of the ring's first readable byte.
    stream_pos: u64,
    /// The ring's length when the writer last looked, to tell how much the
    /// caller has consumed since.
    last_len: usize,
}

/// A tag and length prefix, and the position in the ring's byte stream it
/// goes before.
#[derive(Debug)]
struct Prefix {
    at: u64,
    len: u8,
    bytes: [u8; 10],
}

impl Writer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bytes this writer has appended since it was created or reset.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Starts again, forgetting any length prefixes still waiting to be
    /// drained. The ring should be empty too.
    pub fn reset(&mut self) {
        self.bytes_written = 0;
        self.prefixes.clear();
        self.prefix_done = 0;
        self.stream_pos = 0;
        self.last_len = 0;
    }

    /// True if the length prefix of an embedded message is waiting to be put
    /// before bytes in the ring. The ring is then not the output: drain it with
    /// [`Self::drain_to`].
    pub fn has_pending_prefixes(&self) -> bool {
        !self.prefixes.is_empty()
    }

    /// Writes what is in the ring to `sink`, with each embedded message's
    /// length prefix in front of its content, and consumes it from the ring.
    /// Returns how many bytes went to the sink.
    ///
    /// Stops when the ring is empty or the sink will take no more. If the
    /// sink reports `WouldBlock` after some bytes went, those are counted and
    /// `Ok` returned; if none did, the error is. Either way nothing is lost:
    /// call again when the sink is ready.
    pub fn drain_to<W: io::Write>(&mut self, ring: &mut LoopBuf, sink: &mut W) -> io::Result<usize> {
        self.sync(ring).map_err(WriteError::into_io_error)?;
        let mut moved = 0;
        let outcome = loop {
            let next_prefix = self.prefixes.front().map(|p| p.at);
            if next_prefix == Some(self.stream_pos) {
                let prefix = self.prefixes.front().expect("a prefix at the front");
                let rest = &prefix.bytes[self.prefix_done..prefix.len as usize];
                match sink.write(rest) {
                    Ok(0) => break Err(io::ErrorKind::WriteZero.into()),
                    Ok(n) => {
                        moved += n;
                        self.prefix_done += n;
                        if self.prefix_done == prefix.len as usize {
                            self.prefixes.pop_front();
                            self.prefix_done = 0;
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => break Err(e),
                }
                continue;
            }
            // Ring bytes, up to the next prefix if there is one.
            let until_prefix = next_prefix.map_or(usize::MAX, |at| (at - self.stream_pos) as usize);
            let limit = ring.len().min(until_prefix);
            if limit == 0 {
                break Ok(());
            }
            let written = {
                let (first, second) = ring.readable_spans();
                let chunk = if first.len() >= limit { &first[..limit] } else if first.is_empty() { &second[..limit.min(second.len())] } else { first };
                sink.write(chunk)
            };
            match written {
                Ok(0) => break Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => {
                    ring.advance(n);
                    self.stream_pos += n as u64;
                    moved += n;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => break Err(e),
            }
        };
        self.last_len = ring.len();
        match outcome {
            Ok(()) => Ok(moved),
            Err(e) if moved > 0 && e.kind() == io::ErrorKind::WouldBlock => Ok(moved),
            Err(e) => Err(e),
        }
    }

    /// Notes what the caller has consumed from the ring since the writer last
    /// looked, for the positions of the prefixes still waiting. Consuming
    /// ring bytes that are already final is fine; consuming past a prefix
    /// without it is the caller reading the ring directly when it should have
    /// used [`Self::drain_to`].
    fn sync(&mut self, buf: &LoopBuf) -> WriteResult<()> {
        if buf.len() < self.last_len {
            self.stream_pos += (self.last_len - buf.len()) as u64;
        }
        self.last_len = buf.len();
        if self.prefixes.front().is_some_and(|p| p.at < self.stream_pos) {
            return Err(invalid_input(
                "the ring was read past an embedded message's length prefix: drain it with Writer::drain_to",
            ));
        }
        Ok(())
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
    /// (with the same writer and ring) straight into the ring.
    ///
    /// The tag and length prefix are not written into the ring: the writer
    /// notes them, and [`Self::drain_to`] puts them in front of the content as
    /// it drains the ring to a sink (see the type documentation). The prefix is
    /// the shortest varint, and takes no room in the ring. An empty message is
    /// a tag and a zero length, written into the ring as it is.
    ///
    /// The whole message is atomic: if `content` fails, for instance because
    /// the ring filled up, everything written since the field began is taken
    /// back and the error returned, so the caller can drain the ring and repeat
    /// the same call. This means the message's content must fit the ring as a
    /// whole, and `content` must only append to it, never consume from it.
    pub fn write_message_field<F>(
        &mut self,
        buf: &mut LoopBuf,
        field_number: u32,
        content: F,
    ) -> WriteResult<()>
    where
        F: FnOnce(&mut Writer, &mut LoopBuf) -> WriteResult<()>,
    {
        self.sync(buf)?;
        let start = buf.len();
        let at = self.stream_pos + start as u64;
        let prefix_mark = self.prefixes.len();
        let written_before = self.bytes_written;

        if let Err(e) = content(self, buf) {
            buf.truncate(start);
            self.prefixes.truncate(prefix_mark);
            self.bytes_written = written_before;
            self.last_len = buf.len();
            return Err(e);
        }

        // The message is what the content put in the ring and the prefixes of
        // the messages inside it, which are kept aside.
        let in_ring = buf
            .len()
            .checked_sub(start)
            .expect("the content callback must only append to the ring, not consume from it");
        let inner_prefixes: usize = self.prefixes.range(prefix_mark..).map(|p| p.len as usize).sum();
        let content_len = in_ring + inner_prefixes;
        if content_len > i32::MAX as usize {
            buf.truncate(start);
            self.prefixes.truncate(prefix_mark);
            self.bytes_written = written_before;
            self.last_len = buf.len();
            return Err(invalid_input("embedded message is longer than a length prefix can express"));
        }

        let (tag, tag_len) = tag_bytes(field_number, WIRETYPE_LEN);
        let (length, length_len) = varint_bytes(content_len as u64);
        let prefix_len = tag_len + length_len;
        if in_ring == 0 {
            // Nothing for a prefix to go before: it is the whole field.
            if let Err(e) = ensure_room(buf, prefix_len, "embedded message") {
                self.last_len = buf.len();
                return Err(e);
            }
            buf.extend_from_slice(&tag[..tag_len]);
            buf.extend_from_slice(&length[..length_len]);
        } else {
            let mut bytes = [0u8; 10];
            bytes[..tag_len].copy_from_slice(&tag[..tag_len]);
            bytes[tag_len..prefix_len].copy_from_slice(&length[..length_len]);
            // Before the prefixes of the messages inside this one, which were
            // added after `prefix_mark` and come later in the stream.
            self.prefixes.insert(prefix_mark, Prefix { at, len: prefix_len as u8, bytes });
        }
        self.bytes_written += prefix_len as u64;
        self.last_len = buf.len();
        Ok(())
    }

    /// Number of bytes a varint takes.
    pub fn varint_size(value: u64) -> usize {
        varint_bytes(value).1
    }

    /// Appends `first` then `second` as one atomic write.
    fn put(&mut self, buf: &mut LoopBuf, first: &[u8], second: &[u8], what: &str) -> WriteResult<()> {
        self.sync(buf)?;
        ensure_room(buf, first.len() + second.len(), what)?;
        buf.extend_from_slice(first);
        buf.extend_from_slice(second);
        self.bytes_written += (first.len() + second.len()) as u64;
        self.last_len = buf.len();
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
        self.sync(buf)?;
        ensure_room(buf, need, what)?;
        buf.extend_from_slice(&tag[..tag_len]);
        buf.extend_from_slice(&value[..value_len]);
        buf.extend_from_slice(payload);
        self.bytes_written += need as u64;
        self.last_len = buf.len();
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

