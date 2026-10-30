//! Handler trait and default implementation.

use loopbuf::LoopBuf;

/// Callback interface for push-based protobuf parsing.
///
/// The parser invokes these methods as fields are encountered. Schema
/// knowledge lives in the handler implementation.
pub trait Handler {
    /// Varint field (int32, int64, uint32, uint64, bool, enum; apply zigzag for sint*).
    fn handle_varint(&mut self, _field_number: u32, _value: u64) {}

    /// Fixed 64-bit field (fixed64, sfixed64, double via `decode_helpers::as_double`).
    fn handle_fixed64(&mut self, _field_number: u32, _value: u64) {}

    /// Fixed 32-bit field (fixed32, sfixed32, float via `decode_helpers::as_float`).
    fn handle_fixed32(&mut self, _field_number: u32, _value: u32) {}

    /// Length-delimited field that is not an embedded message (bytes, string, packed).
    ///
    /// `data` is a window onto the parser's own ring, holding exactly the
    /// field's payload: no copy was made. Read it with
    /// [`LoopBuf::readable_spans`] (the second span is empty unless the
    /// payload straddles the ring's wrap), or hand it straight to another
    /// parser's `receive` when the field holds an encoded message or other
    /// data in a format that has one. Whatever the handler advances past is
    /// not carried over, and nothing can be written to it.
    ///
    /// It is only valid for the duration of the call, so a handler that needs
    /// the content afterwards must copy it (see [`decode_helpers::as_bytes`]).
    fn handle_bytes(&mut self, _field_number: u32, _data: &mut LoopBuf) {}

    /// Return true if a length-delimited field is an embedded message.
    fn is_message(&self, _field_number: u32) -> bool {
        false
    }

    /// Begin parsing an embedded message.
    fn start_message(&mut self, _field_number: u32) {}

    /// End parsing an embedded message.
    fn end_message(&mut self) {}
}

/// No-op handler; override selected methods or use `decode_helpers`.
pub struct DefaultHandler;

impl Handler for DefaultHandler {}

/// Value interpretation helpers (mirror gumdrop `DefaultProtobufHandler`).
pub mod decode_helpers {
    use loopbuf::LoopBuf;

    pub fn as_bool(value: u64) -> bool {
        value != 0
    }

    pub fn as_sint32(value: u64) -> i32 {
        let n = value as u32;
        ((n >> 1) as i32) ^ (-((n & 1) as i32))
    }

    pub fn as_sint64(value: u64) -> i64 {
        ((value >> 1) as i64) ^ (-((value & 1) as i64))
    }

    pub fn as_double(value: u64) -> f64 {
        f64::from_bits(value)
    }

    pub fn as_float(value: u32) -> f32 {
        f32::from_bits(value)
    }

    /// The payload of a bytes field as a `String`, copying it out of the ring.
    pub fn as_string(data: &LoopBuf) -> Result<String, std::string::FromUtf8Error> {
        String::from_utf8(as_bytes(data))
    }

    /// The payload of a bytes field as a `Vec`, copying it out of the ring.
    pub fn as_bytes(data: &LoopBuf) -> Vec<u8> {
        let (a, b) = data.readable_spans();
        let mut bytes = Vec::with_capacity(a.len() + b.len());
        bytes.extend_from_slice(a);
        bytes.extend_from_slice(b);
        bytes
    }
}
