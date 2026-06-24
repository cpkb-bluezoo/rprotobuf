//! Handler trait and default implementation.

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
    fn handle_bytes(&mut self, _field_number: u32, _data: &[u8]) {}

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

    pub fn as_string(data: &[u8]) -> Result<String, std::string::FromUtf8Error> {
        String::from_utf8(data.to_vec())
    }

    pub fn as_bytes(data: &[u8]) -> Vec<u8> {
        data.to_vec()
    }
}
