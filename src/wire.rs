//! Protobuf wire type constants.

/// Protobuf wire types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WireType {
    Varint = 0,
    I64 = 1,
    Len = 2,
    I32 = 5,
}

impl WireType {
    pub fn from_tag(tag: u32) -> Option<Self> {
        match tag & 0x07 {
            0 => Some(Self::Varint),
            1 => Some(Self::I64),
            2 => Some(Self::Len),
            5 => Some(Self::I32),
            _ => None,
        }
    }
}

/// Wire type for variable-length integers.
pub const WIRETYPE_VARINT: u8 = 0;
/// Wire type for 64-bit fixed values.
pub const WIRETYPE_I64: u8 = 1;
/// Wire type for length-delimited values.
pub const WIRETYPE_LEN: u8 = 2;
/// Wire type for 32-bit fixed values.
pub const WIRETYPE_I32: u8 = 5;
