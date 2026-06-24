use crate::error::{ParseError, ParseResult};
use crate::handler::Handler;
use crate::wire::{WIRETYPE_I32, WIRETYPE_I64, WIRETYPE_LEN, WIRETYPE_VARINT};

/// Push-based protobuf parser.
pub struct Parser<'a, H: Handler + ?Sized> {
    handler: &'a mut H,
    message_stack: Vec<i64>,
    underflow: bool,
}

impl<'a, H: Handler + ?Sized> Parser<'a, H> {
    pub fn new(handler: &'a mut H) -> Self {
        Self {
            handler,
            message_stack: Vec::new(),
            underflow: false,
        }
    }

    pub fn is_underflow(&self) -> bool {
        self.underflow
    }

    /// Parse as many complete fields as possible from the front of `data`.
    ///
    /// Advances `data` by the number of bytes consumed. On underflow, parsing
    /// stops before the incomplete field and `is_underflow()` returns true.
    pub fn receive(&mut self, data: &mut &[u8]) -> ParseResult<()> {
        self.underflow = false;

        while !data.is_empty() {
            while let Some(&remaining) = self.message_stack.last() {
                if remaining > 0 {
                    break;
                }
                self.message_stack.pop();
                self.handler.end_message();
            }

            if data.is_empty() {
                break;
            }

            let field_start = *data;

            let tag = match try_read_varint(data) {
                Ok(v) => v,
                Err(TryVarint::Underflow) => {
                    *data = field_start;
                    self.underflow = true;
                    return Ok(());
                }
                Err(TryVarint::Error(e)) => return Err(e),
            };

            let field_number = (tag >> 3) as u32;
            let wire_type = (tag & 0x07) as u8;

            if field_number == 0 {
                return Err(ParseError::new("invalid field number 0"));
            }

            let mut bytes_consumed;

            match wire_type {
                WIRETYPE_VARINT => {
                    match try_read_varint(data) {
                        Ok(value) => {
                            bytes_consumed = field_start.len() - data.len();
                            self.handler.handle_varint(field_number, value);
                        }
                        Err(TryVarint::Underflow) => {
                            *data = field_start;
                            self.underflow = true;
                            return Ok(());
                        }
                        Err(TryVarint::Error(e)) => return Err(e),
                    }
                }
                WIRETYPE_I64 => {
                    if data.len() < 8 {
                        *data = field_start;
                        self.underflow = true;
                        return Ok(());
                    }
                    let value = read_fixed64(data);
                    bytes_consumed = field_start.len() - data.len();
                    self.handler.handle_fixed64(field_number, value);
                }
                WIRETYPE_I32 => {
                    if data.len() < 4 {
                        *data = field_start;
                        self.underflow = true;
                        return Ok(());
                    }
                    let value = read_fixed32(data);
                    bytes_consumed = field_start.len() - data.len();
                    self.handler.handle_fixed32(field_number, value);
                }
                WIRETYPE_LEN => {
                    let length_value = match try_read_varint(data) {
                        Ok(v) => v,
                        Err(TryVarint::Underflow) => {
                            *data = field_start;
                            self.underflow = true;
                            return Ok(());
                        }
                        Err(TryVarint::Error(e)) => return Err(e),
                    };

                    let length = length_value as i64;
                    if length < 0 {
                        return Err(ParseError::new(format!(
                            "negative length-delimited field size: {length}"
                        )));
                    }
                    let length = length as usize;

                    if self.handler.is_message(field_number) {
                        bytes_consumed = field_start.len() - data.len();
                        self.decrement_message_bytes(bytes_consumed);
                        bytes_consumed = 0;
                        self.handler.start_message(field_number);
                        self.message_stack.push(length as i64);
                    } else {
                        if data.len() < length {
                            *data = field_start;
                            self.underflow = true;
                            return Ok(());
                        }
                        let content = &data[..length];
                        *data = &data[length..];
                        bytes_consumed = field_start.len() - data.len();
                        self.handler.handle_bytes(field_number, content);
                    }
                }
                other => {
                    return Err(ParseError::new(format!("unknown wire type: {other}")));
                }
            }

            self.decrement_message_bytes(bytes_consumed);
        }

        while let Some(&remaining) = self.message_stack.last() {
            if remaining > 0 {
                break;
            }
            self.message_stack.pop();
            self.handler.end_message();
        }

        Ok(())
    }

    pub fn close(&self) -> ParseResult<()> {
        if self.underflow {
            return Err(ParseError::new("incomplete field at end of input"));
        }
        if !self.message_stack.is_empty() {
            return Err(ParseError::new(format!(
                "unclosed embedded messages: {}",
                self.message_stack.len()
            )));
        }
        Ok(())
    }

    pub fn reset(&mut self) {
        self.message_stack.clear();
        self.underflow = false;
    }

    fn decrement_message_bytes(&mut self, count: usize) {
        if count == 0 || self.message_stack.is_empty() {
            return;
        }
        let count = count as i64;
        for level in &mut self.message_stack {
            *level -= count;
        }
    }
}

enum TryVarint {
    Underflow,
    Error(ParseError),
}

fn try_read_varint(data: &mut &[u8]) -> Result<u64, TryVarint> {
    let start = *data;
    let mut result = 0u64;
    let mut shift = 0u32;

    while !data.is_empty() {
        let b = data[0];
        *data = &data[1..];
        result |= u64::from(b & 0x7F) << shift;

        if (b & 0x80) == 0 {
            return Ok(result);
        }

        shift += 7;
        if shift >= 64 {
            return Err(TryVarint::Error(ParseError::new("varint too long")));
        }
    }

    *data = start;
    Err(TryVarint::Underflow)
}

fn read_fixed64(data: &mut &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&data[..8]);
    *data = &data[8..];
    u64::from_le_bytes(bytes)
}

fn read_fixed32(data: &mut &[u8]) -> u32 {
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&data[..4]);
    *data = &data[4..];
    u32::from_le_bytes(bytes)
}
