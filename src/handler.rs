//! Handler trait and default implementation.

use loopbuf::LoopBuf;

use crate::error::ParseResult;

/// Callback interface for push-based protobuf parsing.
///
/// The parser invokes these methods as fields are encountered. Schema
/// knowledge lives in the handler implementation. Every callback may return an
/// error, which stops the parse and is returned by [`crate::Parser::receive`].
pub trait Handler {
    /// Varint field (int32, int64, uint32, uint64, bool, enum; apply zigzag for sint*).
    fn handle_varint(&mut self, _field_number: u32, _value: u64) -> ParseResult<()> {
        Ok(())
    }

    /// Fixed 64-bit field (fixed64, sfixed64, double via `decode_helpers::as_double`).
    fn handle_fixed64(&mut self, _field_number: u32, _value: u64) -> ParseResult<()> {
        Ok(())
    }

    /// Fixed 32-bit field (fixed32, sfixed32, float via `decode_helpers::as_float`).
    fn handle_fixed32(&mut self, _field_number: u32, _value: u32) -> ParseResult<()> {
        Ok(())
    }

    /// The start of a length-delimited field that is not an embedded message
    /// (bytes, a string, a packed field). `length` is its whole payload, which
    /// follows as any number of [`Self::bytes_content`] calls and then
    /// [`Self::end_bytes`]. A payload of any size streams through a ring of any
    /// size, so nothing here has to fit anywhere.
    fn start_bytes(&mut self, _field_number: u32, _length: u64) -> ParseResult<()> {
        Ok(())
    }

    /// A piece of the payload of the field most recently started by
    /// [`Self::start_bytes`], as a window onto the parser's own ring: no copy
    /// was made, even if it straddles the ring's wrap. Read it with
    /// [`LoopBuf::readable_spans`], or hand it straight to another parser's
    /// `receive` when the field holds data in a format that has one.
    ///
    /// **What the handler advances past is consumed; what it leaves is offered
    /// again, at the front of the next piece.** So a handler that copies the
    /// piece must also consume it (see [`decode_helpers::BytesCollector`]), and
    /// one that feeds another parser leaves that parser's unfinished token. The
    /// default consumes everything. A handler that leaves every byte of a piece
    /// when the ring cannot take more is an error. Whatever it leaves of the
    /// last piece is dropped.
    ///
    /// The window is valid only for the duration of the call.
    fn bytes_content(&mut self, data: &mut LoopBuf) -> ParseResult<()> {
        data.advance(data.len());
        Ok(())
    }

    /// The payload of the field started by [`Self::start_bytes`] is complete.
    fn end_bytes(&mut self) -> ParseResult<()> {
        Ok(())
    }

    /// Return true if a length-delimited field is an embedded message.
    fn is_message(&self, _field_number: u32) -> bool {
        false
    }

    /// Begin parsing an embedded message.
    fn start_message(&mut self, _field_number: u32) -> ParseResult<()> {
        Ok(())
    }

    /// End parsing an embedded message.
    fn end_message(&mut self) -> ParseResult<()> {
        Ok(())
    }
}

macro_rules! forward_handler {
    ($target:ty) => {
        impl<H: Handler + ?Sized> Handler for $target {
            fn handle_varint(&mut self, field_number: u32, value: u64) -> ParseResult<()> {
                (**self).handle_varint(field_number, value)
            }
            fn handle_fixed64(&mut self, field_number: u32, value: u64) -> ParseResult<()> {
                (**self).handle_fixed64(field_number, value)
            }
            fn handle_fixed32(&mut self, field_number: u32, value: u32) -> ParseResult<()> {
                (**self).handle_fixed32(field_number, value)
            }
            fn start_bytes(&mut self, field_number: u32, length: u64) -> ParseResult<()> {
                (**self).start_bytes(field_number, length)
            }
            fn bytes_content(&mut self, data: &mut LoopBuf) -> ParseResult<()> {
                (**self).bytes_content(data)
            }
            fn end_bytes(&mut self) -> ParseResult<()> {
                (**self).end_bytes()
            }
            fn is_message(&self, field_number: u32) -> bool {
                (**self).is_message(field_number)
            }
            fn start_message(&mut self, field_number: u32) -> ParseResult<()> {
                (**self).start_message(field_number)
            }
            fn end_message(&mut self) -> ParseResult<()> {
                (**self).end_message()
            }
        }
    };
}

// A borrowed handler drives the parser like an owned one: `Parser::new(&mut handler)`.
forward_handler!(&mut H);
// So does a boxed one, `Box<dyn Handler>` included.
forward_handler!(Box<H>);

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

    /// A piece of a bytes field as a `String`, copying it out of the ring
    /// without consuming it.
    pub fn as_string(data: &LoopBuf) -> Result<String, std::string::FromUtf8Error> {
        String::from_utf8(as_bytes(data))
    }

    /// A piece of a bytes field as a `Vec`, copying it out of the ring without
    /// consuming it.
    pub fn as_bytes(data: &LoopBuf) -> Vec<u8> {
        let (a, b) = data.readable_spans();
        let mut bytes = Vec::with_capacity(a.len() + b.len());
        bytes.extend_from_slice(a);
        bytes.extend_from_slice(b);
        bytes
    }

    /// Gathers a bytes field that arrives in pieces, for a handler that wants
    /// the whole of it (a name, say) and knows it is small enough to hold.
    ///
    /// ```
    /// # use rprotobuf::{Handler, ParseResult, decode_helpers::BytesCollector};
    /// # use loopbuf::LoopBuf;
    /// #[derive(Default)]
    /// struct Names { collector: BytesCollector, names: Vec<String> }
    /// impl Handler for Names {
    ///     fn start_bytes(&mut self, _field: u32, length: u64) -> ParseResult<()> {
    ///         self.collector.start(length);
    ///         Ok(())
    ///     }
    ///     fn bytes_content(&mut self, data: &mut LoopBuf) -> ParseResult<()> {
    ///         self.collector.push(data);
    ///         Ok(())
    ///     }
    ///     fn end_bytes(&mut self) -> ParseResult<()> {
    ///         let name = self.collector.take_string().map_err(|e| rprotobuf::ParseError::new(e.to_string()))?;
    ///         self.names.push(name);
    ///         Ok(())
    ///     }
    /// }
    /// ```
    #[derive(Debug, Default)]
    pub struct BytesCollector {
        bytes: Vec<u8>,
    }

    impl BytesCollector {
        /// Starts a field of `length` bytes, forgetting the last one. Reserves
        /// room for it, but never more than 64 KiB on the strength of a
        /// length that has not yet been backed by data.
        pub fn start(&mut self, length: u64) {
            self.bytes.clear();
            self.bytes.reserve(length.min(64 * 1024) as usize);
        }

        /// Copies a piece out of the ring and consumes it.
        pub fn push(&mut self, data: &mut LoopBuf) {
            let (a, b) = data.readable_spans();
            self.bytes.extend_from_slice(a);
            self.bytes.extend_from_slice(b);
            data.advance(data.len());
        }

        /// What has been gathered so far.
        pub fn bytes(&self) -> &[u8] {
            &self.bytes
        }

        /// Takes what has been gathered, leaving the collector empty.
        pub fn take(&mut self) -> Vec<u8> {
            std::mem::take(&mut self.bytes)
        }

        /// Takes what has been gathered as a `String`.
        pub fn take_string(&mut self) -> Result<String, std::string::FromUtf8Error> {
            String::from_utf8(self.take())
        }
    }
}
