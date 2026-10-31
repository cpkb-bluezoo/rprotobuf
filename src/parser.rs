use loopbuf::{LoopBuf, Spans};

use crate::error::{ParseError, ParseResult};
use crate::handler::Handler;
use crate::wire::{WIRETYPE_I32, WIRETYPE_I64, WIRETYPE_LEN, WIRETYPE_VARINT};

/// Default maximum nested message depth for untrusted input.
pub const DEFAULT_MAX_MESSAGE_DEPTH: usize = 100;

/// No limit on declared length-delimited field sizes beyond the largest the
/// wire format allows here (`i32::MAX`).
pub const UNLIMITED_LENGTH_DELIMITED: usize = i32::MAX as usize;

/// Push-based protobuf parser.
///
/// # Buffering
///
/// [`Self::receive`] takes a caller-owned [`LoopBuf`] by reference: the same
/// ring across every call. The parser consumes whole fields and holds none of
/// the input itself. A field it cannot complete yet is left unconsumed in the
/// ring, starting at its tag, to be presented again once the caller has read
/// more in after it; [`Self::is_underflow`] says that happened.
///
/// A length-delimited field that is not an embedded message (bytes, a string,
/// a packed field) is delivered only once all of it is in the ring, so the
/// ring must be larger than the largest such field. A ring full of one
/// unfinished field makes `receive` return an error rather than wait for data
/// that cannot fit.
///
/// # Untrusted input
///
/// [`Self::new`] places no limit on the size a field may declare, so a peer
/// can announce a huge field and a ring that large would be needed to hold it.
/// For data you do not trust, use [`Self::with_limits`]: a field declaring
/// more than `max_length_delimited_size` is rejected as soon as its length
/// prefix has been read, before any of its payload is buffered.
pub struct Parser<H: Handler> {
    handler: H,
    max_message_depth: usize,
    max_length_delimited_size: usize,
    /// At each open message level, the stream offset at which that message
    /// ends. Offsets are measured against the running `consumed` count, so
    /// completing a field is a single addition however deeply it is nested.
    message_end: Vec<u64>,
    /// Bytes of fully-parsed fields since construction or `reset()`.
    consumed: u64,
    underflow: bool,
}

/// What `scan` found at the front of the ring. Nothing is consumed or
/// reported until the field is known to be complete.
enum Scanned {
    Underflow,
    Varint { field: u32, value: u64, len: usize },
    Fixed64 { field: u32, value: u64, len: usize },
    Fixed32 { field: u32, value: u32, len: usize },
    /// The tag and length prefix of an embedded message (its content is
    /// parsed as ordinary fields).
    Message { field: u32, header: usize, length: usize },
    /// A bytes/string/packed field, all of it present.
    Bytes { field: u32, header: usize, length: usize },
}

impl<H: Handler> Parser<H> {
    /// Creates a parser that owns `handler`, with the default nesting limit
    /// and no limit on field size.
    ///
    /// To keep the handler yourself, lend it: `Parser::new(&mut handler)`
    /// works because `&mut H` is a [`Handler`] whenever `H` is. Use
    /// [`Self::handler`], [`Self::handler_mut`] and [`Self::into_handler`] to
    /// get at one the parser owns.
    ///
    /// See *Untrusted input* in the type documentation before using this on
    /// data you do not trust.
    pub fn new(handler: H) -> Self {
        Self::with_limits(handler, DEFAULT_MAX_MESSAGE_DEPTH, UNLIMITED_LENGTH_DELIMITED)
    }

    /// Creates a parser with security limits.
    ///
    /// `max_message_depth` is the deepest nesting of embedded messages allowed
    /// and `max_length_delimited_size` the largest declared payload for a
    /// length-delimited field (bytes, strings, embedded messages).
    ///
    /// # Panics
    ///
    /// If `max_message_depth` is less than 1.
    pub fn with_limits(
        handler: H,
        max_message_depth: usize,
        max_length_delimited_size: usize,
    ) -> Self {
        assert!(max_message_depth >= 1, "max_message_depth must be at least 1");
        Self {
            handler,
            max_message_depth,
            max_length_delimited_size,
            message_end: Vec::new(),
            consumed: 0,
            underflow: false,
        }
    }

    /// The handler the parser is driving.
    pub fn handler(&self) -> &H {
        &self.handler
    }

    /// Mutable access to the handler between calls to [`Self::receive`].
    pub fn handler_mut(&mut self) -> &mut H {
        &mut self.handler
    }

    /// Takes the handler back, dropping the parser.
    pub fn into_handler(self) -> H {
        self.handler
    }

    /// True if the last [`Self::receive`] ended with an incomplete field left
    /// in the ring. More data is needed before parsing can continue.
    pub fn is_underflow(&self) -> bool {
        self.underflow
    }

    /// Parses as many complete fields as possible from `buf`, calling the
    /// handler for each and consuming them from the ring.
    ///
    /// If the ring holds an incomplete field, parsing stops at its tag and
    /// [`Self::is_underflow`] returns true. Read more data into the ring after
    /// it and call this again.
    ///
    /// # Errors
    ///
    /// A [`ParseError`] if the data is malformed, a limit is exceeded, or `buf`
    /// is full of a single field that is not yet complete (it can never fit).
    pub fn receive(&mut self, buf: &mut LoopBuf) -> ParseResult<()> {
        self.underflow = false;

        while !buf.is_empty() {
            self.end_completed_messages();

            let scanned = self.scan(buf.spans())?;

            let consumed = match scanned {
                Scanned::Underflow => {
                    self.underflow = true;
                    // Whatever was parsed before this has been consumed, so a
                    // full ring means this field is all it holds and can
                    // never be completed
                    if buf.is_full() {
                        return Err(ParseError::new(format!(
                            "receive buffer too small: the next field needs more than the {} bytes it holds",
                            buf.len()
                        )));
                    }
                    return Ok(());
                }
                Scanned::Varint { field, value, len } => {
                    buf.advance(len);
                    self.handler.handle_varint(field, value);
                    len
                }
                Scanned::Fixed64 { field, value, len } => {
                    buf.advance(len);
                    self.handler.handle_fixed64(field, value);
                    len
                }
                Scanned::Fixed32 { field, value, len } => {
                    buf.advance(len);
                    self.handler.handle_fixed32(field, value);
                    len
                }
                Scanned::Message { field, header, length } => {
                    // The tag and length prefix count against the parent
                    // levels BEFORE the new message is pushed, whose end is
                    // measured from the end of that prefix
                    buf.advance(header);
                    self.consumed += header as u64;
                    self.push_message_end(length)?;
                    self.handler.start_message(field);
                    0 // already counted
                }
                Scanned::Bytes { field, header, length } => {
                    // The handler sees the payload as a window onto the ring:
                    // no copy, even if it straddles the wrap
                    {
                        let mut payload = buf.window(header, length);
                        self.handler.handle_bytes(field, &mut payload);
                    }
                    buf.advance(header + length);
                    header + length
                }
            };

            // Count the field against all enclosing messages
            self.consumed += consumed as u64;
        }

        self.end_completed_messages();
        Ok(())
    }

    /// Completes parsing and validates state.
    ///
    /// Fails if the last [`Self::receive`] left an incomplete field, or if
    /// embedded messages are still open.
    pub fn close(&self) -> ParseResult<()> {
        if self.underflow {
            return Err(ParseError::new("incomplete field at end of input"));
        }
        if !self.message_end.is_empty() {
            return Err(ParseError::new(format!(
                "unclosed embedded messages: {}",
                self.message_end.len()
            )));
        }
        Ok(())
    }

    /// Resets the parser to its initial state, to reuse it for a new
    /// independent message.
    pub fn reset(&mut self) {
        self.message_end.clear();
        self.consumed = 0;
        self.underflow = false;
    }

    /// Reads the field at the front of `input` without consuming anything.
    fn scan(&self, input: Spans<'_>) -> ParseResult<Scanned> {
        let message_left = self.message_bytes_remaining();

        let (tag, tag_len) = match read_varint(input, 0, message_left)? {
            Varint::Underflow => return Ok(Scanned::Underflow),
            Varint::Value(value, len) => (value, len),
        };

        // A tag is an unsigned 32-bit value. Truncating a wider varint would
        // let this parser read different fields from the same bytes than a
        // strict one.
        if tag >> 32 != 0 {
            return Err(ParseError::new("Tag exceeds 32 bits"));
        }
        let tag = tag as u32;
        let field = tag >> 3;
        let wire_type = (tag & 0x07) as u8;
        if field == 0 {
            return Err(ParseError::new("Invalid field number 0"));
        }

        // What is left of the enclosing message for the rest of this field
        let left = message_left.saturating_sub(tag_len as u64);

        match wire_type {
            WIRETYPE_VARINT => Ok(match read_varint(input, tag_len, left)? {
                Varint::Underflow => Scanned::Underflow,
                Varint::Value(value, len) => Scanned::Varint {
                    field,
                    value,
                    len: tag_len + len,
                },
            }),

            WIRETYPE_I64 => {
                if 8 > left {
                    return Err(field_exceeds_message());
                }
                if input.len() < tag_len + 8 {
                    return Ok(Scanned::Underflow);
                }
                Ok(Scanned::Fixed64 {
                    field,
                    value: u64::from_le_bytes(fixed(input, tag_len)),
                    len: tag_len + 8,
                })
            }

            WIRETYPE_I32 => {
                if 4 > left {
                    return Err(field_exceeds_message());
                }
                if input.len() < tag_len + 4 {
                    return Ok(Scanned::Underflow);
                }
                Ok(Scanned::Fixed32 {
                    field,
                    value: u32::from_le_bytes(fixed(input, tag_len)),
                    len: tag_len + 4,
                })
            }

            WIRETYPE_LEN => {
                let (declared, prefix_len) = match read_varint(input, tag_len, left)? {
                    Varint::Underflow => return Ok(Scanned::Underflow),
                    Varint::Value(value, len) => (value, len),
                };

                // Check the full 64-bit value: casting first would turn
                // lengths such as 2^32 + 3 into small, valid-looking ones.
                if declared > i64::MAX as u64 {
                    return Err(ParseError::new(format!(
                        "Negative length: {}",
                        declared as i64
                    )));
                }
                if declared > i32::MAX as u64 {
                    return Err(ParseError::new(format!("Length out of range: {declared}")));
                }
                let length = declared as usize;
                if length > self.max_length_delimited_size {
                    return Err(ParseError::new(format!(
                        "Length-delimited field size exceeds limit: {length}"
                    )));
                }
                let header = tag_len + prefix_len;
                if length as u64 > message_left.saturating_sub(header as u64) {
                    return Err(field_exceeds_message());
                }

                if self.handler.is_message(field) {
                    Ok(Scanned::Message { field, header, length })
                } else if input.len() < header + length {
                    Ok(Scanned::Underflow)
                } else {
                    Ok(Scanned::Bytes { field, header, length })
                }
            }

            other => Err(ParseError::new(format!("Unknown wire type: {other}"))),
        }
    }

    /// Bytes still available in the innermost open message, or `u64::MAX` if
    /// there is none.
    fn message_bytes_remaining(&self) -> u64 {
        match self.message_end.last() {
            None => u64::MAX,
            Some(&end) => end.saturating_sub(self.consumed),
        }
    }

    /// Ends every open message whose last byte has been consumed.
    fn end_completed_messages(&mut self) {
        while let Some(&end) = self.message_end.last() {
            if self.consumed < end {
                break;
            }
            self.message_end.pop();
            self.handler.end_message();
        }
    }

    fn push_message_end(&mut self, length: usize) -> ParseResult<()> {
        if self.message_end.len() >= self.max_message_depth {
            return Err(ParseError::new(format!(
                "Nested message depth exceeds limit of {}",
                self.max_message_depth
            )));
        }
        self.message_end.push(self.consumed + length as u64);
        Ok(())
    }
}

fn field_exceeds_message() -> ParseError {
    ParseError::new("Field extends past embedded message boundary")
}

/// The `N` bytes at `at`, wherever they sit (the caller has checked they are there).
fn fixed<const N: usize>(input: Spans<'_>, at: usize) -> [u8; N] {
    let mut bytes = [0u8; N];
    input.copy_to(at, &mut bytes);
    bytes
}

enum Varint {
    /// Not enough data yet.
    Underflow,
    /// The value and the number of bytes it took.
    Value(u64, usize),
}

/// Reads a varint at `at`, using at most `max_bytes` bytes (the room left in
/// the enclosing message). A varint that runs past that budget is an error;
/// one cut off by the end of the data is an underflow.
fn read_varint(input: Spans<'_>, at: usize, max_bytes: u64) -> ParseResult<Varint> {
    let available = input.len() - at;
    if available == 0 {
        return Ok(Varint::Underflow);
    }
    if max_bytes == 0 {
        return Err(field_exceeds_message());
    }
    let first = input[at];
    if first < 0x80 {
        // most tags and small values are a single byte
        return Ok(Varint::Value(u64::from(first), 1));
    }

    let mut result = u64::from(first & 0x7F);
    for i in 1..10usize {
        if i >= available {
            return Ok(Varint::Underflow);
        }
        if i as u64 >= max_bytes {
            return Err(field_exceeds_message());
        }
        let b = input[at + i];
        result |= u64::from(b & 0x7F) << (7 * i);
        if b < 0x80 {
            return Ok(Varint::Value(result, i + 1));
        }
    }
    // Tenth byte still has its continuation bit set
    Err(ParseError::new("Varint too long"))
}
