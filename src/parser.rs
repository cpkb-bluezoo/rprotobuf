use std::io::Read;

use loopbuf::{LoopBuf, Spans};

use crate::error::{ParseError, ParseResult};
use crate::handler::Handler;
use crate::limits::ParserLimits;
use crate::wire::{WIRETYPE_I32, WIRETYPE_I64, WIRETYPE_LEN, WIRETYPE_VARINT};

const DEFAULT_BUFFER_SIZE: usize = 8192;

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
/// The ring has to hold one whole field, except for a length-delimited one that
/// is not an embedded message (bytes, a string, a packed field). Those are
/// streamed to the handler as [`Handler::start_bytes`], any number of
/// [`Handler::bytes_content`] pieces and [`Handler::end_bytes`], so a payload of
/// any size passes through a ring of any size. Embedded messages are parsed as
/// ordinary fields in place. So the largest the ring need be is the largest
/// scalar field, about fifteen bytes; a ring full of one unfinished field makes
/// `receive` return an error rather than wait for data that cannot fit.
///
/// # Untrusted input
///
/// [`Self::new`] uses [`ParserLimits::default`]: nesting is limited to 100, and
/// a field may declare any size, so a handler that accumulates what it is given
/// should set [`ParserLimits::max_field_length`]. A field declaring more is
/// rejected as soon as its length prefix has been read, before any of its
/// payload.
pub struct Parser<H: Handler> {
    handler: H,
    limits: ParserLimits,
    /// Only used to size [`Self::parse`]'s own ring; `receive` scans whatever
    /// ring the caller chose.
    buffer_size: usize,
    /// At each open message level, the stream offset at which that message
    /// ends. Offsets are measured against the running `consumed` count, so
    /// completing a field is a single addition however deeply it is nested.
    message_end: Vec<u64>,
    /// Bytes of fully-parsed fields since construction or `reset()`.
    consumed: u64,
    /// How much of the bytes field being streamed is still to come.
    bytes_left: Option<u64>,
    underflow: bool,
    closed: bool,
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
    /// The tag and length prefix of a bytes/string/packed field, whose payload
    /// is streamed to the handler.
    Bytes { field: u32, header: usize, length: usize },
}

impl<H: Handler> Parser<H> {
    /// Creates a parser that owns `handler`, with the default limits.
    ///
    /// To keep the handler yourself, lend it: `Parser::new(&mut handler)`
    /// works because `&mut H` is a [`Handler`] whenever `H` is. Use
    /// [`Self::handler`], [`Self::handler_mut`] and [`Self::into_handler`] to
    /// get at one the parser owns.
    pub fn new(handler: H) -> Self {
        Self::with_limits(handler, ParserLimits::default())
    }

    /// Creates a parser with the given limits.
    ///
    /// # Panics
    ///
    /// If `limits.max_nesting_depth` is `Some(0)`.
    pub fn with_limits(handler: H, limits: ParserLimits) -> Self {
        assert!(
            limits.max_nesting_depth != Some(0),
            "max_nesting_depth must be at least 1"
        );
        Self {
            handler,
            limits,
            buffer_size: DEFAULT_BUFFER_SIZE,
            message_end: Vec::new(),
            consumed: 0,
            bytes_left: None,
            underflow: false,
            closed: false,
        }
    }

    /// The limits in force.
    pub fn limits(&self) -> &ParserLimits {
        &self.limits
    }

    /// Mutable access to the limits, to change one without restating the rest.
    ///
    /// A `max_nesting_depth` of `Some(0)` is refused when the next message
    /// is parsed.
    pub fn limits_mut(&mut self) -> &mut ParserLimits {
        &mut self.limits
    }

    /// Replaces the limits.
    ///
    /// # Panics
    ///
    /// If `limits.max_nesting_depth` is `Some(0)`.
    pub fn set_limits(&mut self, limits: ParserLimits) {
        assert!(
            limits.max_nesting_depth != Some(0),
            "max_nesting_depth must be at least 1"
        );
        self.limits = limits;
    }

    /// Sets the size of the ring [`Self::parse`] allocates for itself. It has
    /// no effect on [`Self::receive`], which scans whatever ring it is given.
    ///
    /// # Panics
    ///
    /// If `buffer_size` is zero.
    pub fn set_buffer_size(&mut self, buffer_size: usize) {
        assert!(buffer_size > 0, "buffer size must be positive");
        self.buffer_size = buffer_size;
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
    /// A [`ParseError`] if the data is malformed, a limit is exceeded, the
    /// handler returned an error, or `buf` is full of a single field that is not
    /// yet complete (it can never fit).
    pub fn receive(&mut self, buf: &mut LoopBuf) -> ParseResult<()> {
        if self.closed {
            return Err(ParseError::new("parser is closed: call reset() to parse another message"));
        }
        self.underflow = false;

        while !buf.is_empty() {
            if let Some(remaining) = self.bytes_left {
                if !self.pass_bytes(buf, remaining)? {
                    self.underflow = true;
                    return Ok(());
                }
                continue;
            }

            self.end_completed_messages()?;

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
                    self.handler.handle_varint(field, value)?;
                    len
                }
                Scanned::Fixed64 { field, value, len } => {
                    buf.advance(len);
                    self.handler.handle_fixed64(field, value)?;
                    len
                }
                Scanned::Fixed32 { field, value, len } => {
                    buf.advance(len);
                    self.handler.handle_fixed32(field, value)?;
                    len
                }
                Scanned::Message { field, header, length } => {
                    // The tag and length prefix count against the parent
                    // levels BEFORE the new message is pushed, whose end is
                    // measured from the end of that prefix
                    buf.advance(header);
                    self.consumed += header as u64;
                    self.push_message_end(length)?;
                    self.handler.start_message(field)?;
                    0 // already counted
                }
                Scanned::Bytes { field, header, length } => {
                    buf.advance(header);
                    self.consumed += header as u64;
                    self.handler.start_bytes(field, length as u64)?;
                    if length == 0 {
                        self.handler.end_bytes()?;
                    } else {
                        self.bytes_left = Some(length as u64);
                    }
                    0 // the header is counted; the payload is as it is passed on
                }
            };

            // Count the field against all enclosing messages
            self.consumed += consumed as u64;
        }

        if self.bytes_left.is_none() {
            self.end_completed_messages()?;
        } else {
            self.underflow = true;
        }
        Ok(())
    }

    /// Offers the handler the next piece of the bytes field being streamed:
    /// whatever of the payload is in the ring. Returns true if the field ended
    /// (and parsing can go on to the next), false if more of its payload is to
    /// come, so there is nothing to do until the caller has added to the ring.
    fn pass_bytes(&mut self, buf: &mut LoopBuf, remaining: u64) -> ParseResult<bool> {
        let offer = remaining.min(buf.len() as u64) as usize;
        let left = {
            let mut window = buf.window(0, offer);
            self.handler.bytes_content(&mut window)?;
            window.len()
        };
        let taken = offer - left;

        if offer as u64 == remaining {
            // The last piece: the field ends here, whatever the handler left
            buf.advance(offer);
            self.consumed += offer as u64;
            self.bytes_left = None;
            self.handler.end_bytes()?;
            return Ok(true);
        }
        if taken == 0 && buf.is_full() {
            return Err(ParseError::new(format!(
                "receive buffer too small: the handler left all {} bytes it was offered and the ring cannot take more",
                buf.len()
            )));
        }
        // Everything in the ring was offered, so there is nothing more to
        // offer until the caller adds to what the handler left.
        buf.advance(taken);
        self.consumed += taken as u64;
        self.bytes_left = Some(remaining - taken as u64);
        Ok(false)
    }

    /// Completes parsing and validates state.
    ///
    /// `buf` is the same ring passed to [`Self::receive`] throughout. Anything
    /// still in it is parsed first; fails if that leaves an incomplete field, a
    /// bytes field short of its declared length, or embedded messages still
    /// open. After closing, further calls to [`Self::receive`] return an error.
    /// Use [`Self::reset`] to parse a new message.
    pub fn close(&mut self, buf: &mut LoopBuf) -> ParseResult<()> {
        if self.closed {
            return Ok(());
        }
        if !buf.is_empty() {
            self.receive(buf)?;
        }
        self.closed = true;
        if !buf.is_empty() || (self.underflow && self.bytes_left.is_none()) {
            return Err(ParseError::new("incomplete field at end of input"));
        }
        if let Some(left) = self.bytes_left {
            return Err(ParseError::new(format!(
                "incomplete bytes field at end of input: {left} bytes short"
            )));
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
    /// independent message. The limits and handler are kept; the ring passed to
    /// [`Self::receive`] is caller-owned and not this parser's concern.
    pub fn reset(&mut self) {
        self.message_end.clear();
        self.consumed = 0;
        self.bytes_left = None;
        self.underflow = false;
        self.closed = false;
    }

    /// Parses a message from a [`Read`] source.
    ///
    /// The parser is automatically reset before parsing. Allocates one ring
    /// (sized by [`Self::set_buffer_size`]) and never grows it: reads from
    /// `reader` into its spare capacity and calls [`Self::receive`] until EOF,
    /// then [`Self::close`]. The reader is not closed.
    pub fn parse<R: Read>(&mut self, mut reader: R) -> ParseResult<()> {
        self.reset();

        let mut buf = LoopBuf::with_capacity(self.buffer_size);
        loop {
            let (spare, _) = buf.spare_capacity_mut();
            if spare.is_empty() {
                // `receive` reports a ring full of one unfinished field, so
                // this is not reached; it is here so a bug cannot loop forever
                return Err(ParseError::new("receive buffer too small"));
            }
            let bytes_read = reader
                .read(spare)
                .map_err(|e| ParseError::new(format!("I/O error reading stream: {e}")))?;
            if bytes_read == 0 {
                break;
            }
            buf.commit_write(bytes_read);
            self.receive(&mut buf)?;
        }

        self.close(&mut buf)
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
                if self.limits.max_field_length.is_some_and(|max| length > max) {
                    return Err(ParseError::new(format!(
                        "Length-delimited field size exceeds limit: {length}"
                    )));
                }
                let header = tag_len + prefix_len;
                if length as u64 > message_left.saturating_sub(header as u64) {
                    return Err(field_exceeds_message());
                }

                if input.len() < header {
                    return Ok(Scanned::Underflow);
                }
                if self.handler.is_message(field) {
                    Ok(Scanned::Message { field, header, length })
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
    fn end_completed_messages(&mut self) -> ParseResult<()> {
        while let Some(&end) = self.message_end.last() {
            if self.consumed < end {
                break;
            }
            self.message_end.pop();
            self.handler.end_message()?;
        }
        Ok(())
    }

    fn push_message_end(&mut self, length: usize) -> ParseResult<()> {
        if let Some(max) = self.limits.max_nesting_depth {
            if self.message_end.len() >= max {
                return Err(ParseError::new(format!(
                    "Nested message depth exceeds limit of {max}"
                )));
            }
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
