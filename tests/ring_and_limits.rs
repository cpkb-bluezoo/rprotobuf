//! The caller-owned ring contract and the untrusted-input checks, ported from
//! the jprotobuf `ProtobufParserTest` / limits tests.
//!
//! `receive` takes a `LoopBuf`. The parser consumes whole fields and leaves
//! an incomplete one in the ring; fields (and the payloads handed to
//! `handle_bytes` as windows) must parse identically wherever the ring's wrap
//! falls.

use loopbuf::LoopBuf;
use rprotobuf::decode_helpers::as_bytes;
use rprotobuf::{Handler, ParseResult, Parser, ParserLimits, Writer};

fn ring_of(bytes: &[u8]) -> LoopBuf {
    let mut buf = LoopBuf::with_capacity(bytes.len() + 64);
    buf.extend_from_slice(bytes);
    buf
}

/// Records every event as a string, so two parses can be compared whole.
#[derive(Default)]
struct Events {
    messages: Vec<u32>,
    log: Vec<String>,
    field: u32,
    gathered: Vec<u8>,
}

impl Handler for Events {
    fn handle_varint(&mut self, f: u32, v: u64) -> ParseResult<()> {
        self.log.push(format!("varint {f}={v}"));
        Ok(())
    }
    fn handle_fixed64(&mut self, f: u32, v: u64) -> ParseResult<()> {
        self.log.push(format!("fixed64 {f}={v}"));
        Ok(())
    }
    fn handle_fixed32(&mut self, f: u32, v: u32) -> ParseResult<()> {
        self.log.push(format!("fixed32 {f}={v}"));
        Ok(())
    }
    fn start_bytes(&mut self, f: u32, _length: u64) -> ParseResult<()> {
        self.field = f;
        self.gathered.clear();
        Ok(())
    }
    fn bytes_content(&mut self, data: &mut LoopBuf) -> ParseResult<()> {
        self.gathered.extend(as_bytes(data));
        data.advance(data.len());
        Ok(())
    }
    fn end_bytes(&mut self) -> ParseResult<()> {
        self.log
            .push(format!("bytes {}={:?}", self.field, self.gathered));
        Ok(())
    }
    fn is_message(&self, f: u32) -> bool {
        self.messages.contains(&f)
    }
    fn start_message(&mut self, f: u32) -> ParseResult<()> {
        self.log.push(format!("start {f}"));
        Ok(())
    }
    fn end_message(&mut self) -> ParseResult<()> {
        self.log.push("end".into());
        Ok(())
    }
}

fn events_with_messages(messages: &[u32]) -> Events {
    Events {
        messages: messages.to_vec(),
        ..Events::default()
    }
}

fn err_of(data: &[u8], messages: &[u32]) -> String {
    let mut h = events_with_messages(messages);
    let mut p = Parser::new(&mut h);
    p.receive(&mut ring_of(data)).unwrap_err().to_string()
}

// -- untrusted input ---------------------------------------------------------

#[test]
fn a_field_that_overruns_its_enclosing_message_is_rejected() {
    // field 1 is a message declared 2 bytes long, but holds a varint field
    // (tag 0x10, value 0x96 0x01) that needs 3
    let err = err_of(&[0x0A, 0x02, 0x10, 0x96, 0x01, 0x18, 0x07], &[1]);
    assert!(err.contains("past embedded message boundary"), "{err}");
}

#[test]
fn fixed_and_length_delimited_fields_cannot_overrun_their_message_either() {
    // fixed32 (4 bytes) in a 2 byte message
    let err = err_of(&[0x0A, 0x02, 0x15, 0, 0, 0, 0, 0], &[1]);
    assert!(err.contains("past embedded message boundary"), "{err}");
    // a bytes field declaring 5 in a 3 byte message
    let err = err_of(&[0x0A, 0x03, 0x12, 0x05, 1, 2, 3, 4, 5], &[1]);
    assert!(err.contains("past embedded message boundary"), "{err}");
}

#[test]
fn a_tag_wider_than_32_bits_is_rejected() {
    // 2^32 + 8 would be field 536870913, above protobuf's maximum, if truncated
    let err = err_of(&[0x88, 0x80, 0x80, 0x80, 0x10, 0x05], &[]);
    assert!(err.contains("32 bits"), "{err}");
}

#[test]
fn nesting_deeper_than_the_limit_is_rejected() {
    // n messages inside each other, field 1 each time, innermost empty
    fn nested(n: usize) -> Vec<u8> {
        let mut inner = Vec::new();
        for _ in 0..n {
            let mut next = vec![0x0A];
            let mut len = inner.len();
            loop {
                let b = (len & 0x7F) as u8;
                len >>= 7;
                if len == 0 {
                    next.push(b);
                    break;
                }
                next.push(b | 0x80);
            }
            next.extend(inner);
            inner = next;
        }
        inner
    }

    let parse = |depth: usize, limit: Option<usize>| {
        let mut h = events_with_messages(&[1]);
        let mut p = match limit {
            Some(l) => Parser::with_limits(
                &mut h,
                ParserLimits {
                    max_nesting_depth: Some(l),
                    ..ParserLimits::default()
                },
            ),
            None => Parser::new(&mut h),
        };
        let mut input = ring_of(&nested(depth));
        let r = p.receive(&mut input);
        r.map(|_| p.close(&mut input))
    };
    assert!(parse(100, None).unwrap().is_ok(), "the default allows 100");
    let err = parse(101, None).unwrap_err().to_string();
    assert!(err.contains("depth"), "{err}");
    assert!(parse(3, Some(3)).unwrap().is_ok());
    assert!(parse(4, Some(3)).is_err());
}

#[test]
fn an_oversize_field_is_rejected_from_its_length_prefix_alone() {
    // a bytes field declaring 1,000,000 bytes, with none of them sent
    let prefix = [0x0A, 0xC0, 0x84, 0x3D];
    let mut h = Events::default();
    let mut p = Parser::with_limits(
        &mut h,
        ParserLimits {
            max_field_length: Some(4096),
            ..ParserLimits::default()
        },
    );
    let err = p.receive(&mut ring_of(&prefix)).unwrap_err().to_string();
    assert!(err.contains("exceeds limit"), "{err}");

    // while the same prefix without a limit is just an incomplete field
    let mut h = Events::default();
    let mut p = Parser::new(&mut h);
    p.receive(&mut ring_of(&prefix)).unwrap();
    assert!(
        p.is_underflow(),
        "it starts the field and waits for the payload"
    );
}

#[test]
fn lengths_that_do_not_fit_an_int_are_rejected() {
    // 2^32 + 3 must not wrap to 3
    let err = err_of(&[0x0A, 0x83, 0x80, 0x80, 0x80, 0x10], &[]);
    assert!(err.contains("out of range"), "{err}");
    // a length with the top bit set is negative as a signed 64-bit value
    let mut neg = vec![0x0A];
    neg.extend([0xFF; 9]);
    neg.push(0x01);
    let err = err_of(&neg, &[]);
    assert!(err.contains("Negative length"), "{err}");
}

#[test]
fn an_eleven_byte_varint_is_rejected() {
    let mut data = vec![0x08];
    data.extend([0xFF; 10]);
    data.push(0x01);
    let err = err_of(&data, &[]);
    assert!(err.contains("Varint too long"), "{err}");
}

#[test]
fn a_negative_int64_takes_ten_bytes_and_round_trips() {
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    w.write_varint_field(&mut ring, 1, u64::MAX).unwrap();
    let data = ring.spans().to_vec();
    assert_eq!(data.len(), 11);
    let mut h = Events::default();
    let mut p = Parser::new(&mut h);
    let mut input = ring_of(&data);
    p.receive(&mut input).unwrap();
    p.close(&mut input).unwrap();
    assert_eq!(h.log, [format!("varint 1={}", u64::MAX)]);
}

// -- the ring contract --------------------------------------------------------

#[test]
fn an_incomplete_field_stays_in_the_ring_from_its_tag() {
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    w.write_varint_field(&mut ring, 1, 7).unwrap();
    w.write_fixed64_field(&mut ring, 2, 0x0102_0304_0506_0708)
        .unwrap();
    let data = ring.spans().to_vec();
    let first = 2; // the complete varint field
    let mut h = Events::default();
    let mut p = Parser::new(&mut h);
    let mut buf = ring_of(&data[..data.len() - 3]);
    p.receive(&mut buf).unwrap();
    assert!(p.is_underflow());
    assert_eq!(
        buf.len(),
        data.len() - 3 - first,
        "only the incomplete field is left"
    );

    buf.extend_from_slice(&data[data.len() - 3..]);
    p.receive(&mut buf).unwrap();
    assert!(!p.is_underflow());
    assert!(buf.is_empty());
    p.close(&mut buf).unwrap();
}

#[test]
fn a_ring_too_small_for_a_scalar_field_is_an_error() {
    // a fixed64 field is 9 bytes and is read whole
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(64);
    w.write_fixed64_field(&mut ring, 1, 7).unwrap();
    let data = ring.spans().to_vec();
    let mut h = Events::default();
    let mut p = Parser::new(&mut h);
    let mut buf = LoopBuf::with_capacity(8);
    let mut offset = 0;
    let err = loop {
        let n = buf.extend_from_slice(&data[offset..]);
        offset += n;
        assert!(
            n > 0,
            "the ring is full of one unfinished field and the parser did not say so"
        );
        if let Err(e) = p.receive(&mut buf) {
            break e;
        }
    };
    assert!(err.to_string().contains("buffer"), "{err}");
}

#[test]
fn a_bytes_field_needs_no_more_ring_than_its_header() {
    // a 100 byte field through a ring of 8: only the tag and length are read whole
    let mut data = vec![0x0A, 100];
    data.extend([7u8; 100]);
    let mut h = Events::default();
    let mut p = Parser::new(&mut h);
    let mut buf = LoopBuf::with_capacity(8);
    let mut offset = 0;
    while offset < data.len() {
        offset += buf.extend_from_slice(&data[offset..]);
        p.receive(&mut buf).unwrap();
    }
    p.close(&mut buf).unwrap();
    assert_eq!(h.log, [format!("bytes 1={:?}", vec![7u8; 100])]);
}

/// A message with every kind of field, multi-byte varints, and nesting.
fn sample() -> (Vec<u8>, &'static [u32]) {
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    w.write_varint_field(&mut ring, 1, 300).unwrap();
    w.write_varint_field(&mut ring, 2, u64::MAX).unwrap();
    w.write_fixed64_field(&mut ring, 3, 0x0102_0304_0506_0708)
        .unwrap();
    w.write_fixed32_field(&mut ring, 4, 0xAABB_CCDD).unwrap();
    w.write_string_field(&mut ring, 5, "hello, world").unwrap();
    w.write_message_field(&mut ring, 6, |m, ring| {
        m.write_varint_field(ring, 1, 1_000_000)?;
        m.write_bytes_field(ring, 2, &[9u8; 20])?;
        m.write_message_field(ring, 3, |mm, ring| mm.write_string_field(ring, 1, "deep"))
    })
    .unwrap();
    w.write_varint_field(&mut ring, 7, 1).unwrap();
    let mut out = Vec::new();
    w.drain_to(&mut ring, &mut out).unwrap();
    (out, &[6, 3])
}

#[test]
fn fields_parse_identically_wherever_the_ring_wraps() {
    let (data, messages) = sample();
    let whole = {
        let mut h = events_with_messages(messages);
        let mut p = Parser::new(&mut h);
        let mut input = ring_of(&data);
        p.receive(&mut input).unwrap();
        p.close(&mut input).unwrap();
        h.log
    };
    assert!(whole.len() > 10);

    // the largest field that must be present whole is the 22 byte bytes field
    // in the nested message; every capacity from there up must agree
    for capacity in 24..=data.len() {
        for step in 1..=7 {
            let mut h = events_with_messages(messages);
            {
                let mut p = Parser::new(&mut h);
                let mut buf = LoopBuf::with_capacity(capacity);
                let mut offset = 0;
                while offset < data.len() {
                    let end = (offset + step).min(data.len());
                    let n = buf.extend_from_slice(&data[offset..end]);
                    offset += n;
                    p.receive(&mut buf)
                        .unwrap_or_else(|e| panic!("capacity {capacity} step {step}: {e}"));
                }
                p.close(&mut buf)
                    .unwrap_or_else(|e| panic!("capacity {capacity} step {step}: {e}"));
                assert!(buf.is_empty());
            }
            assert_eq!(h.log, whole, "capacity {capacity} step {step}");
        }
    }
}

// -- the window handed to bytes_content ---------------------------------------

/// A bytes field holding an encoded message, which the handler parses with a
/// second parser straight from the window: no copy of the payload. What the
/// second parser leaves (the front of a field cut off by the end of the piece)
/// is left in the window, and offered again at the front of the next piece.
struct Nested {
    inner: Parser<Events>,
    spans_seen: Vec<(usize, usize)>,
}

impl Handler for Nested {
    fn bytes_content(&mut self, data: &mut LoopBuf) -> ParseResult<()> {
        let (a, b) = data.readable_spans();
        self.spans_seen.push((a.len(), b.len()));
        self.inner.receive(data)
    }

    fn end_bytes(&mut self) -> ParseResult<()> {
        // nothing is left in the window now, so this asks only whether the
        // inner message ended on a field boundary
        self.inner.close(&mut LoopBuf::with_capacity(1))
    }
}

#[test]
fn a_bytes_field_can_be_handed_straight_to_another_parser() {
    let mut inner = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    inner.write_varint_field(&mut ring, 1, 12345).unwrap();
    inner.write_string_field(&mut ring, 2, "inner").unwrap();
    let inner = ring.spans().to_vec();
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    w.write_bytes_field(&mut ring, 1, &inner).unwrap();
    w.write_varint_field(&mut ring, 2, 9).unwrap();
    let data = ring.spans().to_vec();

    let mut wrapped = 0;
    // a ring only a little bigger than the field, fed a byte at a time, so
    // the payload lands across the wrap at some offset
    for capacity in (inner.len() + 3)..(inner.len() + 12) {
        let mut h = Nested {
            inner: Parser::new(Events::default()),
            spans_seen: Vec::new(),
        };
        {
            let mut p = Parser::new(&mut h);
            let mut buf = LoopBuf::with_capacity(capacity);
            // park the write cursor so that the field wraps
            buf.extend_from_slice(&vec![0u8; capacity / 2]);
            buf.advance(capacity / 2);
            for piece in data.chunks(5) {
                buf.extend_from_slice(piece);
                p.receive(&mut buf).unwrap();
            }
            p.close(&mut buf).unwrap();
            assert!(buf.is_empty());
        }
        assert_eq!(
            h.inner.handler().log,
            [
                "varint 1=12345".to_string(),
                "bytes 2=[105, 110, 110, 101, 114]".to_string()
            ],
            "capacity {capacity}"
        );
        if h.spans_seen.iter().any(|&(_, b)| b > 0) {
            wrapped += 1;
        }
    }
    assert!(
        wrapped > 0,
        "no capacity exercised a payload straddling the wrap"
    );
}

#[test]
fn a_bytes_field_much_longer_than_the_ring_streams_through_it() {
    // 1 MB through a ring of 32 bytes
    struct Tally {
        starts: Vec<(u32, u64)>,
        total: u64,
        sum: u64,
        ends: usize,
        pieces: usize,
    }
    impl Handler for Tally {
        fn start_bytes(&mut self, f: u32, length: u64) -> ParseResult<()> {
            self.starts.push((f, length));
            Ok(())
        }
        fn bytes_content(&mut self, data: &mut LoopBuf) -> ParseResult<()> {
            let (a, b) = data.readable_spans();
            self.total += (a.len() + b.len()) as u64;
            self.sum += a.iter().chain(b).map(|&x| u64::from(x)).sum::<u64>();
            self.pieces += 1;
            data.advance(data.len());
            Ok(())
        }
        fn end_bytes(&mut self) -> ParseResult<()> {
            self.ends += 1;
            Ok(())
        }
    }
    let payload: Vec<u8> = (0..1_000_000u32).map(|i| (i % 251) as u8).collect();
    let mut wire = vec![0x0A];
    let mut len = payload.len();
    loop {
        let b = (len & 0x7F) as u8;
        len >>= 7;
        if len == 0 {
            wire.push(b);
            break;
        }
        wire.push(b | 0x80);
    }
    wire.extend(&payload);
    wire.extend([0x10, 0x07]); // and a field after it

    let mut p = Parser::new(Tally {
        starts: vec![],
        total: 0,
        sum: 0,
        ends: 0,
        pieces: 0,
    });
    let mut ring = LoopBuf::with_capacity(32);
    for chunk in wire.chunks(20) {
        ring.extend_from_slice(chunk);
        p.receive(&mut ring).unwrap();
    }
    p.close(&mut ring).unwrap();
    let t = p.into_handler();
    assert_eq!(t.starts, [(1, 1_000_000)]);
    assert_eq!(t.total, 1_000_000);
    assert_eq!(t.sum, payload.iter().map(|&x| u64::from(x)).sum::<u64>());
    assert_eq!(t.ends, 1);
    assert!(t.pieces > 1000, "it came through in pieces");
}

#[test]
fn a_handler_that_leaves_bytes_gets_them_again_with_the_next_piece() {
    // keeps four bytes of every piece back, as a foreign parser might keep the
    // front of a token cut off at the end of a piece
    #[derive(Default)]
    struct Keeper {
        seen: Vec<Vec<u8>>,
        kept_total: usize,
    }
    impl Handler for Keeper {
        fn bytes_content(&mut self, data: &mut LoopBuf) -> ParseResult<()> {
            self.seen.push(as_bytes(data));
            let keep = 4.min(data.len());
            data.advance(data.len() - keep);
            self.kept_total += keep;
            Ok(())
        }
    }
    let mut p = Parser::new(Keeper::default());
    let mut ring = LoopBuf::with_capacity(64);
    ring.extend_from_slice(&[0x0A, 12]);
    ring.extend_from_slice(b"abcdefgh");
    p.receive(&mut ring).unwrap();
    ring.extend_from_slice(b"ijkl");
    p.receive(&mut ring).unwrap();
    p.close(&mut ring).unwrap();
    let k = p.into_handler();
    assert_eq!(k.seen, [b"abcdefgh".to_vec(), b"efghijkl".to_vec()]);
    assert!(
        ring.is_empty(),
        "what is left of the last piece is dropped with the field"
    );
}

#[test]
fn a_handler_that_never_consumes_stalls_the_ring_and_is_told_so() {
    struct Hoarder;
    impl Handler for Hoarder {
        fn bytes_content(&mut self, _data: &mut LoopBuf) -> ParseResult<()> {
            Ok(()) // consumes nothing
        }
    }
    let mut p = Parser::new(Hoarder);
    let mut ring = LoopBuf::with_capacity(16);
    ring.extend_from_slice(&[0x0A, 100]);
    p.receive(&mut ring).unwrap(); // the header is read; the payload is still to come
    ring.extend_from_slice(&[1; 16]); // and fills the ring
    let err = p.receive(&mut ring).unwrap_err().to_string();
    assert!(err.contains("handler left all"), "{err}");

    // not full yet, it just waits for more
    let mut p = Parser::new(Hoarder);
    let mut ring = LoopBuf::with_capacity(64);
    ring.extend_from_slice(&[0x0A, 100]);
    ring.extend_from_slice(&[1; 14]);
    p.receive(&mut ring).unwrap();
    assert!(p.is_underflow());
}

#[test]
fn a_handler_error_stops_the_parse() {
    struct Refuses;
    impl Handler for Refuses {
        fn handle_varint(&mut self, f: u32, _v: u64) -> ParseResult<()> {
            if f == 2 {
                Err(rprotobuf::ParseError::new("field 2 is not allowed"))
            } else {
                Ok(())
            }
        }
    }
    let mut p = Parser::new(Refuses);
    let mut ring = ring_of(&[0x08, 1, 0x10, 2, 0x18, 3]);
    let err = p.receive(&mut ring).unwrap_err().to_string();
    assert_eq!(err, "field 2 is not allowed");
}

#[test]
fn a_bytes_field_short_of_its_length_or_a_ring_left_full_is_caught_at_close() {
    let mut p = Parser::new(Events::default());
    let mut ring = ring_of(&[0x0A, 5, 1, 2]);
    p.receive(&mut ring).unwrap();
    assert!(p.is_underflow());
    let err = p.close(&mut ring).unwrap_err().to_string();
    assert!(err.contains("3 bytes short"), "{err}");

    // and bytes the caller added since the last receive are parsed by close
    let mut p = Parser::new(Events::default());
    let mut ring = ring_of(&[0x08, 1]);
    p.close(&mut ring).unwrap();
    assert!(p.handler().log.contains(&"varint 1=1".to_string()));
    assert!(p.receive(&mut ring).is_err(), "closed parsers refuse more");
}

#[test]
fn parse_reads_a_whole_message_from_a_reader() {
    let mut wire = Vec::new();
    wire.extend([0x08, 0x96, 0x01]);
    wire.extend([0x12, 0x04, b't', b'e', b's', b't']);
    let mut p = Parser::new(Events::default());
    p.set_buffer_size(4); // a ring smaller than the message
    p.parse(&wire[..]).unwrap();
    assert_eq!(
        p.handler().log,
        ["varint 1=150", "bytes 2=[116, 101, 115, 116]"]
    );

    // a truncated one is an error
    let mut p = Parser::new(Events::default());
    assert!(p.parse(&wire[..wire.len() - 1]).is_err());
}

#[test]
fn a_writer_can_encode_straight_into_the_ring_the_parser_reads() {
    let mut buf = LoopBuf::with_capacity(256);
    let mut w = Writer::new();
    w.write_varint_field(&mut buf, 1, 300).unwrap();
    w.write_string_field(&mut buf, 2, "straight in").unwrap();
    w.write_fixed32_field(&mut buf, 3, 7).unwrap();

    let mut h = Events::default();
    let mut p = Parser::new(&mut h);
    p.receive(&mut buf).unwrap();
    p.close(&mut buf).unwrap();
    assert!(buf.is_empty());
    assert_eq!(h.log.len(), 3);
    assert_eq!(h.log[0], "varint 1=300");
    assert_eq!(h.log[2], "fixed32 3=7");
}
