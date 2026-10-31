//! The caller-owned ring contract and the untrusted-input checks, ported from
//! the jprotobuf `ProtobufParserTest` / limits tests.
//!
//! `receive` takes a `LoopBuf`. The parser consumes whole fields and leaves
//! an incomplete one in the ring; fields (and the payloads handed to
//! `handle_bytes` as windows) must parse identically wherever the ring's wrap
//! falls.

use loopbuf::LoopBuf;
use rprotobuf::decode_helpers::as_bytes;
use rprotobuf::{Handler, Parser, Writer};

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
}

impl Handler for Events {
    fn handle_varint(&mut self, f: u32, v: u64) {
        self.log.push(format!("varint {f}={v}"));
    }
    fn handle_fixed64(&mut self, f: u32, v: u64) {
        self.log.push(format!("fixed64 {f}={v}"));
    }
    fn handle_fixed32(&mut self, f: u32, v: u32) {
        self.log.push(format!("fixed32 {f}={v}"));
    }
    fn handle_bytes(&mut self, f: u32, data: &mut LoopBuf) {
        self.log.push(format!("bytes {f}={:?}", as_bytes(data)));
    }
    fn is_message(&self, f: u32) -> bool {
        self.messages.contains(&f)
    }
    fn start_message(&mut self, f: u32) {
        self.log.push(format!("start {f}"));
    }
    fn end_message(&mut self) {
        self.log.push("end".into());
    }
}

fn events_with_messages(messages: &[u32]) -> Events {
    Events {
        messages: messages.to_vec(),
        log: Vec::new(),
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
            Some(l) => Parser::with_limits(&mut h, l, rprotobuf::UNLIMITED_LENGTH_DELIMITED),
            None => Parser::new(&mut h),
        };
        let r = p.receive(&mut ring_of(&nested(depth)));
        r.map(|_| p.close())
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
    let mut p = Parser::with_limits(&mut h, 100, 4096);
    let err = p.receive(&mut ring_of(&prefix)).unwrap_err().to_string();
    assert!(err.contains("exceeds limit"), "{err}");

    // while the same prefix without a limit is just an incomplete field
    let mut h = Events::default();
    let mut p = Parser::new(&mut h);
    p.receive(&mut ring_of(&prefix)).unwrap();
    assert!(p.is_underflow());
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
    p.receive(&mut ring_of(&data)).unwrap();
    p.close().unwrap();
    assert_eq!(h.log, [format!("varint 1={}", u64::MAX)]);
}

// -- the ring contract --------------------------------------------------------

#[test]
fn an_incomplete_field_stays_in_the_ring_from_its_tag() {
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    w.write_varint_field(&mut ring, 1, 7).unwrap();
    w.write_string_field(&mut ring, 2, "hello world").unwrap();
    let data = ring.spans().to_vec();
    let first = 2; // the complete varint field
    let mut h = Events::default();
    let mut p = Parser::new(&mut h);
    let mut buf = ring_of(&data[..data.len() - 3]);
    p.receive(&mut buf).unwrap();
    assert!(p.is_underflow());
    assert_eq!(buf.len(), data.len() - 3 - first, "only the incomplete field is left");

    buf.extend_from_slice(&data[data.len() - 3..]);
    p.receive(&mut buf).unwrap();
    assert!(!p.is_underflow());
    assert!(buf.is_empty());
    p.close().unwrap();
}

#[test]
fn a_ring_too_small_for_a_field_is_an_error() {
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    w.write_bytes_field(&mut ring, 1, &[7u8; 100]).unwrap();
    let data = ring.spans().to_vec();
    let mut h = Events::default();
    let mut p = Parser::new(&mut h);
    let mut buf = LoopBuf::with_capacity(16);
    let mut offset = 0;
    let err = loop {
        let n = buf.extend_from_slice(&data[offset..]);
        offset += n;
        assert!(n > 0, "the ring is full of one unfinished field and the parser did not say so");
        if let Err(e) = p.receive(&mut buf) {
            break e;
        }
    };
    assert!(err.to_string().contains("buffer"), "{err}");
}

/// A message with every kind of field, multi-byte varints, and nesting.
fn sample() -> (Vec<u8>, &'static [u32]) {
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    w.write_varint_field(&mut ring, 1, 300).unwrap();
    w.write_varint_field(&mut ring, 2, u64::MAX).unwrap();
    w.write_fixed64_field(&mut ring, 3, 0x0102_0304_0506_0708).unwrap();
    w.write_fixed32_field(&mut ring, 4, 0xAABB_CCDD).unwrap();
    w.write_string_field(&mut ring, 5, "hello, world").unwrap();
    w.write_message_field(&mut ring, 6, |m, ring| {
        m.write_varint_field(ring, 1, 1_000_000)?;
        m.write_bytes_field(ring, 2, &[9u8; 20])?;
        m.write_message_field(ring, 3, |mm, ring| mm.write_string_field(ring, 1, "deep"))
    })
    .unwrap();
    w.write_varint_field(&mut ring, 7, 1).unwrap();
    (ring.spans().to_vec(), &[6, 3])
}

#[test]
fn fields_parse_identically_wherever_the_ring_wraps() {
    let (data, messages) = sample();
    let whole = {
        let mut h = events_with_messages(messages);
        let mut p = Parser::new(&mut h);
        p.receive(&mut ring_of(&data)).unwrap();
        p.close().unwrap();
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
                p.close().unwrap_or_else(|e| panic!("capacity {capacity} step {step}: {e}"));
                assert!(buf.is_empty());
            }
            assert_eq!(h.log, whole, "capacity {capacity} step {step}");
        }
    }
}

// -- the window handed to handle_bytes ---------------------------------------

/// A bytes field holding an encoded message, which the handler parses with a
/// second parser straight from the window: no copy of the payload.
struct Nested {
    inner_log: Vec<String>,
    spans_seen: Vec<(usize, usize)>,
}

struct Inner<'a>(&'a mut Vec<String>);

impl Handler for Inner<'_> {
    fn handle_varint(&mut self, f: u32, v: u64) {
        self.0.push(format!("varint {f}={v}"));
    }
    fn handle_bytes(&mut self, f: u32, data: &mut LoopBuf) {
        self.0.push(format!("bytes {f}={:?}", as_bytes(data)));
    }
}

impl Handler for Nested {
    fn handle_bytes(&mut self, _f: u32, data: &mut LoopBuf) {
        let (a, b) = data.readable_spans();
        self.spans_seen.push((a.len(), b.len()));
        let mut inner = Inner(&mut self.inner_log);
        let mut parser = Parser::new(&mut inner);
        parser.receive(data).unwrap();
        parser.close().unwrap();
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
            inner_log: Vec::new(),
            spans_seen: Vec::new(),
        };
        {
            let mut p = Parser::new(&mut h);
            let mut buf = LoopBuf::with_capacity(capacity);
            // park the write cursor so that the field wraps
            buf.extend_from_slice(&vec![0u8; capacity / 2]);
            buf.advance(capacity / 2);
            for &byte in &data {
                buf.extend_from_slice(&[byte]);
                p.receive(&mut buf).unwrap();
            }
            p.close().unwrap();
            assert!(buf.is_empty());
        }
        assert_eq!(
            h.inner_log,
            ["varint 1=12345".to_string(), "bytes 2=[105, 110, 110, 101, 114]".to_string()],
            "capacity {capacity}"
        );
        if h.spans_seen.iter().any(|&(_, b)| b > 0) {
            wrapped += 1;
        }
    }
    assert!(wrapped > 0, "no capacity exercised a payload straddling the wrap");
}

#[test]
fn what_a_handler_consumes_from_the_window_does_not_leak() {
    struct Eater(Vec<usize>);
    impl Handler for Eater {
        fn handle_bytes(&mut self, _f: u32, data: &mut LoopBuf) {
            self.0.push(data.len());
            data.advance(data.len()); // swallow all of it
        }
    }
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    w.write_bytes_field(&mut ring, 1, b"abcdef").unwrap();
    w.write_varint_field(&mut ring, 2, 5).unwrap();
    let data = ring.spans().to_vec();
    let mut h = Eater(Vec::new());
    let mut p = Parser::new(&mut h);
    let mut buf = ring_of(&data);
    p.receive(&mut buf).unwrap();
    p.close().unwrap();
    assert!(buf.is_empty(), "the parser advanced past the whole field itself");
    assert_eq!(h.0, [6]);
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
    p.close().unwrap();
    assert!(buf.is_empty());
    assert_eq!(h.log.len(), 3);
    assert_eq!(h.log[0], "varint 1=300");
    assert_eq!(h.log[2], "fixed32 3=7");
}
