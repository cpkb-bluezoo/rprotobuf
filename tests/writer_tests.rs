//! Integration tests ported from gumdrop `ProtobufWriterTest`.
//!
//! Gumdrop's `testNullString` / `testNullBytes` are intentionally omitted: the Rust
//! API uses `&str` and `&[u8]`, so null-skip behaviour does not apply.

use loopbuf::LoopBuf;
use rprotobuf::{Writer, WIRETYPE_LEN, WIRETYPE_VARINT};

/// Everything the writer has put in the ring, with embedded messages' length
/// prefixes merged in, as a sink would receive it.
fn drained(writer: &mut Writer, ring: &mut LoopBuf) -> Vec<u8> {
    let mut out = Vec::new();
    writer.drain_to(ring, &mut out).unwrap();
    out
}

#[test]
fn varint_small_value() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer.write_varint(&mut ring, 1).unwrap();
    assert_eq!(ring.spans().to_vec(), vec![0x01]);
}

#[test]
fn varint_value_150() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer.write_varint(&mut ring, 150).unwrap();
    assert_eq!(ring.spans().to_vec(), vec![0x96, 0x01]);
}

#[test]
fn varint_value_300() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer.write_varint(&mut ring, 300).unwrap();
    assert_eq!(ring.spans().to_vec(), vec![0xAC, 0x02]);
}

#[test]
fn varint_large_value() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer.write_varint(&mut ring, 16384).unwrap();
    assert_eq!(ring.spans().to_vec(), vec![0x80, 0x80, 0x01]);
}

#[test]
fn varint_max_int() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer.write_varint(&mut ring, i32::MAX as u64).unwrap();
    assert_eq!(ring.spans().to_vec().len(), 5);
}

#[test]
fn varint_max_long() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer.write_varint(&mut ring, i64::MAX as u64).unwrap();
    assert_eq!(ring.spans().to_vec().len(), 9);
}

#[test]
fn svarint_zigzag() {
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    w.write_svarint(&mut ring, 0).unwrap();
    assert_eq!(ring.spans().to_vec(), vec![0x00]);

    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    w.write_svarint(&mut ring, -1).unwrap();
    assert_eq!(ring.spans().to_vec(), vec![0x01]);

    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    w.write_svarint(&mut ring, 1).unwrap();
    assert_eq!(ring.spans().to_vec(), vec![0x02]);

    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    w.write_svarint(&mut ring, -2).unwrap();
    assert_eq!(ring.spans().to_vec(), vec![0x03]);
}

#[test]
fn fixed64_little_endian() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer.write_fixed64(&mut ring, 0x0102_0304_0506_0708).unwrap();
    assert_eq!(
        ring.spans().to_vec(),
        vec![0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]
    );
}

#[test]
fn fixed32_little_endian() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer.write_fixed32(&mut ring, 0x0102_0304).unwrap();
    assert_eq!(ring.spans().to_vec(), vec![0x04, 0x03, 0x02, 0x01]);
}

#[test]
fn tag_encoding() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer.write_tag(&mut ring, 1, WIRETYPE_VARINT).unwrap();
    assert_eq!(ring.spans().to_vec(), vec![0x08]);
}

#[test]
fn tag_encoding_field2_len() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer.write_tag(&mut ring, 2, WIRETYPE_LEN).unwrap();
    assert_eq!(ring.spans().to_vec(), vec![0x12]);
}

#[test]
fn tag_encoding_large_field_number() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer.write_tag(&mut ring, 16, WIRETYPE_VARINT).unwrap();
    assert_eq!(ring.spans().to_vec(), vec![0x80, 0x01]);
}

#[test]
fn varint_field() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer.write_varint_field(&mut ring, 1, 150).unwrap();
    assert_eq!(ring.spans().to_vec(), vec![0x08, 0x96, 0x01]);
}

#[test]
fn bool_field() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer.write_bool_field(&mut ring, 1, true).unwrap();
    writer.write_bool_field(&mut ring, 2, false).unwrap();
    assert_eq!(ring.spans().to_vec(), vec![0x08, 0x01, 0x10, 0x00]);
}

#[test]
fn string_field() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer.write_string_field(&mut ring, 1, "hello").unwrap();
    assert_eq!(
        ring.spans().to_vec(),
        vec![0x0A, 0x05, b'h', b'e', b'l', b'l', b'o']
    );
}

#[test]
fn bytes_field() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer
        .write_bytes_field(&mut ring, 1, &[0x01, 0x02, 0x03, 0x04])
        .unwrap();
    assert_eq!(
        ring.spans().to_vec(),
        vec![0x0A, 0x04, 0x01, 0x02, 0x03, 0x04]
    );
}

#[test]
fn fixed64_field() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer
        .write_fixed64_field(&mut ring, 1, 0x0807_0605_0403_0201)
        .unwrap();
    assert_eq!(
        ring.spans().to_vec(),
        vec![0x09, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
    );
}

#[test]
fn message_field() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer
        .write_message_field(&mut ring, 1, |inner, ring| {
            inner.write_string_field(ring, 1, "test")
        })
        .unwrap();
    // The ring holds only the content; the tag and length are the writer's,
    // and go in front as the ring is drained. The length is the shortest
    // varint, as any encoder would write it.
    assert_eq!(ring.spans().to_vec(), vec![0x0A, 0x04, b't', b'e', b's', b't']);
    assert!(writer.has_pending_prefixes());
    assert_eq!(
        drained(&mut writer, &mut ring),
        vec![0x0A, 0x06, 0x0A, 0x04, b't', b'e', b's', b't']
    );
    assert!(!writer.has_pending_prefixes());
    assert!(ring.is_empty());
    assert_eq!(writer.bytes_written(), 8);
}

#[test]
fn an_empty_message_is_a_plain_zero_length() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer
        .write_message_field(&mut ring, 3, |_, _| Ok(()))
        .unwrap();
    assert_eq!(ring.spans().to_vec(), vec![0x1A, 0x00]);
    assert_eq!(writer.bytes_written(), 2);
}

#[test]
fn nested_messages_fill_in_every_length() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer
        .write_message_field(&mut ring, 1, |w, ring| {
            w.write_varint_field(ring, 1, 7)?;
            w.write_message_field(ring, 2, |w, ring| {
                w.write_bytes_field(ring, 1, &[9u8; 200])
            })
        })
        .unwrap();
    // inner: tag 0A, a 2 byte length (200 + 3 = 203 = CB 01), 200 bytes;
    // outer: 08 07 (2), the inner field (1 + 2 + 203 = 206), so 208 = D0 01
    let bytes = drained(&mut writer, &mut ring);
    assert_eq!(&bytes[..3], &[0x0A, 0xD0, 0x01]);
    assert_eq!(&bytes[3..8], &[0x08, 0x07, 0x12, 0xCB, 0x01]);
    assert_eq!(bytes.len(), 3 + 208);
    assert_eq!(writer.bytes_written(), bytes.len() as u64);
}

/// The same message as an encoder that knows its size up front would write.
fn reference_message(field: u32, content: &[u8]) -> Vec<u8> {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(content.len() + 32);
    writer.write_encoded_message_field(&mut ring, field, content).unwrap();
    if content.is_empty() {
        // write_encoded_message_field skips an empty message; a message
        // field of nothing is a plain zero length.
        return vec![(field << 3 | 2) as u8, 0];
    }
    ring.spans().to_vec()
}

#[test]
fn message_lengths_are_the_shortest_varint_at_every_boundary() {
    // Content sizes either side of where the length gains a byte.
    for target in [0usize, 1, 2, 126, 127, 128, 129, 130, 16382, 16383, 16384, 16385, 40000] {
        let payload = target.saturating_sub(3);
        let mut content_ring = LoopBuf::with_capacity(target + 16);
        let mut w = Writer::new();
        if target >= 3 {
            w.write_bytes_field(&mut content_ring, 1, &vec![0xAB; payload]).unwrap();
        } else if target > 0 {
            for _ in 0..target {
                content_ring.extend_from_slice(&[0x00]);
            }
        }
        let content = content_ring.spans().to_vec();

        let mut writer = Writer::new();
        let mut ring = LoopBuf::with_capacity(content.len() + 16);
        writer
            .write_message_field(&mut ring, 5, |inner, ring| {
                if target >= 3 {
                    inner.write_bytes_field(ring, 1, &vec![0xAB; payload])
                } else {
                    ring.extend_from_slice(&content);
                    Ok(())
                }
            })
            .unwrap();
        let out = drained(&mut writer, &mut ring);
        assert_eq!(out, reference_message(5, &content), "content of {} bytes", content.len());
        if target == 0 || target >= 3 {
            // (the smallest are filled in by hand, past the writer's count)
            assert_eq!(writer.bytes_written(), out.len() as u64);
        }
    }
}

#[test]
fn double_field() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer.write_double_field(&mut ring, 1, 3.14159).unwrap();
    let out = ring.spans().to_vec();
    assert_eq!(out.len(), 9);
    assert_eq!(out[0], 0x09);
}

#[test]
fn float_field() {
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    writer.write_float_field(&mut ring, 1, 3.14f32).unwrap();
    let out = ring.spans().to_vec();
    assert_eq!(out.len(), 5);
    assert_eq!(out[0], 0x0D);
}

// -- backpressure ------------------------------------------------------------

use std::io::{ErrorKind, Read};

#[test]
fn a_field_goes_in_whole_or_not_at_all() {
    // room for the tag and the length, but not for the data after them
    let mut ring = LoopBuf::with_capacity(8);
    let mut w = Writer::new();
    w.write_varint_field(&mut ring, 1, 1).unwrap(); // 2 of 8
    let before = ring.spans().to_vec();
    let err = w.write_string_field(&mut ring, 2, "twelve bytes").unwrap_err();
    assert!(err.is_would_block() || err.kind() == ErrorKind::InvalidInput);
    assert_eq!(ring.spans().to_vec(), before, "no tag or length left behind");

    let err = w.write_string_field(&mut ring, 2, "abcde").unwrap_err(); // 7 needed, 6 spare
    assert!(err.is_would_block(), "{err}");
    assert_eq!(ring.spans().to_vec(), before);
    assert_eq!(w.bytes_written(), 2, "only what was written is counted");

    ring.advance(2);
    w.write_string_field(&mut ring, 2, "abcde").unwrap();
    assert_eq!(ring.spans().to_vec(), [0x12, 5, b'a', b'b', b'c', b'd', b'e']);
}

#[test]
fn a_field_larger_than_the_whole_ring_can_never_succeed() {
    let mut ring = LoopBuf::with_capacity(16);
    let mut w = Writer::new();
    let err = w.write_bytes_field(&mut ring, 1, &[0u8; 100]).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    assert!(!err.is_would_block());
    assert!(ring.is_empty());
}

#[test]
fn a_message_that_does_not_fit_is_taken_back_whole() {
    let mut ring = LoopBuf::with_capacity(32);
    let mut w = Writer::new();
    w.write_varint_field(&mut ring, 1, 5).unwrap();
    let before = ring.spans().to_vec();
    let written = w.bytes_written();

    let err = w
        .write_message_field(&mut ring, 2, |w, ring| {
            // the first two fit; the third runs out of ring partway through
            w.write_string_field(ring, 1, "0123456789")?;
            w.write_string_field(ring, 2, "0123456789")?;
            w.write_string_field(ring, 3, "0123456789")
        })
        .unwrap_err();
    assert!(err.is_would_block(), "{err}");
    assert_eq!(ring.spans().to_vec(), before, "the half-written message is gone");
    assert_eq!(w.bytes_written(), written);
    assert!(!w.has_pending_prefixes(), "and so are the prefixes of any messages inside it");

    // drain, then the same call works
    ring.advance(ring.len());
    w.write_message_field(&mut ring, 2, |w, ring| {
        w.write_string_field(ring, 1, "0123456789")?;
        w.write_string_field(ring, 2, "0123456789")
    })
    .unwrap();
}

#[test]
fn a_stream_far_bigger_than_the_ring_comes_out_the_same() {
    fn emit(w: &mut Writer, ring: &mut LoopBuf, out: &mut Vec<u8>) {
        // Drain to the sink and repeat the same call whenever the ring is full.
        macro_rules! retry {
            ($call:expr) => {
                loop {
                    match $call {
                        Ok(()) => break,
                        Err(e) if e.is_would_block() => {
                            w.drain_to(ring, out).unwrap();
                        }
                        Err(e) => panic!("{e}"),
                    }
                }
            };
        }
        for i in 0..400u64 {
            retry!(w.write_varint_field(ring, 1, i * 1000));
            retry!(w.write_string_field(ring, 2, &format!("item {i}")));
            retry!(w.write_message_field(ring, 3, |w, r| {
                w.write_fixed32_field(r, 1, i as u32)?;
                w.write_double_field(r, 2, i as f64)
            }));
        }
        w.drain_to(ring, out).unwrap();
    }

    let mut reference = Vec::new();
    emit(&mut Writer::new(), &mut LoopBuf::with_capacity(1 << 20), &mut reference);
    // the largest unit is the embedded message's content: 5 + 9 = 14 bytes
    for capacity in [14, 15, 33, 64] {
        let mut out = Vec::new();
        emit(&mut Writer::new(), &mut LoopBuf::with_capacity(capacity), &mut out);
        assert_eq!(out, reference, "capacity {capacity}");
    }
}

// -- length prefixes are merged in as the ring is drained -----------------------

/// A sink that takes at most `max` bytes at a time and will not take any on
/// every third call.
struct Trickle {
    out: Vec<u8>,
    max: usize,
    calls: usize,
}

impl std::io::Write for Trickle {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.calls += 1;
        if self.calls % 3 == 0 {
            return Err(ErrorKind::WouldBlock.into());
        }
        let n = bytes.len().min(self.max);
        self.out.extend_from_slice(&bytes[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The same message as an encoder that knows its size up front would write it.
fn nested_reference() -> Vec<u8> {
    fn wrap(field: u8, content: &[u8]) -> Vec<u8> {
        let mut len = Vec::new();
        let mut v = content.len();
        loop {
            if v < 0x80 {
                len.push(v as u8);
                break;
            }
            len.push((v & 0x7F) as u8 | 0x80);
            v >>= 7;
        }
        let mut out = vec![field << 3 | 2];
        out.extend(len);
        out.extend_from_slice(content);
        out
    }
    let mut innermost = vec![0x08, 0x07];
    innermost.extend(wrap(1, &[0xAA; 200]));
    let mut inner = vec![0x08, 0x01];
    inner.extend(wrap(2, &innermost));
    inner.extend([0x10, 0x02]);
    let mut outer = vec![0x08, 0x05];
    outer.extend(wrap(3, &inner));
    outer.extend(wrap(4, &[]));
    outer.extend([0x18, 0x09]);
    wrap(1, &outer)
}

fn write_nested(w: &mut Writer, ring: &mut LoopBuf) -> rprotobuf::WriteResult<()> {
    w.write_message_field(ring, 1, |w, ring| {
        w.write_varint_field(ring, 1, 5)?;
        w.write_message_field(ring, 3, |w, ring| {
            w.write_varint_field(ring, 1, 1)?;
            w.write_message_field(ring, 2, |w, ring| {
                w.write_varint_field(ring, 1, 7)?;
                w.write_bytes_field(ring, 1, &[0xAA; 200])
            })?;
            w.write_varint_field(ring, 2, 2)
        })?;
        w.write_message_field(ring, 4, |_, _| Ok(()))?;
        w.write_varint_field(ring, 3, 9)
    })
}

#[test]
fn prefixes_are_merged_in_order_whatever_the_nesting() {
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    write_nested(&mut w, &mut ring).unwrap();
    let out = drained(&mut w, &mut ring);
    // (the reference's innermost message has the 0x08 0x07 first, then field 1)
    assert_eq!(out, nested_reference());
    assert_eq!(w.bytes_written(), out.len() as u64);
}

#[test]
fn a_prefix_takes_no_room_in_the_ring() {
    // The content alone is 6 bytes, so a ring of 6 holds the message whatever
    // its prefix.
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(6);
    w.write_message_field(&mut ring, 1, |w, ring| w.write_bytes_field(ring, 1, b"1234")).unwrap();
    assert!(ring.is_full());
    assert_eq!(drained(&mut w, &mut ring), [0x0A, 0x06, 0x0A, 0x04, b'1', b'2', b'3', b'4']);
}

#[test]
fn a_sink_that_trickles_and_blocks_gets_the_same_bytes() {
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    write_nested(&mut w, &mut ring).unwrap();
    let mut sink = Trickle { out: Vec::new(), max: 1, calls: 0 };
    let mut rounds = 0;
    while w.has_pending_prefixes() || !ring.is_empty() {
        // Each call moves what the sink will take before it blocks.
        match w.drain_to(&mut ring, &mut sink) {
            Ok(moved) => assert!(moved > 0),
            Err(e) if e.kind() == ErrorKind::WouldBlock => {}
            Err(e) => panic!("{e}"),
        }
        rounds += 1;
    }
    assert!(rounds > 100, "the sink took the message a byte or two at a time");
    assert_eq!(sink.out, nested_reference());
}

#[test]
fn a_sink_that_takes_nothing_loses_nothing() {
    struct Blocked;
    impl std::io::Write for Blocked {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(ErrorKind::WouldBlock.into())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    write_nested(&mut w, &mut ring).unwrap();
    let len = ring.len();
    let err = w.drain_to(&mut ring, &mut Blocked).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::WouldBlock);
    assert_eq!(ring.len(), len);
    assert_eq!(drained(&mut w, &mut ring), nested_reference());
}

#[test]
fn writing_to_another_ring_is_just_a_sink() {
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    write_nested(&mut w, &mut ring).unwrap();
    let mut wire = LoopBuf::with_capacity(4096);
    w.drain_to(&mut ring, &mut wire).unwrap();
    assert_eq!(wire.spans().to_vec(), nested_reference());
}

#[test]
fn final_bytes_can_be_read_from_the_ring_directly() {
    // With no prefix waiting the ring is the output: it can be consumed
    // without the writer, and a message written afterwards is still right.
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(64);
    w.write_varint_field(&mut ring, 1, 1).unwrap();
    w.write_string_field(&mut ring, 2, "abc").unwrap();
    assert!(!w.has_pending_prefixes());
    let first = ring.spans().to_vec();
    ring.advance(ring.len());
    w.write_message_field(&mut ring, 3, |w, ring| w.write_varint_field(ring, 1, 2)).unwrap();
    assert_eq!(first, [0x08, 0x01, 0x12, 0x03, b'a', b'b', b'c']);
    assert_eq!(drained(&mut w, &mut ring), [0x1A, 0x02, 0x08, 0x02]);
}

#[test]
fn reading_past_a_pending_prefix_is_caught() {
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(64);
    w.write_message_field(&mut ring, 1, |w, ring| w.write_varint_field(ring, 1, 2)).unwrap();
    ring.advance(1); // the content, read without the prefix that goes before it
    let err = w.write_varint_field(&mut ring, 2, 3).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    assert!(w.drain_to(&mut ring, &mut Vec::new()).is_err());
}

#[test]
fn empty_messages_leave_no_prefix_behind() {
    // A million of them through a ring of 8: each is a tag and a zero in the
    // ring, so nothing accumulates but the ring itself.
    let mut w = Writer::new();
    let mut ring = LoopBuf::with_capacity(8);
    let mut sunk = 0usize;
    for _ in 0..1_000_000 {
        loop {
            match w.write_message_field(&mut ring, 1, |_, _| Ok(())) {
                Ok(()) => break,
                Err(e) if e.is_would_block() => {
                    let mut out = Vec::new();
                    w.drain_to(&mut ring, &mut out).unwrap();
                    sunk += out.len();
                }
                Err(e) => panic!("{e}"),
            }
        }
        assert!(!w.has_pending_prefixes());
    }
    sunk += ring.len();
    assert_eq!(sunk, 2_000_000);
}

// -- the parser reads what the writer wrote -------------------------------------

#[test]
fn the_parser_reads_what_the_writer_drained() {
    use rprotobuf::{Handler, Parser};
    #[derive(Default)]
    struct Log(Vec<String>);
    impl Handler for Log {
        fn handle_varint(&mut self, f: u32, v: u64) {
            self.0.push(format!("varint {f}={v}"));
        }
        fn handle_bytes(&mut self, f: u32, d: &mut LoopBuf) {
            self.0.push(format!("bytes {f}={}", rprotobuf::decode_helpers::as_string(d).unwrap()));
        }
        fn is_message(&self, f: u32) -> bool {
            f == 2 || f == 3
        }
        fn start_message(&mut self, f: u32) {
            self.0.push(format!("start {f}"));
        }
        fn end_message(&mut self) {
            self.0.push("end".into());
        }
    }

    let mut ring = LoopBuf::with_capacity(512);
    let mut w = Writer::new();
    w.write_varint_field(&mut ring, 1, 300).unwrap();
    w.write_message_field(&mut ring, 2, |w, ring| {
        w.write_string_field(ring, 1, "nested")?;
        w.write_message_field(ring, 3, |w, ring| w.write_varint_field(ring, 1, 9))?;
        w.write_message_field(ring, 3, |_, _| Ok(())) // an empty one
    })
    .unwrap();
    w.write_string_field(&mut ring, 4, "after").unwrap();

    // the writer drains into the ring the parser reads
    let mut wire = LoopBuf::with_capacity(512);
    w.drain_to(&mut ring, &mut wire).unwrap();
    let mut log = Log::default();
    let mut p = Parser::new(&mut log);
    p.receive(&mut wire).unwrap();
    p.close().unwrap();
    assert!(wire.is_empty());
    assert_eq!(
        log.0,
        [
            "varint 1=300",
            "start 2",
            "bytes 1=nested",
            "start 3",
            "varint 1=9",
            "end",
            "start 3",
            "end",
            "end",
            "bytes 4=after",
        ]
    );
}
