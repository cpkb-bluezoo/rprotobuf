//! Integration tests ported from gumdrop `ProtobufWriterTest`.
//!
//! Gumdrop's `testNullString` / `testNullBytes` are intentionally omitted: the Rust
//! API uses `&str` and `&[u8]`, so null-skip behaviour does not apply.

use loopbuf::LoopBuf;
use rprotobuf::{Writer, WIRETYPE_LEN, WIRETYPE_VARINT};

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
    // The length is the shortest varint, as any encoder would write it.
    assert_eq!(
        ring.spans().to_vec(),
        vec![0x0A, 0x06, 0x0A, 0x04, b't', b'e', b's', b't']
    );
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
    let bytes = ring.spans().to_vec();
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
        assert_eq!(ring.spans().to_vec(), reference_message(5, &content), "content of {} bytes", content.len());
        if target == 0 || target >= 3 {
            // (the smallest are filled in by hand, past the writer's count)
            assert_eq!(writer.bytes_written(), ring.len() as u64);
        }
    }
}

#[test]
fn a_message_that_gains_a_length_byte_is_right_wherever_the_ring_wraps() {
    // 200 bytes of content needs a 2 byte length, so the content is moved on
    // by one within the ring: at every read position, that move may straddle
    // the end of the backing array.
    let content_writer = |inner: &mut Writer, ring: &mut LoopBuf| {
        inner.write_bytes_field(ring, 1, &(0..197u32).map(|i| i as u8).collect::<Vec<u8>>())
    };
    let mut expected_ring = LoopBuf::with_capacity(1024);
    Writer::new().write_message_field(&mut expected_ring, 9, content_writer).unwrap();
    let expected = expected_ring.spans().to_vec();
    for capacity in [expected.len(), expected.len() + 1, 256, 300] {
        for offset in 0..capacity {
            let mut ring = LoopBuf::with_capacity(capacity);
            ring.extend_from_slice(&vec![0; offset]);
            ring.advance(offset);
            Writer::new().write_message_field(&mut ring, 9, content_writer).unwrap();
            assert_eq!(ring.spans().to_vec(), expected, "capacity {capacity} offset {offset}");
        }
    }
}

#[test]
fn widening_the_length_without_room_waits_or_fails_by_whether_it_could_ever_fit() {
    // Content of 128 bytes: 1 tag + 1 reserved length + 128 fits a ring of
    // 130, but the finished message is 1 + 2 + 128 = 131.
    let write = |ring: &mut LoopBuf| {
        Writer::new().write_message_field(ring, 1, |inner, ring| {
            inner.write_bytes_field(ring, 1, &[1u8; 126]) // 1 + 1 + 126 = 128
        })
    };
    let mut ring = LoopBuf::with_capacity(130);
    let err = write(&mut ring).unwrap_err();
    assert!(!err.is_would_block(), "can never fit");
    assert!(ring.is_empty(), "and nothing was left behind");

    // In a bigger ring that is partly full it only has to wait.
    let mut ring = LoopBuf::with_capacity(200);
    ring.extend_from_slice(&[0xEE; 70]); // 130 spare: the content fits, the finished message does not
    let err = write(&mut ring).unwrap_err();
    assert!(err.is_would_block());
    assert_eq!(ring.len(), 70);
    ring.advance(70);
    write(&mut ring).unwrap();
    assert_eq!(ring.len(), 131);
}

#[test]
fn varint_size() {
    assert_eq!(Writer::varint_size(0), 1);
    assert_eq!(Writer::varint_size(1), 1);
    assert_eq!(Writer::varint_size(127), 1);
    assert_eq!(Writer::varint_size(128), 2);
    assert_eq!(Writer::varint_size(150), 2);
    assert_eq!(Writer::varint_size(16383), 2);
    assert_eq!(Writer::varint_size(16384), 3);
    assert_eq!(Writer::varint_size(i32::MAX as u64), 5);
    assert_eq!(Writer::varint_size(i64::MAX as u64), 9);
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
        fn retry(ring: &mut LoopBuf, out: &mut Vec<u8>, mut f: impl FnMut(&mut LoopBuf) -> rprotobuf::WriteResult<()>) {
            loop {
                match f(ring) {
                    Ok(()) => return,
                    Err(e) if e.is_would_block() => {
                        ring.read_to_end(out).unwrap();
                    }
                    Err(e) => panic!("{e}"),
                }
            }
        }
        for i in 0..400u64 {
            retry(ring, out, |r| w.write_varint_field(r, 1, i * 1000));
            retry(ring, out, |r| w.write_string_field(r, 2, &format!("item {i}")));
            retry(ring, out, |r| {
                w.write_message_field(r, 3, |w, r| {
                    w.write_fixed32_field(r, 1, i as u32)?;
                    w.write_double_field(r, 2, i as f64)
                })
            });
        }
        ring.read_to_end(out).unwrap();
    }

    let mut reference = Vec::new();
    emit(&mut Writer::new(), &mut LoopBuf::with_capacity(1 << 20), &mut reference);
    // the largest unit is the embedded message: 1 + 5 + 5 + 9 = 20 bytes
    for capacity in [20, 21, 33, 64] {
        let mut out = Vec::new();
        emit(&mut Writer::new(), &mut LoopBuf::with_capacity(capacity), &mut out);
        assert_eq!(out, reference, "capacity {capacity}");
    }
}

// -- the parser reads what the writer wrote, from the same ring ---------------

#[test]
fn the_parser_reads_the_ring_the_writer_filled() {
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

    // no copy between them: the parser consumes the ring the writer filled
    let mut log = Log::default();
    let mut p = Parser::new(&mut log);
    p.receive(&mut ring).unwrap();
    p.close().unwrap();
    assert!(ring.is_empty());
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
