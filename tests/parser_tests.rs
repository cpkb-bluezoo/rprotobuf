//! Integration tests ported from gumdrop `ProtobufParserTest`.

use loopbuf::LoopBuf;
use rprotobuf::decode_helpers::{as_bytes, BytesCollector};
use rprotobuf::{Handler, ParseResult, Parser, Writer};

/// A ring holding `bytes`, with room to spare.
fn ring(bytes: &[u8]) -> LoopBuf {
    let mut buf = LoopBuf::with_capacity(bytes.len() + 64);
    buf.extend_from_slice(bytes);
    buf
}

fn write_message<F>(write: F) -> Vec<u8>
where
    F: FnOnce(&mut Writer, &mut LoopBuf) -> Result<(), rprotobuf::WriteError>,
{
    let mut writer = Writer::new();
    let mut ring = LoopBuf::with_capacity(4096);
    write(&mut writer, &mut ring).unwrap();
    // Embedded messages' length prefixes are merged in as the ring drains.
    let mut out = Vec::new();
    writer.drain_to(&mut ring, &mut out).unwrap();
    out
}

#[derive(Default)]
struct RecordingHandler {
    varints: Vec<(u32, u64)>,
    fixed64s: Vec<(u32, u64)>,
    fixed32s: Vec<(u32, u32)>,
    bytes: Vec<(u32, Vec<u8>)>,
}

impl Handler for RecordingHandler {
    fn handle_varint(&mut self, field_number: u32, value: u64) -> ParseResult<()> {
        self.varints.push((field_number, value));
        Ok(())
    }

    fn handle_fixed64(&mut self, field_number: u32, value: u64) -> ParseResult<()> {
        self.fixed64s.push((field_number, value));
        Ok(())
    }

    fn handle_fixed32(&mut self, field_number: u32, value: u32) -> ParseResult<()> {
        self.fixed32s.push((field_number, value));
        Ok(())
    }

    fn start_bytes(&mut self, field_number: u32, _length: u64) -> ParseResult<()> {
        self.bytes.push((field_number, Vec::new()));
        Ok(())
    }

    fn bytes_content(&mut self, data: &mut LoopBuf) -> ParseResult<()> {
        self.bytes.last_mut().unwrap().1.extend(as_bytes(data));
        data.advance(data.len());
        Ok(())
    }
}

struct MessageTrackingHandler {
    message_fields: Vec<u32>,
    message_starts: Vec<u32>,
    message_ends: u32,
    varints: Vec<(u32, u64)>,
    last_string: Option<String>,
    collector: BytesCollector,
}

impl Handler for MessageTrackingHandler {
    fn is_message(&self, field_number: u32) -> bool {
        self.message_fields.contains(&field_number)
    }

    fn start_message(&mut self, field_number: u32) -> ParseResult<()> {
        self.message_starts.push(field_number);
        Ok(())
    }

    fn end_message(&mut self) -> ParseResult<()> {
        self.message_ends += 1;
        Ok(())
    }

    fn handle_varint(&mut self, field_number: u32, value: u64) -> ParseResult<()> {
        self.varints.push((field_number, value));
        Ok(())
    }

    fn start_bytes(&mut self, _field_number: u32, _length: u64) -> ParseResult<()> {
        self.collector.start(0);
        Ok(())
    }

    fn bytes_content(&mut self, data: &mut LoopBuf) -> ParseResult<()> {
        self.collector.push(data);
        Ok(())
    }

    fn end_bytes(&mut self) -> ParseResult<()> {
        self.last_string = self.collector.take_string().ok();
        Ok(())
    }
}

#[test]
fn parse_varint_field() {
    let data = write_message(|w, ring| w.write_varint_field(ring, 1, 150));
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = ring(data.as_slice());
    parser.receive(&mut input).unwrap();
    parser.close(&mut input).unwrap();
    assert_eq!(handler.varints, vec![(1, 150)]);
}

#[test]
fn parse_multiple_varint_fields() {
    let data = write_message(|w, ring| {
        w.write_varint_field(ring, 1, 42)?;
        w.write_varint_field(ring, 2, 100)?;
        w.write_varint_field(ring, 3, 256)
    });
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = ring(data.as_slice());
    parser.receive(&mut input).unwrap();
    parser.close(&mut input).unwrap();
    assert_eq!(handler.varints, vec![(1, 42), (2, 100), (3, 256)]);
}

#[test]
fn parse_bool_field() {
    let data = write_message(|w, ring| {
        w.write_bool_field(ring, 1, true)?;
        w.write_bool_field(ring, 2, false)
    });
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = ring(data.as_slice());
    parser.receive(&mut input).unwrap();
    parser.close(&mut input).unwrap();
    assert_eq!(handler.varints, vec![(1, 1), (2, 0)]);
}

#[test]
fn parse_fixed64_field() {
    let data = write_message(|w, ring| w.write_fixed64_field(ring, 1, 0x0102_0304_0506_0708));
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = ring(data.as_slice());
    parser.receive(&mut input).unwrap();
    parser.close(&mut input).unwrap();
    assert_eq!(handler.fixed64s, vec![(1, 0x0102_0304_0506_0708)]);
}

#[test]
fn parse_fixed32_field() {
    let data = write_message(|w, ring| w.write_fixed32_field(ring, 1, 0x0102_0304));
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = ring(data.as_slice());
    parser.receive(&mut input).unwrap();
    parser.close(&mut input).unwrap();
    assert_eq!(handler.fixed32s, vec![(1, 0x0102_0304)]);
}

#[test]
fn parse_string_field() {
    let data = write_message(|w, ring| w.write_string_field(ring, 1, "hello"));
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = ring(data.as_slice());
    parser.receive(&mut input).unwrap();
    parser.close(&mut input).unwrap();
    assert_eq!(handler.bytes.len(), 1);
    assert_eq!(
        String::from_utf8(handler.bytes[0].1.clone()).unwrap(),
        "hello"
    );
}

#[test]
fn parse_bytes_field() {
    let data = write_message(|w, ring| w.write_bytes_field(ring, 1, &[1, 2, 3, 4]));
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = ring(data.as_slice());
    parser.receive(&mut input).unwrap();
    parser.close(&mut input).unwrap();
    assert_eq!(handler.bytes, vec![(1, vec![1, 2, 3, 4])]);
}

#[test]
fn parse_embedded_message() {
    let data = write_message(|w, ring| {
        w.write_varint_field(ring, 1, 42)?;
        w.write_message_field(ring, 2, |inner, ring| {
            inner.write_string_field(ring, 1, "nested")?;
            inner.write_varint_field(ring, 2, 100)
        })?;
        w.write_varint_field(ring, 3, 99)
    });
    let mut handler = MessageTrackingHandler {
        message_fields: vec![2],
        message_starts: Vec::new(),
        message_ends: 0,
        varints: Vec::new(),
        last_string: None,
        collector: BytesCollector::default(),
    };
    let mut parser = Parser::new(&mut handler);
    let mut input = ring(data.as_slice());
    parser.receive(&mut input).unwrap();
    parser.close(&mut input).unwrap();
    assert_eq!(handler.message_starts, vec![2]);
    assert_eq!(handler.message_ends, 1);
    assert_eq!(handler.varints, vec![(1, 42), (2, 100), (3, 99)]);
    assert_eq!(handler.last_string.as_deref(), Some("nested"));
}

#[test]
fn parse_nested_messages() {
    let data = write_message(|w, ring| {
        w.write_message_field(ring, 1, |level1, ring| {
            level1.write_message_field(ring, 2, |level2, ring| {
                level2.write_string_field(ring, 3, "deepest")
            })
        })
    });
    let mut handler = MessageTrackingHandler {
        message_fields: vec![1, 2],
        message_starts: Vec::new(),
        message_ends: 0,
        varints: Vec::new(),
        last_string: None,
        collector: BytesCollector::default(),
    };
    let mut parser = Parser::new(&mut handler);
    let mut input = ring(data.as_slice());
    parser.receive(&mut input).unwrap();
    parser.close(&mut input).unwrap();
    assert_eq!(handler.message_starts, vec![1, 2]);
    assert_eq!(handler.message_ends, 2);
    assert_eq!(handler.last_string.as_deref(), Some("deepest"));
}

#[test]
fn underflow_varint() {
    let data = write_message(|w, ring| w.write_varint_field(ring, 1, 16384));
    let partial = &data[..2];
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = ring(partial);
    parser.receive(&mut input).unwrap();
    assert!(parser.is_underflow());
    assert!(handler.varints.is_empty());
}

#[test]
fn underflow_recovery() {
    let data = write_message(|w, ring| {
        w.write_varint_field(ring, 1, 16384)?;
        w.write_varint_field(ring, 2, 42)
    });
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);

    let mut chunk1 = ring(&data[..2]);
    parser.receive(&mut chunk1).unwrap();
    assert!(parser.is_underflow());

    let mut chunk2 = ring(data.as_slice());
    parser.receive(&mut chunk2).unwrap();
    assert!(!parser.is_underflow());
    parser.close(&mut chunk2).unwrap();
    assert_eq!(handler.varints, vec![(1, 16384), (2, 42)]);
}

#[test]
fn close_with_underflow_errors() {
    let data = write_message(|w, ring| w.write_string_field(ring, 1, "hello world"));
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = ring(&data[..3]);
    parser.receive(&mut input).unwrap();
    assert!(parser.is_underflow());
    let err = parser.close(&mut input).unwrap_err();
    assert!(err.to_string().contains("incomplete"));
}

#[test]
fn invalid_field_number() {
    let data = [0x00u8, 0x01];
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = ring(&data);
    let err = parser.receive(&mut input).unwrap_err();
    assert!(err.to_string().contains("field number 0"));
}

#[test]
fn unknown_wire_type() {
    let data = [0x0Bu8, 0x00];
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = ring(&data);
    let err = parser.receive(&mut input).unwrap_err();
    assert!(err.to_string().contains("wire type"));
}

#[test]
fn reset_parser() {
    let data1 = write_message(|w, ring| w.write_varint_field(ring, 1, 42));
    let data2 = write_message(|w, ring| w.write_varint_field(ring, 1, 99));
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);

    let mut input1 = ring(data1.as_slice());
    parser.receive(&mut input1).unwrap();
    parser.close(&mut input1).unwrap();

    parser.reset();

    let mut input2 = ring(data2.as_slice());
    parser.receive(&mut input2).unwrap();
    parser.close(&mut input2).unwrap();

    // Same parser instance; handler accumulates both messages (gumdrop clears
    // handler between phases — not possible here while parser borrows handler).
    assert_eq!(handler.varints, vec![(1, 42), (1, 99)]);
}

// ===== Handler ownership =====

#[test]
fn a_parser_owns_the_handler_it_is_given_and_hands_it_back() {
    let data = write_message(|w, ring| {
        w.write_varint_field(ring, 1, 7)?;
        w.write_varint_field(ring, 2, 8)
    });
    let mut parser = Parser::new(RecordingHandler::default());
    let mut input = ring(data.as_slice());
    parser.receive(&mut input).unwrap();
    parser.close(&mut input).unwrap();
    // Readable while the parser is still in use, between calls...
    assert_eq!(parser.handler().varints, vec![(1, 7), (2, 8)]);
    parser.handler_mut().varints.clear();
    // ...and taken back for good at the end.
    let handler = parser.into_handler();
    assert!(handler.varints.is_empty());
}

#[test]
fn a_handler_can_still_be_lent_to_the_parser() {
    let data = write_message(|w, ring| w.write_varint_field(ring, 3, 9));
    let mut handler = RecordingHandler::default();
    {
        let mut parser = Parser::new(&mut handler);
        let mut input = ring(data.as_slice());
        parser.receive(&mut input).unwrap();
        parser.close(&mut input).unwrap();
    }
    assert_eq!(handler.varints, vec![(3, 9)]);
}

#[test]
fn a_boxed_dyn_handler_drives_the_parser() {
    let data = write_message(|w, ring| w.write_varint_field(ring, 1, 5));
    let mut parser = Parser::new(Box::new(RecordingHandler::default()) as Box<dyn Handler>);
    let mut input = ring(data.as_slice());
    parser.receive(&mut input).unwrap();
    parser.close(&mut input).unwrap();

    // A handler behind a reference to the trait object works the same.
    let mut recording = RecordingHandler::default();
    let dynamic: &mut dyn Handler = &mut recording;
    let mut parser = Parser::new(dynamic);
    let mut input = ring(data.as_slice());
    parser.receive(&mut input).unwrap();
    drop(parser);
    assert_eq!(recording.varints, vec![(1, 5)]);
}
