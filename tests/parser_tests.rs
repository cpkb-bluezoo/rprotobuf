//! Integration tests ported from gumdrop `ProtobufParserTest`.

use rprotobuf::decode_helpers::as_string;
use rprotobuf::{Handler, Parser, Writer};

fn write_message<F>(write: F) -> Vec<u8>
where
    F: FnOnce(&mut Writer<rprotobuf::Buffer>) -> Result<(), rprotobuf::WriteError>,
{
    let mut writer = Writer::buffer(256);
    write(&mut writer).unwrap();
    writer.finish()
}

struct RecordingHandler {
    varints: Vec<(u32, u64)>,
    fixed64s: Vec<(u32, u64)>,
    fixed32s: Vec<(u32, u32)>,
    bytes: Vec<(u32, Vec<u8>)>,
}

impl Default for RecordingHandler {
    fn default() -> Self {
        Self {
            varints: Vec::new(),
            fixed64s: Vec::new(),
            fixed32s: Vec::new(),
            bytes: Vec::new(),
        }
    }
}

impl Handler for RecordingHandler {
    fn handle_varint(&mut self, field_number: u32, value: u64) {
        self.varints.push((field_number, value));
    }

    fn handle_fixed64(&mut self, field_number: u32, value: u64) {
        self.fixed64s.push((field_number, value));
    }

    fn handle_fixed32(&mut self, field_number: u32, value: u32) {
        self.fixed32s.push((field_number, value));
    }

    fn handle_bytes(&mut self, field_number: u32, data: &[u8]) {
        self.bytes.push((field_number, data.to_vec()));
    }
}

struct MessageTrackingHandler {
    message_fields: Vec<u32>,
    message_starts: Vec<u32>,
    message_ends: u32,
    varints: Vec<(u32, u64)>,
    last_string: Option<String>,
}

impl Handler for MessageTrackingHandler {
    fn is_message(&self, field_number: u32) -> bool {
        self.message_fields.contains(&field_number)
    }

    fn start_message(&mut self, field_number: u32) {
        self.message_starts.push(field_number);
    }

    fn end_message(&mut self) {
        self.message_ends += 1;
    }

    fn handle_varint(&mut self, field_number: u32, value: u64) {
        self.varints.push((field_number, value));
    }

    fn handle_bytes(&mut self, field_number: u32, data: &[u8]) {
        self.last_string = as_string(data).ok();
        let _ = field_number;
    }
}

#[test]
fn parse_varint_field() {
    let data = write_message(|w| w.write_varint_field(1, 150));
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = data.as_slice();
    parser.receive(&mut input).unwrap();
    parser.close().unwrap();
    assert_eq!(handler.varints, vec![(1, 150)]);
}

#[test]
fn parse_multiple_varint_fields() {
    let data = write_message(|w| {
        w.write_varint_field(1, 42)?;
        w.write_varint_field(2, 100)?;
        w.write_varint_field(3, 256)
    });
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = data.as_slice();
    parser.receive(&mut input).unwrap();
    parser.close().unwrap();
    assert_eq!(
        handler.varints,
        vec![(1, 42), (2, 100), (3, 256)]
    );
}

#[test]
fn parse_bool_field() {
    let data = write_message(|w| {
        w.write_bool_field(1, true)?;
        w.write_bool_field(2, false)
    });
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = data.as_slice();
    parser.receive(&mut input).unwrap();
    parser.close().unwrap();
    assert_eq!(handler.varints, vec![(1, 1), (2, 0)]);
}

#[test]
fn parse_fixed64_field() {
    let data = write_message(|w| w.write_fixed64_field(1, 0x0102_0304_0506_0708));
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = data.as_slice();
    parser.receive(&mut input).unwrap();
    parser.close().unwrap();
    assert_eq!(handler.fixed64s, vec![(1, 0x0102_0304_0506_0708)]);
}

#[test]
fn parse_fixed32_field() {
    let data = write_message(|w| w.write_fixed32_field(1, 0x0102_0304));
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = data.as_slice();
    parser.receive(&mut input).unwrap();
    parser.close().unwrap();
    assert_eq!(handler.fixed32s, vec![(1, 0x0102_0304)]);
}

#[test]
fn parse_string_field() {
    let data = write_message(|w| w.write_string_field(1, "hello"));
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = data.as_slice();
    parser.receive(&mut input).unwrap();
    parser.close().unwrap();
    assert_eq!(handler.bytes.len(), 1);
    assert_eq!(as_string(&handler.bytes[0].1).unwrap(), "hello");
}

#[test]
fn parse_bytes_field() {
    let data = write_message(|w| w.write_bytes_field(1, &[1, 2, 3, 4]));
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = data.as_slice();
    parser.receive(&mut input).unwrap();
    parser.close().unwrap();
    assert_eq!(handler.bytes, vec![(1, vec![1, 2, 3, 4])]);
}

#[test]
fn parse_embedded_message() {
    let data = write_message(|w| {
        w.write_varint_field(1, 42)?;
        w.write_message_field(2, |inner| {
            inner.write_string_field(1, "nested")?;
            inner.write_varint_field(2, 100)
        })?;
        w.write_varint_field(3, 99)
    });
    let mut handler = MessageTrackingHandler {
        message_fields: vec![2],
        message_starts: Vec::new(),
        message_ends: 0,
        varints: Vec::new(),
        last_string: None,
    };
    let mut parser = Parser::new(&mut handler);
    let mut input = data.as_slice();
    parser.receive(&mut input).unwrap();
    parser.close().unwrap();
    assert_eq!(handler.message_starts, vec![2]);
    assert_eq!(handler.message_ends, 1);
    assert_eq!(handler.varints, vec![(1, 42), (2, 100), (3, 99)]);
    assert_eq!(handler.last_string.as_deref(), Some("nested"));
}

#[test]
fn parse_nested_messages() {
    let data = write_message(|w| {
        w.write_message_field(1, |level1| {
            level1.write_message_field(2, |level2| level2.write_string_field(3, "deepest"))
        })
    });
    let mut handler = MessageTrackingHandler {
        message_fields: vec![1, 2],
        message_starts: Vec::new(),
        message_ends: 0,
        varints: Vec::new(),
        last_string: None,
    };
    let mut parser = Parser::new(&mut handler);
    let mut input = data.as_slice();
    parser.receive(&mut input).unwrap();
    parser.close().unwrap();
    assert_eq!(handler.message_starts, vec![1, 2]);
    assert_eq!(handler.message_ends, 2);
    assert_eq!(handler.last_string.as_deref(), Some("deepest"));
}

#[test]
fn underflow_varint() {
    let data = write_message(|w| w.write_varint_field(1, 16384));
    let partial = &data[..2];
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = partial;
    parser.receive(&mut input).unwrap();
    assert!(parser.is_underflow());
    assert!(handler.varints.is_empty());
}

#[test]
fn underflow_recovery() {
    let data = write_message(|w| {
        w.write_varint_field(1, 16384)?;
        w.write_varint_field(2, 42)
    });
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);

    let mut chunk1 = &data[..2];
    parser.receive(&mut chunk1).unwrap();
    assert!(parser.is_underflow());

    let mut chunk2 = data.as_slice();
    parser.receive(&mut chunk2).unwrap();
    assert!(!parser.is_underflow());
    parser.close().unwrap();
    assert_eq!(handler.varints, vec![(1, 16384), (2, 42)]);
}

#[test]
fn close_with_underflow_errors() {
    let data = write_message(|w| w.write_string_field(1, "hello world"));
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = &data[..3];
    parser.receive(&mut input).unwrap();
    assert!(parser.is_underflow());
    let err = parser.close().unwrap_err();
    assert!(err.to_string().contains("incomplete"));
}

#[test]
fn invalid_field_number() {
    let data = [0x00u8, 0x01];
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = &data[..];
    let err = parser.receive(&mut input).unwrap_err();
    assert!(err.to_string().contains("field number 0"));
}

#[test]
fn unknown_wire_type() {
    let data = [0x0Bu8, 0x00];
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);
    let mut input = &data[..];
    let err = parser.receive(&mut input).unwrap_err();
    assert!(err.to_string().contains("wire type"));
}

#[test]
fn reset_parser() {
    let data1 = write_message(|w| w.write_varint_field(1, 42));
    let data2 = write_message(|w| w.write_varint_field(1, 99));
    let mut handler = RecordingHandler::default();
    let mut parser = Parser::new(&mut handler);

    let mut input1 = data1.as_slice();
    parser.receive(&mut input1).unwrap();
    parser.close().unwrap();

    parser.reset();

    let mut input2 = data2.as_slice();
    parser.receive(&mut input2).unwrap();
    parser.close().unwrap();

    // Same parser instance; handler accumulates both messages (gumdrop clears
    // handler between phases — not possible here while parser borrows handler).
    assert_eq!(handler.varints, vec![(1, 42), (1, 99)]);
}
