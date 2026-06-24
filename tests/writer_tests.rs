//! Integration tests ported from gumdrop `ProtobufWriterTest`.
//!
//! Gumdrop's `testNullString` / `testNullBytes` are intentionally omitted: the Rust
//! API uses `&str` and `&[u8]`, so null-skip behaviour does not apply.

use rprotobuf::{Buffer, Writer, WIRETYPE_LEN, WIRETYPE_VARINT};

#[test]
fn varint_small_value() {
    let mut writer = Writer::buffer(16);
    writer.write_varint(1).unwrap();
    assert_eq!(writer.finish(), vec![0x01]);
}

#[test]
fn varint_value_150() {
    let mut writer = Writer::buffer(16);
    writer.write_varint(150).unwrap();
    assert_eq!(writer.finish(), vec![0x96, 0x01]);
}

#[test]
fn varint_value_300() {
    let mut writer = Writer::buffer(16);
    writer.write_varint(300).unwrap();
    assert_eq!(writer.finish(), vec![0xAC, 0x02]);
}

#[test]
fn varint_large_value() {
    let mut writer = Writer::buffer(16);
    writer.write_varint(16384).unwrap();
    assert_eq!(writer.finish(), vec![0x80, 0x80, 0x01]);
}

#[test]
fn varint_max_int() {
    let mut writer = Writer::buffer(16);
    writer.write_varint(i32::MAX as u64).unwrap();
    assert_eq!(writer.finish().len(), 5);
}

#[test]
fn varint_max_long() {
    let mut writer = Writer::buffer(16);
    writer.write_varint(i64::MAX as u64).unwrap();
    assert_eq!(writer.finish().len(), 9);
}

#[test]
fn svarint_zigzag() {
    let mut w = Writer::buffer(16);
    w.write_svarint(0).unwrap();
    assert_eq!(w.finish(), vec![0x00]);

    let mut w = Writer::buffer(16);
    w.write_svarint(-1).unwrap();
    assert_eq!(w.finish(), vec![0x01]);

    let mut w = Writer::buffer(16);
    w.write_svarint(1).unwrap();
    assert_eq!(w.finish(), vec![0x02]);

    let mut w = Writer::buffer(16);
    w.write_svarint(-2).unwrap();
    assert_eq!(w.finish(), vec![0x03]);
}

#[test]
fn fixed64_little_endian() {
    let mut writer = Writer::buffer(16);
    writer.write_fixed64(0x0102_0304_0506_0708).unwrap();
    assert_eq!(
        writer.finish(),
        vec![0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]
    );
}

#[test]
fn fixed32_little_endian() {
    let mut writer = Writer::buffer(16);
    writer.write_fixed32(0x0102_0304).unwrap();
    assert_eq!(writer.finish(), vec![0x04, 0x03, 0x02, 0x01]);
}

#[test]
fn tag_encoding() {
    let mut writer = Writer::buffer(16);
    writer.write_tag(1, WIRETYPE_VARINT).unwrap();
    assert_eq!(writer.finish(), vec![0x08]);
}

#[test]
fn tag_encoding_field2_len() {
    let mut writer = Writer::buffer(16);
    writer.write_tag(2, WIRETYPE_LEN).unwrap();
    assert_eq!(writer.finish(), vec![0x12]);
}

#[test]
fn tag_encoding_large_field_number() {
    let mut writer = Writer::buffer(16);
    writer.write_tag(16, WIRETYPE_VARINT).unwrap();
    assert_eq!(writer.finish(), vec![0x80, 0x01]);
}

#[test]
fn varint_field() {
    let mut writer = Writer::buffer(16);
    writer.write_varint_field(1, 150).unwrap();
    assert_eq!(writer.finish(), vec![0x08, 0x96, 0x01]);
}

#[test]
fn bool_field() {
    let mut writer = Writer::buffer(16);
    writer.write_bool_field(1, true).unwrap();
    writer.write_bool_field(2, false).unwrap();
    assert_eq!(writer.finish(), vec![0x08, 0x01, 0x10, 0x00]);
}

#[test]
fn string_field() {
    let mut writer = Writer::buffer(32);
    writer.write_string_field(1, "hello").unwrap();
    assert_eq!(
        writer.finish(),
        vec![0x0A, 0x05, b'h', b'e', b'l', b'l', b'o']
    );
}

#[test]
fn bytes_field() {
    let mut writer = Writer::buffer(32);
    writer
        .write_bytes_field(1, &[0x01, 0x02, 0x03, 0x04])
        .unwrap();
    assert_eq!(
        writer.finish(),
        vec![0x0A, 0x04, 0x01, 0x02, 0x03, 0x04]
    );
}

#[test]
fn fixed64_field() {
    let mut writer = Writer::buffer(16);
    writer
        .write_fixed64_field(1, 0x0807_0605_0403_0201)
        .unwrap();
    assert_eq!(
        writer.finish(),
        vec![0x09, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
    );
}

#[test]
fn message_field() {
    let mut writer = Writer::buffer(64);
    writer
        .write_message_field(1, |inner| inner.write_string_field(1, "test"))
        .unwrap();
    assert_eq!(
        writer.finish(),
        vec![0x0A, 0x06, 0x0A, 0x04, b't', b'e', b's', b't']
    );
}

#[test]
fn varint_size() {
    assert_eq!(Writer::<Buffer>::varint_size(0), 1);
    assert_eq!(Writer::<Buffer>::varint_size(1), 1);
    assert_eq!(Writer::<Buffer>::varint_size(127), 1);
    assert_eq!(Writer::<Buffer>::varint_size(128), 2);
    assert_eq!(Writer::<Buffer>::varint_size(150), 2);
    assert_eq!(Writer::<Buffer>::varint_size(16383), 2);
    assert_eq!(Writer::<Buffer>::varint_size(16384), 3);
    assert_eq!(Writer::<Buffer>::varint_size(i32::MAX as u64), 5);
    assert_eq!(Writer::<Buffer>::varint_size(i64::MAX as u64), 9);
}

#[test]
fn double_field() {
    let mut writer = Writer::buffer(16);
    writer.write_double_field(1, 3.14159).unwrap();
    let out = writer.finish();
    assert_eq!(out.len(), 9);
    assert_eq!(out[0], 0x09);
}

#[test]
fn float_field() {
    let mut writer = Writer::buffer(16);
    writer.write_float_field(1, 3.14f32).unwrap();
    let out = writer.finish();
    assert_eq!(out.len(), 5);
    assert_eq!(out[0], 0x0D);
}
