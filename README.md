# rprotobuf

Push-based Protocol Buffers parser and writer for Rust.

**rprotobuf** ports the [gumdrop](https://github.com/cpkb-bluezoo/gumdrop) `telemetry.protobuf` package to Rust, using the same design as [jsonparser](https://github.com/cpkb-bluezoo/jsonparser):

- Incremental `receive()` parsing — constant memory, no internal buffering of incomplete fields
- Handler callbacks instead of generated message types
- Symmetric `Writer` for encoding
- Zero dependencies beyond the Rust standard library

This is **not** a replacement for `prost` or `protobuf-rs` when you want generated structs. Use rprotobuf when you want event-driven processing: parameterise a state machine from queue records, stream fields without materialising a domain object, or build in-memory objects only where appropriate (e.g. config CDC).

## Parser

```rust
use rprotobuf::{Handler, Parser, Writer};

struct JobHandler {
    job_id: Option<String>,
}

impl Handler for JobHandler {
    fn handle_bytes(&mut self, field_number: u32, data: &[u8]) {
        if field_number == 1 {
            self.job_id = String::from_utf8(data.to_vec()).ok();
        }
    }
}

let encoded = {
    let mut w = Writer::buffer(64);
    w.write_string_field(1, "job-abc").unwrap();
    w.finish()
};

let mut handler = JobHandler { job_id: None };
let mut parser = Parser::new(&mut handler);
let mut input = encoded.as_slice();
parser.receive(&mut input).unwrap();
parser.close().unwrap();
```

### Streaming (NIO-style buffer contract)

```rust
// Same compact/flip pattern as jsonparser and gumdrop:
loop {
    // read more bytes into `buf`...
    let mut slice = &buf[..filled];
    parser.receive(&mut slice)?;
    // `slice` advanced; keep unconsumed suffix for next read
}
```

On underflow, `parser.is_underflow()` is true and `close()` fails until more data arrives.

## Writer

```rust
use rprotobuf::Writer;

let mut w = Writer::buffer(128);
w.write_varint_field(1, 150)?;
w.write_string_field(2, "hello")?;
w.write_message_field(3, |inner| {
    inner.write_bool_field(1, true)
})?;
let bytes = w.finish();
```

## Relationship to other bluezoo libraries

| Library | Format | Pattern |
|---------|--------|---------|
| [jsonparser](https://github.com/cpkb-bluezoo/jsonparser) | JSON | `JSONContentHandler` + `receive` |
| [gumdrop](https://github.com/cpkb-bluezoo/gumdrop) | Protobuf (Java) | `ProtobufHandler` + `receive` |
| **rprotobuf** | Protobuf (Rust) | `Handler` + `receive` |
| [gonzalez](https://github.com/cpkb-bluezoo/gonzalez) | XML/XPath | SAX `ContentHandler` |

## License

LGPL-2.1-or-later (see [LICENSE](LICENSE)).

## Development

```bash
cargo test
cargo doc --open
```

## Publishing

```bash
cargo publish   # crates.io, when ready
```

Or depend via git until the first release:

```toml
rprotobuf = { git = "https://github.com/cpkb-bluezoo/rprotobuf", tag = "v0.1.0" }
```
