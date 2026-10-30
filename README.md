# rprotobuf

Push-based Protocol Buffers parser and writer for Rust.

**rprotobuf** ports the [gumdrop](https://github.com/cpkb-bluezoo/gumdrop) `telemetry.protobuf` package to Rust, using the same design as [jsonparser](https://github.com/cpkb-bluezoo/jsonparser):

- Incremental `receive()` parsing out of a caller-owned [`loopbuf`](https://github.com/cpkb-bluezoo/loopbuf) ring — the parser consumes whole fields and holds none of the input itself
- Handler callbacks instead of generated message types
- Symmetric `Writer` for encoding
- Only `loopbuf` as a dependency

This is **not** a replacement for `prost` or `protobuf-rs` when you want generated structs. Use rprotobuf when you want event-driven processing: parameterise a state machine from queue records, stream fields without materialising a domain object, or build in-memory objects only where appropriate (e.g. config CDC).

## Parser

```rust
use loopbuf::LoopBuf;
use rprotobuf::decode_helpers::as_string;
use rprotobuf::{Handler, Parser, Writer};

struct JobHandler {
    job_id: Option<String>,
}

impl Handler for JobHandler {
    // `data` is a window onto the parser's ring: no copy was made
    fn handle_bytes(&mut self, field_number: u32, data: &mut LoopBuf) {
        if field_number == 1 {
            self.job_id = as_string(data).ok();
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
let mut buf = LoopBuf::with_capacity(4096);
buf.extend_from_slice(&encoded);
parser.receive(&mut buf).unwrap();
parser.close().unwrap();
```

### Streaming (caller-owned buffer contract)

`receive` takes the caller-owned `LoopBuf` and nothing else. It consumes whole
fields and leaves an incomplete one in the ring, starting at its tag. Read more
in after it and call `receive` again; reclaiming consumed space moves no data.

```rust
loop {
    let (spare, _) = buf.spare_capacity_mut();
    let n = reader.read(spare)?;
    if n == 0 {
        break;
    }
    buf.commit_write(n);
    parser.receive(&mut buf)?;
}
parser.close()?; // fails if the last field was incomplete
```

On underflow `parser.is_underflow()` is true and `close()` fails until the
rest arrives.

A bytes, string or packed field is delivered only once all of it is in the
ring, so the ring must be larger than the largest such field; a ring full of
one unfinished field makes `receive` return an error. `handle_bytes` gets the
payload as a window `&mut LoopBuf`: read it with `readable_spans()` (the
second span is empty unless the payload straddles the ring's wrap), or pass it
straight to another parser's `receive` when the field holds an encoded message.

### Untrusted input

`Parser::new` places no limit on the size a field may declare. For data you do
not trust, use `Parser::with_limits(handler, max_message_depth,
max_length_delimited_size)`: a field declaring more than the limit is rejected
as soon as its length prefix has been read, before any payload is buffered.
Nesting is limited to 100 by default. Fields that overrun their enclosing
message, tags wider than 32 bits and over-long varints are always rejected.

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
