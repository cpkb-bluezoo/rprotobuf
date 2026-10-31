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

// the writer fills the ring and the parser reads that same ring
let mut buf = LoopBuf::with_capacity(4096);
Writer::new().write_string_field(&mut buf, 1, "job-abc").unwrap();

let mut handler = JobHandler { job_id: None };
let mut parser = Parser::new(&mut handler);
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

The writer is the mirror of `receive(&mut LoopBuf)`: it encodes straight into a
caller-owned `LoopBuf` and keeps no buffer of its own.

```rust
use loopbuf::LoopBuf;
use rprotobuf::Writer;

let mut ring = LoopBuf::with_capacity(4096);
let mut w = Writer::new();
w.write_varint_field(&mut ring, 1, 150)?;
w.write_string_field(&mut ring, 2, "hello")?;
w.write_message_field(&mut ring, 3, |inner, ring| {
    inner.write_bool_field(ring, 1, true)
})?;
// `ring` now holds the message: drain it to a socket or file, or parse it
```

Every call is **atomic**: it appends the whole field (tag and value together) or
nothing. `err.is_would_block()` means the ring has no room right now: drain it
and repeat the same call. A field larger than the ring's whole capacity can
never fit and is an `InvalidInput` error; as the parser reads a bytes field
whole, so the writer writes it whole, and the ring must be larger than the
largest.

An embedded message is written in place, with no scratch buffer: the tag and a
reserved length, then the content straight into the ring, then the length is
filled in. If the content fails (the ring fills up, say) everything written
since the field began is taken back, so the message as a whole is atomic and the
call can be repeated. This means the message must fit the ring, and the closure
must only append to it, never consume from it.

Because the length is not known until the content is written, it is a varint
padded to a fixed 5 bytes (an empty message is a plain zero). That is valid on
the wire and accepted by every conforming parser, but it is not the shortest
encoding, so the bytes differ from other encoders' output for the same message.

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
