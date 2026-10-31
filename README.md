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

let mut parser = Parser::new(JobHandler { job_id: None });
parser.receive(&mut buf).unwrap();
parser.close().unwrap();
let handler = parser.into_handler();
```

The parser owns its handler: `handler()` and `handler_mut()` reach it between
calls and `into_handler()` takes it back. To keep it yourself instead, lend
it: `Parser::new(&mut handler)` works because `&mut H` (and `Box<H>`, so
`Box<dyn Handler>` too) is a `Handler` whenever `H` is.

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
caller-owned `LoopBuf` and keeps no copy of the output.

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

// drain to a socket or file (any `io::Write`, another `LoopBuf` included)
w.drain_to(&mut ring, &mut socket)?;
```

Every call is **atomic**: it appends the whole field (tag and value together) or
nothing. `err.is_would_block()` means the ring has no room right now: drain it
and repeat the same call. A field larger than the ring's whole capacity can
never fit and is an `InvalidInput` error; as the parser reads a bytes field
whole, so the writer writes it whole, and the ring must be larger than the
largest.

### Embedded messages and `drain_to`

The length of an embedded message goes before its content but is known only
after it. So it is not written into the ring: the ring holds the message's
content, the writer remembers each tag and length prefix and where it goes, and
`drain_to` writes the prefix to the sink and then the content from the ring, in
order. The prefix is the shortest varint, as any other encoder writes it, no
byte is moved, and a prefix takes no room in the ring.

If the content fails (the ring fills up, say) everything written since the
field began is taken back, prefixes included, so the message as a whole is
atomic and the call can be repeated. This means a message's content must fit the
ring, and the closure must only append to it, never consume from it.

Because of this, a ring that has had embedded messages written to it is drained
with `drain_to`, not read directly (`has_pending_prefixes()` says whether it
matters). A ring that has had only scalar and bytes fields written to it holds
exactly the output and can be read directly, and consuming it that way is
tracked, so a message written afterwards is still right. Reading the ring past a
waiting prefix is detected and refused.

`drain_to` stops when the ring is empty or the sink will take no more. If the
sink reports `WouldBlock` after some bytes went, those are counted and `Ok`
returned; if none did, the error is. Nothing is lost either way: call it again
when the sink is ready.

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
