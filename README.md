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
use rprotobuf::decode_helpers::BytesCollector;
use rprotobuf::{Handler, ParseResult, Parser, Writer};

struct JobHandler {
    field: u32,
    collector: BytesCollector,
    job_id: Option<String>,
}

impl Handler for JobHandler {
    // A bytes or string field arrives as start, any number of pieces, end.
    fn start_bytes(&mut self, field_number: u32, length: u64) -> ParseResult<()> {
        self.field = field_number;
        self.collector.start(length);
        Ok(())
    }

    // `data` is a window onto the parser's ring: no copy was made
    fn bytes_content(&mut self, data: &mut LoopBuf) -> ParseResult<()> {
        self.collector.push(data); // copies it out and consumes it
        Ok(())
    }

    fn end_bytes(&mut self) -> ParseResult<()> {
        if self.field == 1 {
            self.job_id = self.collector.take_string().ok();
        }
        Ok(())
    }
}

// the writer fills the ring and the parser reads that same ring
let mut buf = LoopBuf::with_capacity(4096);
Writer::new().write_string_field(&mut buf, 1, "job-abc").unwrap();

let mut parser = Parser::new(JobHandler { field: 0, collector: BytesCollector::default(), job_id: None });
parser.receive(&mut buf).unwrap();
parser.close(&mut buf).unwrap();
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
parser.close(&mut buf)?; // fails if the last field was incomplete
```

On underflow `parser.is_underflow()` is true and `close()` fails until the
rest arrives.

### Bytes, strings and packed fields

A length-delimited field that is not an embedded message is **streamed**:
`start_bytes(field, length)`, any number of `bytes_content(window)` pieces, then
`end_bytes()`. A payload of any size passes through a ring of any size; the
ring need only hold one scalar field (about fifteen bytes), since embedded
messages are parsed as ordinary fields in place. A ring full of one unfinished
scalar field makes `receive` return an error.

`bytes_content` gets the piece as a window onto the ring, with no copy, even if
it straddles the wrap: read it with `readable_spans()`, or hand it straight to
another parser's `receive` when the field holds data in a format that has one.
**What the handler advances past is consumed; what it leaves is offered again,
at the front of the next piece.** A handler that copies the piece must consume
it too; `BytesCollector` does both for one that wants the whole field. The
default consumes everything. A handler that leaves every byte when the ring
cannot take more is an error, and what it leaves of the last piece is dropped.

Every handler method returns a `ParseResult`, so a handler can stop the parse.

### Untrusted input

`ParserLimits` (passed to `Parser::with_limits` or `set_limits`) holds the
limits, each `None` for no limit: `max_nesting_depth` (default 100) and
`max_field_length` (default none), which rejects a field declaring more as soon
as its length prefix has been read, before any payload. A bytes field streams,
so its size costs the parser nothing; set the limit when the handler
accumulates what it is given. Fields that overrun their enclosing message, tags
wider than 32 bits and over-long varints are always rejected.

`parser.parse(reader)` reads a whole message from any `io::Read` through a ring
of `set_buffer_size` bytes (8 KiB by default).

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

### Bytes and strings of any length

`write_bytes_field` and `write_string_field` write a whole field, which must fit
the ring. For one that may not, write it in pieces, named for the parser's
events: `write_start_bytes(field, length)`, any number of
`write_bytes_content(data)` calls that take what fits and return how many bytes
that was, then `write_end_bytes()`. Protobuf puts the length first, so it must be
known up front (for a string, its UTF-8 length) but the payload need not fit
anything. Writing more than the length declared, or ending short of it, is an
`InvalidInput` error, and nothing else can be written while the field is open.
Inside `write_message_field` the message must fit the ring as a whole, so a
field written in pieces there is bounded by the ring too.

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
