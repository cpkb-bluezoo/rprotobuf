//! Push-based Protocol Buffers parser and writer.
//!
//! Port of the gumdrop `telemetry.protobuf` package (same design as
//! [jsonparser](https://github.com/cpkb-bluezoo/jsonparser)): incremental
//! `receive()` parsing with handler callbacks, no generated code, no message
//! domain objects required.
//!
//! # Parser usage
//!
//! ```
//! use loopbuf::LoopBuf;
//! use rprotobuf::Parser;
//!
//! struct MyHandler(u64);
//! impl rprotobuf::Handler for MyHandler {
//!     fn handle_varint(&mut self, _field_number: u32, value: u64) {
//!         self.0 = value;
//!     }
//!     // ... other methods with defaults
//! }
//!
//! let mut handler = MyHandler(0);
//! let mut parser = Parser::new(&mut handler);
//! let mut buf = LoopBuf::with_capacity(4096);
//! buf.extend_from_slice(&[0x08, 0x96, 0x01]); // field 1 = 150
//! parser.receive(&mut buf).unwrap();
//! parser.close().unwrap();
//! assert_eq!(handler.0, 150);
//! ```
//!
//! `receive` takes the caller-owned [`loopbuf::LoopBuf`] ring and nothing
//! else. It consumes whole fields and leaves an incomplete one in the ring to
//! be presented again with more data.

mod error;
mod handler;
mod parser;
mod wire;
mod writer;

pub use error::{ParseError, ParseResult, WriteError, WriteResult};
pub use handler::{decode_helpers, DefaultHandler, Handler};
pub use parser::{Parser, DEFAULT_MAX_MESSAGE_DEPTH, UNLIMITED_LENGTH_DELIMITED};
pub use wire::{WireType, WIRETYPE_I32, WIRETYPE_I64, WIRETYPE_LEN, WIRETYPE_VARINT};
pub use writer::Writer;
