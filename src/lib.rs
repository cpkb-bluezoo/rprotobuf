//! Push-based Protocol Buffers parser and writer.
//!
//! Port of the gumdrop `telemetry.protobuf` package (same design as
//! [jsonparser](https://github.com/cpkb-bluezoo/jsonparser)): incremental
//! `receive()` parsing with handler callbacks, no generated code, no message
//! domain objects required.
//!
//! # Parser usage
//!
//! ```no_run
//! use rprotobuf::{DefaultHandler, Parser};
//!
//! struct MyHandler;
//! impl rprotobuf::Handler for MyHandler {
//!     fn handle_varint(&mut self, field_number: u32, value: u64) {
//!         let _ = (field_number, value);
//!     }
//!     // ... other methods with defaults
//! }
//!
//! let mut handler = MyHandler;
//! let mut parser = Parser::new(&mut handler);
//! let mut input = &[0x08u8, 0x96, 0x01][..]; // field 1 = 150
//! parser.receive(&mut input).unwrap();
//! parser.close().unwrap();
//! ```

mod buffer;
mod error;
mod handler;
mod parser;
mod wire;
mod writer;

pub use buffer::Buffer;
pub use error::{ParseError, WriteError};
pub use handler::{decode_helpers, DefaultHandler, Handler};
pub use parser::Parser;
pub use wire::{WireType, WIRETYPE_I32, WIRETYPE_I64, WIRETYPE_LEN, WIRETYPE_VARINT};
pub use writer::Writer;
