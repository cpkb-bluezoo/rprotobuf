/// Limits guarding a parser against hostile input.
///
/// A limit of `None` is no limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserLimits {
    /// The deepest nesting of embedded messages. Default: 100.
    pub max_nesting_depth: Option<usize>,
    /// The largest payload a length-delimited field (bytes, a string, a packed
    /// field, an embedded message) may declare. It is checked as soon as the
    /// length has been read, before any of the payload. Default: none, apart
    /// from the `i32::MAX` the wire format allows.
    ///
    /// A bytes field is streamed to the handler a piece at a time, so its size
    /// costs the parser nothing; this is a cap for a handler that accumulates
    /// what it is given.
    pub max_field_length: Option<usize>,
}

impl Default for ParserLimits {
    fn default() -> Self {
        Self {
            max_nesting_depth: Some(100),
            max_field_length: None,
        }
    }
}

impl ParserLimits {
    /// No limit at all.
    pub fn unlimited() -> Self {
        Self {
            max_nesting_depth: None,
            max_field_length: None,
        }
    }
}
