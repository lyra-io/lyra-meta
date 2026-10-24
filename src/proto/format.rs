use super::pb_meta::Format;
use prost::bytes::Bytes;

impl Format {
    pub fn new(name: impl Into<String>, format_type: i32, definition: impl Into<Bytes>) -> Self {
        Self {
            id: 0, // assigned by Metadata, never selected by a caller
            name: name.into(),
            format_type,
            definition: definition.into(),
        }
    }
}
