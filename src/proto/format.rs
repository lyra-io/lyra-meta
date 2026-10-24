use super::pb_meta::Format;
use prost::bytes::Bytes;

impl Format {
    pub fn new(
        database_id: u32,
        name: impl Into<String>,
        format_type: i32,
        definition: impl Into<Bytes>,
    ) -> Self {
        Self {
            id: 0, // assigned by Metadata, never selected by a caller
            database_id,
            name: name.into(),
            format_type,
            definition: definition.into(),
        }
    }
}
