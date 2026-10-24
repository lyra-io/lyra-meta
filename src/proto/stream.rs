use super::pb_meta::Stream;

impl Stream {
    pub fn new(database_id: u32, schema_name: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: 0, // assigned by Metadata, never selected by a caller
            database_id,
            schema_name: schema_name.into(),
            name: name.into(),
            format_name: None,
            replication: None,
            fragments: Vec::new(),
        }
    }

    pub fn effective_replication(&self) -> u32 {
        self.replication.unwrap_or(2)
    }
}
