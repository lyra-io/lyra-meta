use super::pb_meta::Stream;

impl Stream {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            id: 0, // assigned by Metadata, never selected by a caller
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
