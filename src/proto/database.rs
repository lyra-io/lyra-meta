use super::pb_meta::{Database, DatabaseState};

impl Database {
    pub fn new(name: impl Into<String>, owner_user_id: u32) -> Self {
        Self {
            name: name.into(),
            owner_user_id,
            allow_connections: Some(true),
            connection_limit: Some(-1),
            state: DatabaseState::Ready as i32,
        }
    }

    pub fn accepts_connections(&self) -> bool {
        self.allow_connections.unwrap_or(true)
    }
    pub fn effective_connection_limit(&self) -> i32 {
        self.connection_limit.unwrap_or(-1)
    }
}
