use super::pb_meta::{ScramSha256Verifier, User};
use std::fmt::{Debug, Formatter, Result};

impl Debug for User {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> Result {
        formatter.write_str("User { [REDACTED] }")
    }
}

impl Debug for ScramSha256Verifier {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> Result {
        formatter.write_str("ScramSha256Verifier { [REDACTED] }")
    }
}
