use super::{MetadataError, Result};

/// Startup/API names are exact UTF-8 strings, not SQL identifiers.
pub fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 63 || name.contains('\0') {
        return Err(MetadataError::InvalidName);
    }
    Ok(())
}

/// Normalize a parsed SQL identifier. The caller emits PostgreSQL's truncation
/// notice when the returned boolean is true. Quoted spelling is preserved.
pub fn normalize_sql_identifier(name: &str, quoted: bool) -> Result<(String, bool)> {
    if name.is_empty() || name.contains('\0') {
        return Err(MetadataError::InvalidName);
    }
    let mut name = if quoted {
        name.to_string()
    } else {
        name.to_ascii_lowercase()
    };
    let truncated = name.len() > 63;
    if truncated {
        let mut length = 63;
        while !name.is_char_boundary(length) {
            length -= 1;
        }
        name.truncate(length);
    }
    validate_name(&name)?;
    Ok((name, truncated))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_exact_names_and_normalizes_only_sql_identifiers() {
        for name in ["", "a\0b", &"x".repeat(64), &"é".repeat(32)] {
            assert!(validate_name(name).is_err());
        }
        for name in ["MixedCase", "a/b", "名字", "with space", &"x".repeat(63)] {
            validate_name(name).unwrap();
        }
        assert_eq!(
            normalize_sql_identifier("PUBLIC", false).unwrap(),
            ("public".into(), false)
        );
        assert_eq!(
            normalize_sql_identifier("PUBLIC", true).unwrap(),
            ("PUBLIC".into(), false)
        );
        assert_eq!(
            normalize_sql_identifier(&"é".repeat(32), true).unwrap(),
            ("é".repeat(31), true)
        );
    }
}
