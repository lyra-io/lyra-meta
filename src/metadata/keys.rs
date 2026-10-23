use super::{MetadataError, Result, validate_name};
use std::fmt::Write;
use uuid::Uuid;

pub(crate) const CATALOG: &str = "/catalog";
pub(crate) const PARTITION: &str = "catalog";

#[derive(Clone, Copy)]
pub(crate) enum Collection {
    Users,
    Databases,
}

impl Collection {
    pub(crate) fn prefix(self) -> &'static str {
        match self {
            Self::Users => "/catalog/users/",
            Self::Databases => "/catalog/databases/",
        }
    }

    pub(crate) fn allocator(self) -> &'static str {
        match self {
            Self::Users => "/catalog/allocator/user",
            Self::Databases => "/catalog/allocator/database",
        }
    }

    pub(crate) fn range(self) -> (String, String) {
        (self.prefix().into(), format!("{}~", self.prefix()))
    }

    pub(crate) fn key(self, name: &str) -> Result<String> {
        validate_name(name)?;
        // Bijection over canonical UTF-8 bytes, without path/escaping aliases.
        let mut key = String::with_capacity(self.prefix().len() + name.len() * 2);
        key.push_str(self.prefix());
        for byte in name.bytes() {
            write!(&mut key, "{byte:02x}").unwrap();
        }
        Ok(key)
    }
}

pub(crate) fn validate_component(component: &str) -> Result<()> {
    if component.is_empty()
        || component.len() > 32
        || !component.as_bytes()[0].is_ascii_lowercase()
        || !component
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
    {
        return Err(MetadataError::InvalidRecord("invalid component type"));
    }
    Ok(())
}

pub(crate) fn registration_key(component: &str, id: Uuid) -> Result<String> {
    validate_component(component)?;
    Ok(format!("/discovery/{component}/instances/{id}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_names_have_distinct_safe_keys() {
        for kind in [Collection::Users, Collection::Databases] {
            let names = [
                "public",
                "Public",
                "a/b",
                "a%2fb",
                "a.b",
                "数据库",
                "é",
                "e\u{301}",
            ];
            let keys: std::collections::HashSet<_> =
                names.iter().map(|name| kind.key(name).unwrap()).collect();
            assert_eq!(keys.len(), names.len());
            let (first, last) = kind.range();
            for key in keys {
                assert!(key > first && key < last);
                assert!(
                    key.strip_prefix(kind.prefix())
                        .unwrap()
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit())
                );
            }
            assert!(kind.key("").is_err());
            assert!(kind.key("bad\0name").is_err());
        }
        assert_eq!(
            Collection::Databases.key("public").unwrap(),
            "/catalog/databases/7075626c6963"
        );
    }
}
