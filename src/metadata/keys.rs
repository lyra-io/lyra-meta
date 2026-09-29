use super::{MetadataError, Result};
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
            Self::Users => "/catalog/users/x",
            Self::Databases => "/catalog/databases/x",
        }
    }

    pub(crate) fn index(self) -> &'static str {
        match self {
            Self::Users => "lyra.user.name",
            Self::Databases => "lyra.database.name",
        }
    }

    pub(crate) fn range(self) -> (String, String) {
        let collection = self.prefix().strip_suffix('x').unwrap();
        (collection.to_string(), format!("{collection}/"))
    }

    pub(crate) fn key(self, id: u32) -> Result<String> {
        if id == 0 {
            return Err(MetadataError::InvalidRecord("zero object ID"));
        }
        Ok(format!("{}-{id:020}", self.prefix()))
    }

    pub(crate) fn id(self, key: &str) -> Result<u32> {
        let suffix = key
            .strip_prefix(self.prefix())
            .and_then(|s| s.strip_prefix('-'))
            .filter(|s| s.len() == 20 && s.bytes().all(|b| b.is_ascii_digit()))
            .ok_or(MetadataError::InvalidRecord("invalid object key"))?;
        suffix
            .parse::<u32>()
            .ok()
            .filter(|id| *id > 0)
            .ok_or(MetadataError::InvalidRecord(
                "object ID outside uint32 range",
            ))
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
    fn enforces_canonical_ids() {
        for kind in [Collection::Users, Collection::Databases] {
            for id in [1, u32::MAX] {
                assert_eq!(kind.id(&kind.key(id).unwrap()).unwrap(), id);
            }
            assert!(kind.key(0).is_err());
            assert!(kind.id(&format!("{}-1", kind.prefix())).is_err());
            assert!(
                kind.id(&format!("{}-00000000004294967296", kind.prefix()))
                    .is_err()
            );
        }
        assert!(
            Collection::Users
                .id(&Collection::Databases.key(1).unwrap())
                .is_err()
        );
    }
}
