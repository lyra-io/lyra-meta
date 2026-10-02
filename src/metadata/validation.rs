use super::{MetadataError, Result};
use crate::proto::pb_meta::Instance;
use prost::Message;

pub(super) fn decode_instance(bytes: &[u8]) -> Result<Instance> {
    let instance = Instance::decode(bytes)?;
    if instance.initialized.is_none() {
        return Err(MetadataError::InvalidRecord(
            "initialization flag is absent",
        ));
    }
    Ok(instance)
}
