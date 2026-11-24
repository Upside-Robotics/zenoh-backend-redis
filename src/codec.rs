use zenoh::{
    bytes::Encoding,
    internal::buffers::ZSlice,
    time::{Timestamp, TimestampId, NTP64},
    Result,
};

pub fn timestamp_bytes(timestamp: &Timestamp) -> Vec<u8> {
    let mut bytes = timestamp.get_time().as_u64().to_be_bytes().to_vec();
    bytes.extend_from_slice(&timestamp.get_id().to_le_bytes());
    bytes
}

pub fn timestamp_from_bytes(bytes: &[u8]) -> Result<Timestamp> {
    if bytes.len() != 24 {
        return Err("invalid timestamp length".into());
    }
    let time = u64::from_be_bytes(bytes[..8].try_into()?);
    let id = TimestampId::try_from(&bytes[8..])?;
    Ok(Timestamp::new(NTP64(time), id))
}

pub fn encoding_parts(encoding: &Encoding) -> (u16, bool, Vec<u8>) {
    (
        encoding.id(),
        encoding.schema().is_some(),
        encoding.schema().map(|s| s.to_vec()).unwrap_or_default(),
    )
}

pub fn encoding_from_parts(id: u16, has_schema: bool, schema: Vec<u8>) -> Encoding {
    Encoding::new(id, has_schema.then(|| ZSlice::from(schema)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_bytes_preserve_timestamp_order() {
        let samples = [
            Timestamp::new(NTP64(7), TimestampId::try_from(256u16).unwrap()),
            Timestamp::new(NTP64(7), TimestampId::try_from(1u8).unwrap()),
            Timestamp::new(NTP64(8), TimestampId::try_from(1u8).unwrap()),
        ];
        for a in samples {
            assert_eq!(timestamp_from_bytes(&timestamp_bytes(&a)).unwrap(), a);
            for b in samples {
                assert_eq!(a.cmp(&b), timestamp_bytes(&a).cmp(&timestamp_bytes(&b)));
            }
        }
        assert!(timestamp_from_bytes(&[0; 24]).is_err());
    }
}
