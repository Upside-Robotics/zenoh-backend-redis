use zenoh::{key_expr::OwnedKeyExpr, Result};

pub(crate) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 15) as usize] as char);
    }
    result
}

fn unhex(encoded: &str) -> Result<Vec<u8>> {
    if !encoded.len().is_multiple_of(2) {
        return Err("invalid hex key length".into());
    }
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digit = |c: u8| match c {
                b'0'..=b'9' => Ok(c - b'0'),
                b'a'..=b'f' => Ok(c - b'a' + 10),
                _ => Err("invalid hex key digit"),
            };
            Ok((digit(pair[0])? << 4) | digit(pair[1])?)
        })
        .collect::<std::result::Result<Vec<_>, &str>>()
        .map_err(Into::into)
}

#[derive(Clone, Debug)]
pub struct KeyMapper {
    base: String,
}

impl KeyMapper {
    pub fn new(prefix: &str, namespace: &str) -> Self {
        Self {
            base: format!(
                "zenoh-redis:v1:{}:{}",
                hex(prefix.as_bytes()),
                hex(namespace.as_bytes())
            ),
        }
    }

    pub fn active(&self) -> String {
        format!("{}:active", self.base)
    }

    pub fn entries_pattern(&self) -> String {
        format!("{}:entry:*", self.base)
    }

    pub fn entry(&self, key: Option<&OwnedKeyExpr>) -> String {
        match key {
            None => format!("{}:entry:n", self.base),
            Some(key) => format!("{}:entry:k{}", self.base, hex(key.as_str().as_bytes())),
        }
    }

    pub fn decode(&self, entry: &str) -> Result<Option<OwnedKeyExpr>> {
        let header = format!("{}:entry:", self.base);
        let suffix = entry
            .strip_prefix(&header)
            .ok_or("entry outside storage namespace")?;
        if suffix == "n" {
            return Ok(None);
        }
        let encoded = suffix.strip_prefix('k').ok_or("invalid entry key")?;
        let text = String::from_utf8(unhex(encoded)?)?;
        Ok(Some(OwnedKeyExpr::try_from(text)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_isolation() {
        let m = KeyMapper::new("x:%", "one:two");
        let other = KeyMapper::new("x", "%:one:two");
        let key = OwnedKeyExpr::try_from("a/b%3A".to_owned()).unwrap();
        assert_eq!(m.decode(&m.entry(None)).unwrap(), None);
        assert_eq!(m.decode(&m.entry(Some(&key))).unwrap(), Some(key));
        assert_ne!(m.active(), other.active());
        assert!(other.decode(&m.entry(None)).is_err());
        assert!(m.decode(&format!("{}:entry:k0", m.base)).is_err());
    }
}
