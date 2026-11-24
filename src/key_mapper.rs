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

fn is_unescaped(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'/')
}

fn escape(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789ABCDEF";
    let mut result = String::with_capacity(bytes.len());
    for &byte in bytes {
        if is_unescaped(byte) {
            result.push(char::from(byte));
        } else {
            result.push('%');
            result.push(char::from(DIGITS[(byte >> 4) as usize]));
            result.push(char::from(DIGITS[(byte & 15) as usize]));
        }
    }
    result
}

fn unescape(encoded: &str) -> Result<Vec<u8>> {
    fn digit(byte: u8) -> Result<u8> {
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'A'..=b'F' => Ok(byte - b'A' + 10),
            _ => Err("invalid escape digit".into()),
        }
    }

    let mut result = Vec::with_capacity(encoded.len());
    let mut bytes = encoded.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let high = bytes.next().ok_or("incomplete escape")?;
            let low = bytes.next().ok_or("incomplete escape")?;
            let decoded = (digit(high)? << 4) | digit(low)?;
            if is_unescaped(decoded) {
                return Err("non-canonical escape".into());
            }
            result.push(decoded);
        } else if is_unescaped(byte) {
            result.push(byte);
        } else {
            return Err("invalid unescaped byte".into());
        }
    }
    Ok(result)
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
                escape(prefix.as_bytes()),
                escape(namespace.as_bytes())
            ),
        }
    }

    pub fn entries_pattern(&self) -> String {
        format!("{}:entry:*", self.base)
    }

    pub fn entry(&self, key: Option<&OwnedKeyExpr>) -> String {
        match key {
            None => format!("{}:entry:n", self.base),
            Some(key) => format!("{}:entry:k{}", self.base, escape(key.as_str().as_bytes())),
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
        let text = String::from_utf8(unescape(encoded)?)?;
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
        assert_eq!(
            m.entry(Some(&key)),
            "zenoh-redis:v1:x%3A%25:one%3Atwo:entry:ka/b%253A"
        );
        assert_eq!(m.entry(None), "zenoh-redis:v1:x%3A%25:one%3Atwo:entry:n");
        assert_eq!(m.decode(&m.entry(None)).unwrap(), None);
        assert_eq!(m.decode(&m.entry(Some(&key))).unwrap(), Some(key));
        assert_ne!(m.entries_pattern(), other.entries_pattern());
        assert!(other.decode(&m.entry(None)).is_err());
        for suffix in ["k%", "k%2", "k%3a", "k%GG", "k%41", "k:", "k", "x"] {
            assert!(m.decode(&format!("{}:entry:{suffix}", m.base)).is_err());
        }
    }

    #[test]
    fn escape_round_trips_all_bytes_and_avoids_glob_metacharacters() {
        let bytes: Vec<u8> = (0..=u8::MAX).collect();
        let encoded = escape(&bytes);
        assert_eq!(unescape(&encoded).unwrap(), bytes);
        assert_eq!(
            escape(b"AZaz09-_.~/:%*?[]\\"),
            "AZaz09-_.~/%3A%25%2A%3F%5B%5D%5C"
        );
        assert_eq!(escape("café".as_bytes()), "caf%C3%A9");
        let mapper = KeyMapper::new("*?[]\\", "a:b");
        assert_eq!(
            mapper.entries_pattern(),
            "zenoh-redis:v1:%2A%3F%5B%5D%5C:a%3Ab:entry:*"
        );
    }
}
