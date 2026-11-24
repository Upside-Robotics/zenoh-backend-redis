use serde::Deserialize;
use zenoh::Result;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsPrivate {
    pub root_ca_certificate_file: Option<String>,
    pub client_certificate_file: Option<String>,
    pub client_private_key_file: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsOptions {
    pub private: TlsPrivate,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeOptions {
    pub url: String,
    #[serde(default = "default_prefix")]
    pub key_prefix: String,
    pub private: Option<Credentials>,
    pub tls: Option<TlsOptions>,
    #[serde(default, rename = "backend")]
    pub _backend: Option<String>,
}

fn default_prefix() -> String {
    "zenoh".into()
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageOptions {
    pub namespace: String,
    #[serde(default = "default_cleanup_interval")]
    pub cleanup_interval_seconds: u64,
}

fn default_cleanup_interval() -> u64 {
    60
}

impl VolumeOptions {
    pub fn validate(&self) -> Result<()> {
        if self.key_prefix.is_empty() {
            return Err("key_prefix must not be empty".into());
        }
        if let Some(credentials) = &self.private {
            if credentials.username.as_deref().is_some_and(str::is_empty)
                || credentials.password.as_deref().is_some_and(str::is_empty)
                || (credentials.username.is_some() && credentials.password.is_none())
            {
                return Err(
                    "private credentials must contain a password when a username is specified"
                        .into(),
                );
            }
        }
        if let Some(tls) = &self.tls {
            if tls.private.client_certificate_file.is_some()
                != tls.private.client_private_key_file.is_some()
            {
                return Err(
                    "TLS client certificate and private key must be configured together".into(),
                );
            }
        }
        Ok(())
    }
}

impl StorageOptions {
    pub fn validate(&self) -> Result<()> {
        if self.namespace.is_empty() {
            return Err("storage namespace must not be empty".into());
        }
        if self.cleanup_interval_seconds == 0 || self.cleanup_interval_seconds > u32::MAX as u64 {
            return Err("cleanup interval must be positive and fit in u32".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_credentials_and_tls() {
        let valid: VolumeOptions = serde_json::from_value(serde_json::json!({
            "url": "redis://localhost",
            "private": {"password": "test"}
        }))
        .unwrap();
        assert!(valid.validate().is_ok());
        let missing_password: VolumeOptions = serde_json::from_value(serde_json::json!({
            "url": "redis://localhost",
            "private": {"username": "user"}
        }))
        .unwrap();
        assert!(missing_password.validate().is_err());
        let incomplete_tls: VolumeOptions = serde_json::from_value(serde_json::json!({
            "url": "rediss://localhost",
            "tls": {"private": {"client_certificate_file": "client.pem"}}
        }))
        .unwrap();
        assert!(incomplete_tls.validate().is_err());
    }

    #[test]
    fn validates_storage_settings() {
        let valid: StorageOptions = serde_json::from_value(serde_json::json!({
            "namespace": "x"
        }))
        .unwrap();
        assert!(valid.validate().is_ok());
        for value in [
            serde_json::json!({"namespace": ""}),
            serde_json::json!({"namespace": "x", "cleanup_interval_seconds": 0}),
        ] {
            assert!(serde_json::from_value::<StorageOptions>(value)
                .unwrap()
                .validate()
                .is_err());
        }
        let unknown = serde_json::json!({"namespace": "x", "unsupported_option": 1});
        assert!(serde_json::from_value::<StorageOptions>(unknown).is_err());
    }
}
