use async_trait::async_trait;
use redis::IntoConnectionInfo;
use zenoh::Result;
use zenoh_backend_traits::{
    config::{StorageConfig, VolumeConfig},
    Capability, History, Persistence, Storage, Volume,
};
use zenoh_util::ffi::JsonValue;

use crate::{
    config::{StorageOptions, VolumeOptions},
    key_mapper::KeyMapper,
    runtime,
    storage::RedisStorage,
};

pub struct RedisVolume {
    client: redis::Client,
    prefix: String,
    status: serde_json::Value,
}

impl RedisVolume {
    pub fn new(config: &VolumeConfig) -> Result<Self> {
        let map: serde_json::Map<String, serde_json::Value> = (&config.rest).into();
        let options: VolumeOptions = serde_json::from_value(map.into())?;
        options.validate()?;
        let mut info = options
            .url
            .as_str()
            .into_connection_info()
            .map_err(|_| "invalid Redis URL")?;
        if info.redis.username.is_some() || info.redis.password.is_some() {
            return Err("Redis URL must not contain credentials; use private instead".into());
        }
        if let Some(credentials) = options.private {
            info.redis.username = credentials.username;
            info.redis.password = credentials.password;
        }
        let client = if let Some(tls) = options.tls {
            if !matches!(
                info.addr,
                redis::ConnectionAddr::TcpTls {
                    insecure: false,
                    ..
                }
            ) {
                return Err("TLS certificates require a verified rediss:// URL".into());
            }
            let root_cert = tls
                .private
                .root_ca_certificate_file
                .map(std::fs::read)
                .transpose()?;
            let client_tls = match (
                tls.private.client_certificate_file,
                tls.private.client_private_key_file,
            ) {
                (Some(cert), Some(key)) => Some(redis::ClientTlsConfig {
                    client_cert: std::fs::read(cert)?,
                    client_key: std::fs::read(key)?,
                }),
                (None, None) => None,
                _ => {
                    return Err(
                        "TLS client certificate and private key must be configured together".into(),
                    )
                }
            };
            redis::Client::build_with_tls(
                info,
                redis::TlsCertificates {
                    root_cert,
                    client_tls,
                },
            )?
        } else {
            redis::Client::open(info)?
        };
        let status = serde_json::json!({"url": options.url, "key_prefix": options.key_prefix});
        Ok(Self {
            client,
            prefix: options.key_prefix,
            status,
        })
    }
}

#[async_trait]
impl Volume for RedisVolume {
    fn get_admin_status(&self) -> JsonValue {
        (&self.status).into()
    }

    fn get_capability(&self) -> Capability {
        Capability {
            persistence: Persistence::Volatile,
            history: History::Latest,
        }
    }

    async fn create_storage(&self, config: StorageConfig) -> Result<Box<dyn Storage>> {
        if config.replication.is_some() {
            return Err(
                "Zenoh storage-manager replication is unsupported: delete events are not retained"
                    .into(),
            );
        }
        let options: StorageOptions =
            serde_json::from_value(config.volume_cfg.clone().into_serde_value())?;
        options.validate()?;
        let client = self.client.clone();
        let connection = runtime::run(async move {
            let mut connection = client.get_connection_manager().await?;
            let info: String = redis::cmd("INFO")
                .arg("cluster")
                .query_async(&mut connection)
                .await?;
            match info
                .lines()
                .find(|line| line.starts_with("cluster_enabled:"))
            {
                Some("cluster_enabled:0") => {}
                Some("cluster_enabled:1") => return Err("Redis Cluster is not supported".into()),
                _ => return Err("Redis server did not report cluster status".into()),
            }
            Ok(connection)
        })
        .await?;
        let mapper = KeyMapper::new(&self.prefix, &options.namespace);
        Ok(Box::new(RedisStorage::new(connection, mapper, &config)))
    }
}
