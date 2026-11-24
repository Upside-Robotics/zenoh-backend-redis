use std::time::{SystemTime, UNIX_EPOCH};

use zenoh::{
    bytes::{Encoding, ZBytes},
    time::{Timestamp, TimestampId, NTP64},
};
use zenoh_backend_redis::RedisBackend;
use zenoh_backend_traits::config::PluginConfig;
use zenoh_plugin_trait::Plugin;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires authenticated Redis with TLS and a test CA certificate"]
async fn authenticated_tls_connection() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = std::env::var("REDIS_TEST_TLS_URL")?;
    let ca = std::env::var("REDIS_TEST_TLS_CA")?;
    let password = std::env::var("REDIS_TEST_TLS_PASSWORD")?;
    let namespace = format!(
        "tls-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let json = serde_json::json!({
        "volumes": {"redis": {
            "url": url,
            "key_prefix": "tls-test",
            "private": {"username": "default", "password": password},
            "tls": {"private": {"root_ca_certificate_file": ca}}
        }},
        "storages": {"test": {
            "key_expr": "test/**", "strip_prefix": "test",
            "volume": {"id": "redis", "namespace": namespace}
        }}
    });
    let config = PluginConfig::try_from(("storage_manager", &json))?;
    let volume = RedisBackend::start("redis", &config.volumes[0])?;
    let status: serde_json::Value = volume.get_admin_status().into();
    assert!(!status.to_string().contains(&password));
    let mut storage = volume.create_storage(config.storages[0].clone()).await?;
    let timestamp = Timestamp::new(NTP64(1), TimestampId::try_from(1u8)?);
    storage
        .put(
            None,
            ZBytes::from("secured"),
            Encoding::default(),
            timestamp,
        )
        .await?;
    assert_eq!(
        storage.get(None, "").await?[0].payload.to_bytes().as_ref(),
        b"secured"
    );
    drop(storage);

    use redis::{ClientTlsConfig, IntoConnectionInfo, TlsCertificates};
    let mut info = url.as_str().into_connection_info()?;
    info.redis.username = Some("default".into());
    info.redis.password = Some(password);
    let client = redis::Client::build_with_tls(
        info,
        TlsCertificates {
            root_cert: Some(std::fs::read(ca)?),
            client_tls: None::<ClientTlsConfig>,
        },
    )?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    let hex = |s: &str| s.bytes().map(|b| format!("{b:02x}")).collect::<String>();
    let base = format!("zenoh-redis:v1:{}:{}", hex("tls-test"), hex(&namespace));
    let _: usize = redis::cmd("DEL")
        .arg(format!("{base}:entry:n"))
        .arg(format!("{base}:active"))
        .query_async(&mut connection)
        .await?;
    Ok(())
}
