use std::time::{SystemTime, UNIX_EPOCH};

use redis::AsyncCommands;
use zenoh::{
    bytes::{Encoding, ZBytes},
    key_expr::OwnedKeyExpr,
    time::{Timestamp, TimestampId, NTP64},
};
use zenoh_backend_redis::RedisBackend;
use zenoh_backend_traits::{config::PluginConfig, StorageInsertionResult};
use zenoh_plugin_trait::Plugin;

fn timestamp(time: u64) -> Timestamp {
    Timestamp::new(NTP64(time), TimestampId::try_from(1u8).unwrap())
}

fn key(value: &str) -> Option<OwnedKeyExpr> {
    Some(OwnedKeyExpr::try_from(value.to_owned()).unwrap())
}

fn hex(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn redis_storage_ordering_discovery_and_recovery(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
    let namespace = format!(
        "test-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let json = serde_json::json!({
        "volumes": {"redis": {"url": url, "key_prefix": "integration"}},
        "storages": {"test": {
            "key_expr": "test/**", "strip_prefix": "test",
            "volume": {"id": "redis", "namespace": namespace, "cleanup_interval_seconds": 1}
        }}
    });
    let config = PluginConfig::try_from(("storage_manager", &json))?;
    let volume = RedisBackend::start("redis", &config.volumes[0])?;
    let mut connection = redis::Client::open(url.as_str())?
        .get_multiplexed_async_connection()
        .await?;
    let base = format!("zenoh-redis:v1:{}:{}", hex("integration"), hex(&namespace));

    let mut replicated = config.storages[0].clone();
    replicated.replication = Some(Default::default());
    assert!(volume.create_storage(replicated).await.is_err());
    let mut storage = volume.create_storage(config.storages[0].clone()).await?;
    let mut second = volume.create_storage(config.storages[0].clone()).await?;
    let none_entry = format!("{base}:entry:n");
    assert!(matches!(
        storage
            .put(
                None,
                ZBytes::from(vec![0, 255]),
                Encoding::TEXT_PLAIN.with_schema("utf-8"),
                timestamp(10)
            )
            .await?,
        StorageInsertionResult::Inserted
    ));
    assert!(matches!(
        storage
            .put(None, ZBytes::from("old"), Encoding::default(), timestamp(9))
            .await?,
        StorageInsertionResult::Outdated
    ));
    assert!(matches!(
        storage.delete(None, timestamp(9)).await?,
        StorageInsertionResult::Outdated
    ));
    assert_eq!(
        storage.get(None, "").await?[0].payload.to_bytes().as_ref(),
        &[0, 255]
    );
    assert_eq!(
        storage.get(None, "").await?[0].encoding,
        Encoding::TEXT_PLAIN.with_schema("utf-8")
    );
    assert!(storage.get(None, "_time=now").await.is_err());
    let ttl: i64 = connection.ttl(&none_entry).await?;
    assert_eq!(ttl, -1);
    let _: usize = connection
        .srem(format!("{base}:active"), &none_entry)
        .await?;
    let repaired = volume.create_storage(config.storages[0].clone()).await?;
    assert_eq!(repaired.get_all_entries().await?.len(), 1);
    drop(repaired);
    assert!(matches!(
        storage.delete(None, timestamp(11)).await?,
        StorageInsertionResult::Deleted
    ));
    assert!(storage.get(None, "").await?.is_empty());
    assert!(storage.get_all_entries().await?.is_empty());
    let exists: bool = connection.exists(&none_entry).await?;
    assert!(!exists);
    assert!(matches!(
        storage
            .put(
                None,
                ZBytes::from("late-before-restart"),
                Encoding::default(),
                timestamp(10)
            )
            .await?,
        StorageInsertionResult::Inserted
    ));
    assert!(matches!(
        storage.delete(None, timestamp(11)).await?,
        StorageInsertionResult::Deleted
    ));
    drop(storage);
    let mut storage = volume.create_storage(config.storages[0].clone()).await?;
    assert!(matches!(
        storage
            .put(
                None,
                ZBytes::from("late"),
                Encoding::default(),
                timestamp(10)
            )
            .await?,
        StorageInsertionResult::Inserted
    ));
    assert_eq!(
        storage.get(None, "").await?[0].payload.to_bytes().as_ref(),
        b"late"
    );

    let (a, b) = tokio::join!(
        storage.put(
            key("race"),
            ZBytes::from("one"),
            Encoding::default(),
            timestamp(20)
        ),
        second.put(
            key("race"),
            ZBytes::from("two"),
            Encoding::default(),
            timestamp(21)
        ),
    );
    a?;
    b?;
    assert_eq!(
        storage.get(key("race"), "").await?[0].timestamp,
        timestamp(21)
    );
    storage
        .put(
            key("large"),
            ZBytes::from(vec![42; 2 * 1024 * 1024]),
            Encoding::default(),
            timestamp(30),
        )
        .await?;
    let bad_entry = format!("{base}:entry:k{}", hex("bad"));
    let _: usize = connection.hset(&bad_entry, "payload", "incomplete").await?;
    let _: usize = connection
        .sadd(format!("{base}:active"), &bad_entry)
        .await?;
    assert!(storage
        .get_all_entries()
        .await
        .unwrap_err()
        .to_string()
        .contains("missing field timestamp"));
    let _: usize = connection.hset(&bad_entry, "timestamp", "invalid").await?;
    assert!(storage
        .get_all_entries()
        .await
        .unwrap_err()
        .to_string()
        .contains("invalid timestamp"));
    let _: usize = connection.del(&bad_entry).await?;
    let _: () = connection.set(&bad_entry, "wrong type").await?;
    assert!(storage.get_all_entries().await.is_err());
    let _: usize = connection.del(&bad_entry).await?;
    let _: usize = connection
        .srem(format!("{base}:active"), &bad_entry)
        .await?;
    for i in 0..320 {
        storage
            .put(
                key(&format!("many/{i}")),
                ZBytes::from("batch"),
                Encoding::default(),
                timestamp(40),
            )
            .await?;
    }
    assert_eq!(storage.get_all_entries().await?.len(), 323);
    drop(storage);
    drop(second);
    let entries: Vec<String> = connection.smembers(format!("{base}:active")).await?;
    for entry in entries {
        let _: usize = connection.del(entry).await?;
    }
    let _: usize = connection.del(format!("{base}:active")).await?;
    Ok(())
}
