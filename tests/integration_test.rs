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

#[tokio::test(flavor = "multi_thread")]
async fn escaped_names_round_trip_and_reconcile(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
    let token = format!(
        "{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let namespace = format!("namespace:*?[]\\%-{token}");
    let json = serde_json::json!({
        "volumes": {"redis": {"url": url, "key_prefix": "prefix:*?[]\\%"}},
        "storages": {"test": {
            "key_expr": "test/**", "strip_prefix": "test",
            "volume": {"id": "redis", "namespace": namespace}
        }}
    });
    let config = PluginConfig::try_from(("storage_manager", &json))?;
    let volume = RedisBackend::start("redis", &config.volumes[0])?;
    let mut storage = volume.create_storage(config.storages[0].clone()).await?;
    let mut connection = redis::Client::open(url.as_str())?
        .get_multiplexed_async_connection()
        .await?;
    let base = format!(
        "zenoh-redis:v1:prefix%3A%2A%3F%5B%5D%5C%25:namespace%3A%2A%3F%5B%5D%5C%25-{token}"
    );
    let index = format!("{base}:active");
    let cases = [
        (None, "n"),
        (Some("a:b"), "ka%3Ab"),
        (Some("a%3Ab"), "ka%253Ab"),
        (Some("café/x"), "kcaf%C3%A9/x"),
    ];
    for (key_text, suffix) in cases {
        let owned_key = key_text.map(|value| OwnedKeyExpr::try_from(value.to_owned()).unwrap());
        storage
            .put(
                owned_key.clone(),
                ZBytes::from(suffix),
                Encoding::default(),
                timestamp(10),
            )
            .await?;
        assert_eq!(
            storage.get(owned_key, "").await?[0]
                .payload
                .to_bytes()
                .as_ref(),
            suffix.as_bytes()
        );
        let entry = format!("{base}:entry:{suffix}");
        let indexed: bool = connection.sismember(&index, &entry).await?;
        assert!(indexed, "missing expected Redis entry {entry}");
        let _: usize = connection.srem(&index, entry).await?;
    }

    drop(storage);
    let repaired = volume.create_storage(config.storages[0].clone()).await?;
    let entries = repaired.get_all_entries().await?;
    assert_eq!(entries.len(), cases.len());
    for (key_text, suffix) in cases {
        let expected = key_text.map(|value| OwnedKeyExpr::try_from(value.to_owned()).unwrap());
        assert!(entries.contains(&(expected, timestamp(10))));
        let entry = format!("{base}:entry:{suffix}");
        let indexed: bool = connection.sismember(&index, &entry).await?;
        assert!(indexed, "reconciliation did not restore {entry}");
    }
    drop(repaired);
    for (_, suffix) in cases {
        let _: usize = connection.del(format!("{base}:entry:{suffix}")).await?;
    }
    let _: usize = connection.del(index).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn reconciliation_rejects_incomplete_and_invalid_records(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
    let namespace = format!(
        "repair-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let json = serde_json::json!({
        "volumes": {"redis": {"url": url, "key_prefix": "integration"}},
        "storages": {"test": {
            "key_expr": "test/**", "strip_prefix": "test",
            "volume": {"id": "redis", "namespace": namespace}
        }}
    });
    let config = PluginConfig::try_from(("storage_manager", &json))?;
    let volume = RedisBackend::start("redis", &config.volumes[0])?;
    let mut storage = volume.create_storage(config.storages[0].clone()).await?;
    let mut connection = redis::Client::open(url.as_str())?
        .get_multiplexed_async_connection()
        .await?;
    let base = format!("zenoh-redis:v1:integration:{namespace}");
    let entry = format!("{base}:entry:n");
    let index = format!("{base}:active");
    storage
        .put(None, ZBytes::from(""), Encoding::default(), timestamp(10))
        .await?;
    drop(storage);
    let _: usize = connection.srem(&index, &entry).await?;

    for field in [
        "payload",
        "encoding_id",
        "encoding_schema",
        "has_schema",
        "order",
        "timestamp",
    ] {
        let value: Vec<u8> = connection.hget(&entry, field).await?;
        let _: usize = connection.hdel(&entry, field).await?;
        let error = match volume.create_storage(config.storages[0].clone()).await {
            Ok(_) => panic!("missing {field} was accepted"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("Redis backend record"),
            "unexpected error for missing {field}: {error}"
        );
        let indexed: bool = connection.sismember(&index, &entry).await?;
        assert!(!indexed, "record missing {field} was indexed");
        let _: usize = connection.hset(&entry, field, value).await?;
    }

    for (field, invalid) in [
        ("order", "x".repeat(48)),
        ("encoding_id", "65536".to_owned()),
        ("encoding_id", "bad".to_owned()),
        ("has_schema", "2".to_owned()),
    ] {
        let value: Vec<u8> = connection.hget(&entry, field).await?;
        let _: usize = connection.hset(&entry, field, invalid).await?;
        assert!(volume
            .create_storage(config.storages[0].clone())
            .await
            .is_err());
        let indexed: bool = connection.sismember(&index, &entry).await?;
        assert!(!indexed, "record with invalid {field} was indexed");
        let _: usize = connection.hset(&entry, field, value).await?;
    }

    let valid_timestamp: Vec<u8> = connection.hget(&entry, "timestamp").await?;
    let valid_order: Vec<u8> = connection.hget(&entry, "order").await?;
    let _: usize = connection.hset(&entry, "timestamp", vec![0u8; 24]).await?;
    let _: usize = connection.hset(&entry, "order", "0".repeat(48)).await?;
    assert!(volume
        .create_storage(config.storages[0].clone())
        .await
        .is_err());
    let _: usize = connection
        .hset(&entry, "timestamp", valid_timestamp)
        .await?;
    let _: usize = connection.hset(&entry, "order", valid_order).await?;
    let mut repaired = volume.create_storage(config.storages[0].clone()).await?;
    assert_eq!(
        repaired.get_all_entries().await?,
        vec![(None, timestamp(10))]
    );
    assert!(repaired.get(None, "").await?[0]
        .payload
        .to_bytes()
        .is_empty());
    drop(repaired);
    let _: usize = connection.del(&[entry, index]).await?;
    Ok(())
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
    let base = format!("zenoh-redis:v1:integration:{namespace}");

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
    let bad_entry = format!("{base}:entry:kbad");
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
