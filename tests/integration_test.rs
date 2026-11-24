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
async fn escaped_names_round_trip_and_discovery(
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
        let exists: bool = connection.exists(&entry).await?;
        assert!(exists, "missing expected Redis entry {entry}");
    }

    let other = format!("{base}other:entry:kforeign");
    let _: () = connection.set(&other, "not a record").await?;
    let active = format!("{base}:active");
    let active_exists: bool = connection.exists(&active).await?;
    assert!(!active_exists, "updates must not create an index");
    let entries = storage.get_all_entries().await?;
    assert_eq!(entries.len(), cases.len());
    for (key_text, _) in cases {
        let expected = key_text.map(|value| OwnedKeyExpr::try_from(value.to_owned()).unwrap());
        assert!(entries.contains(&(expected, timestamp(10))));
    }
    drop(storage);
    let reopened = volume.create_storage(config.storages[0].clone()).await?;
    assert_eq!(reopened.get_all_entries().await?.len(), cases.len());
    drop(reopened);
    for (_, suffix) in cases {
        let _: usize = connection.del(format!("{base}:entry:{suffix}")).await?;
    }
    let _: usize = connection.del(other).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn discovery_scans_ten_thousand_records(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
    let namespace = format!(
        "scale-{}-{}",
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
    for i in 0..10_000 {
        storage
            .put(
                key(&format!("record/{i}")),
                ZBytes::from("value"),
                Encoding::default(),
                timestamp(10),
            )
            .await?;
    }
    let entries = storage.get_all_entries().await?;
    assert_eq!(entries.len(), 10_000);
    let entries: std::collections::HashSet<_> = entries.into_iter().collect();
    assert_eq!(entries.len(), 10_000);
    for i in 0..10_000 {
        assert!(entries.contains(&(key(&format!("record/{i}")), timestamp(10))));
    }

    drop(storage);
    let mut connection = redis::Client::open(url.as_str())?
        .get_multiplexed_async_connection()
        .await?;
    let base = format!("zenoh-redis:v1:integration:{namespace}");
    for start in (0..10_000).step_by(128) {
        let mut pipeline = redis::pipe();
        for i in start..(start + 128).min(10_000) {
            pipeline.del(format!("{base}:entry:krecord/{i}")).ignore();
        }
        let _: () = pipeline.query_async(&mut connection).await?;
    }
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
            "volume": {"id": "redis", "namespace": namespace}
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
    assert_eq!(
        storage.get_all_entries().await?,
        vec![(None, timestamp(10))]
    );
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
    for suffix in ["n", "krace", "klarge"] {
        let _: usize = connection.del(format!("{base}:entry:{suffix}")).await?;
    }
    for i in 0..320 {
        let _: usize = connection.del(format!("{base}:entry:kmany/{i}")).await?;
    }
    Ok(())
}
