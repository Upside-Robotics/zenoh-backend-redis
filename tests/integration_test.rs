use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
async fn discovery_reads_metadata_and_detects_invalid_records(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
    let namespace = format!(
        "metadata-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let json = serde_json::json!({
        "volumes": {"redis": {"url": url, "key_prefix": "integration"}},
        "storages": {"test": {
            "key_expr": "test/**", "strip_prefix": "test",
            "volume": {"id": "redis", "namespace": namespace, "tombstone_ttl_seconds": 60}
        }}
    });
    let config = PluginConfig::try_from(("storage_manager", &json))?;
    let volume = RedisBackend::start("redis", &config.volumes[0])?;
    let mut storage = volume.create_storage(config.storages[0].clone()).await?;
    let mut connection = redis::Client::open(url.as_str())?
        .get_multiplexed_async_connection()
        .await?;
    let base = format!("zenoh-redis:v1:{}:{}", hex("integration"), hex(&namespace));
    let active = format!("{base}:active");
    let live_entry = format!("{base}:entry:k{}", hex("large"));
    let missing_entry = format!("{base}:entry:k{}", hex("missing"));
    let bad_entry = format!("{base}:entry:k{}", hex("bad"));

    storage
        .put(
            key("large"),
            ZBytes::from(vec![42; 2 * 1024 * 1024]),
            Encoding::default(),
            timestamp(10),
        )
        .await?;
    let _: usize = connection.sadd(&active, &missing_entry).await?;
    assert_eq!(
        storage.get_all_entries().await?,
        vec![(key("large"), timestamp(10))]
    );
    let indexed: bool = connection.sismember(&active, &missing_entry).await?;
    assert!(!indexed, "discovery must clean missing active records");

    let _: usize = connection.hset(&bad_entry, "payload", "incomplete").await?;
    let _: usize = connection.sadd(&active, &bad_entry).await?;
    let error = storage.get_all_entries().await.unwrap_err();
    assert!(error.to_string().contains("missing field kind"));
    let indexed: bool = connection.sismember(&active, &bad_entry).await?;
    assert!(indexed, "corrupt records must not be treated as missing");

    let _: usize = connection.hset(&bad_entry, "kind", "live").await?;
    let error = storage.get_all_entries().await.unwrap_err();
    assert!(error.to_string().contains("missing field timestamp"));
    let _: usize = connection.hset(&bad_entry, "timestamp", "invalid").await?;
    let error = storage.get_all_entries().await.unwrap_err();
    assert!(error.to_string().contains("invalid timestamp"));
    let _: usize = connection.hset(&bad_entry, "kind", "unknown").await?;
    let error = storage.get_all_entries().await.unwrap_err();
    assert!(error.to_string().contains("invalid Redis record kind"));
    let _: usize = connection.del(&bad_entry).await?;
    let _: () = connection.set(&bad_entry, "wrong type").await?;
    assert!(storage.get_all_entries().await.is_err());

    drop(storage);
    let _: usize = connection.del(&[live_entry, bad_entry, active]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn redis_storage_ordering_tombstones_ttl_and_recovery(
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
            "volume": {"id": "redis", "namespace": namespace, "tombstone_ttl_seconds": 2, "cleanup_interval_seconds": 1}
        }}
    });
    let config = PluginConfig::try_from(("storage_manager", &json))?;
    let volume = RedisBackend::start("redis", &config.volumes[0])?;
    let mut replicated = config.storages[0].clone();
    replicated.replication = Some(Default::default());
    assert!(volume.create_storage(replicated).await.is_err());
    let mut storage = volume.create_storage(config.storages[0].clone()).await?;
    let mut second = volume.create_storage(config.storages[0].clone()).await?;
    let mut connection = redis::Client::open(url.as_str())?
        .get_multiplexed_async_connection()
        .await?;
    let base = format!("zenoh-redis:v1:{}:{}", hex("integration"), hex(&namespace));
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
    assert_eq!(
        storage.get(None, "").await?[0].payload.to_bytes().as_ref(),
        &[0, 255]
    );
    assert_eq!(
        storage.get(None, "").await?[0].encoding,
        Encoding::TEXT_PLAIN.with_schema("utf-8")
    );
    assert!(storage.get(None, "_time=now").await.is_err());
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
    let ttl: i64 = connection.ttl(&none_entry).await?;
    assert!(ttl > 0);
    let _: usize = connection
        .sadd(format!("{base}:active"), &none_entry)
        .await?;
    assert!(storage.get_all_entries().await?.is_empty());
    let indexed: bool = connection
        .sismember(format!("{base}:active"), &none_entry)
        .await?;
    assert!(
        !indexed,
        "discovery must repair stale live index membership"
    );
    let indexed: bool = connection
        .sismember(format!("{base}:tombstones"), &none_entry)
        .await?;
    assert!(indexed, "discovery must retain the tombstone index");
    drop(storage);
    let mut storage = volume.create_storage(config.storages[0].clone()).await?;
    assert!(storage.get_all_entries().await?.is_empty());
    assert!(matches!(
        storage
            .put(
                None,
                ZBytes::from("old"),
                Encoding::default(),
                timestamp(10)
            )
            .await?,
        StorageInsertionResult::Outdated
    ));
    assert!(matches!(
        storage
            .put(
                None,
                ZBytes::from("new"),
                Encoding::default(),
                timestamp(12)
            )
            .await?,
        StorageInsertionResult::Inserted
    ));
    let ttl: i64 = connection.ttl(&none_entry).await?;
    assert_eq!(ttl, -1, "a live put must clear the old tombstone TTL");

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
    storage.delete(key("race"), timestamp(22)).await?;
    let entries = storage.get_all_entries().await?;
    assert_eq!(entries, vec![(None, timestamp(12))]);
    tokio::time::sleep(Duration::from_millis(3200)).await;
    let race_entry = format!("{base}:entry:k{}", hex("race"));
    let indexed: bool = connection
        .sismember(format!("{base}:tombstones"), &race_entry)
        .await?;
    assert!(
        !indexed,
        "periodic cleanup must remove expired tombstones without a query"
    );
    assert!(matches!(
        storage
            .put(
                key("race"),
                ZBytes::from("late"),
                Encoding::default(),
                timestamp(20)
            )
            .await?,
        StorageInsertionResult::Inserted
    ));

    let bad_entry = format!("{base}:entry:k{}", hex("bad"));
    let _: () = connection.set(&bad_entry, "wrong type").await?;
    assert!(storage
        .put(
            key("bad"),
            ZBytes::from("value"),
            Encoding::default(),
            timestamp(30)
        )
        .await
        .is_err());
    let bad: String = connection.get(&bad_entry).await?;
    assert_eq!(bad, "wrong type");
    let _: usize = connection.del(&bad_entry).await?;

    let mut expiring_config = json.clone();
    expiring_config["storages"]["test"]["volume"]["ttl_seconds"] = 1.into();
    let expiring_config = PluginConfig::try_from(("storage_manager", &expiring_config))?;
    let mut expiring = volume
        .create_storage(expiring_config.storages[0].clone())
        .await?;
    expiring
        .put(
            key("expires"),
            ZBytes::from("short"),
            Encoding::default(),
            timestamp(30),
        )
        .await?;
    let expiring_entry = format!("{base}:entry:k{}", hex("expires"));
    let ttl: i64 = connection.ttl(&expiring_entry).await?;
    assert!(ttl >= 0);
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert!(expiring.get(key("expires"), "").await?.is_empty());
    let indexed: bool = connection
        .sismember(format!("{base}:active"), &expiring_entry)
        .await?;
    assert!(!indexed, "periodic cleanup must remove expired live values");
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
    assert_eq!(
        storage.get_all_entries().await?.len(),
        322,
        "SSCAN and pipelined reads must enumerate the complete index"
    );
    let entries: Vec<String> = redis::cmd("SMEMBERS")
        .arg(format!("{base}:active"))
        .query_async(&mut connection)
        .await?;
    let tombstones: Vec<String> = redis::cmd("SMEMBERS")
        .arg(format!("{base}:tombstones"))
        .query_async(&mut connection)
        .await?;
    drop(storage);
    drop(second);
    drop(expiring);
    for entry in entries.into_iter().chain(tombstones) {
        let _: usize = connection.del(entry).await?;
    }
    let _: usize = connection
        .del(&[format!("{base}:active"), format!("{base}:tombstones")])
        .await?;
    Ok(())
}
