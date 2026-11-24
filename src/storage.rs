use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use redis::{aio::ConnectionManager, AsyncCommands, Script};
use zenoh::{
    bytes::{Encoding, ZBytes},
    key_expr::OwnedKeyExpr,
    time::Timestamp,
    Result,
};
use zenoh_backend_traits::{config::StorageConfig, Storage, StorageInsertionResult, StoredData};
use zenoh_util::ffi::JsonValue;

use crate::{
    codec,
    key_mapper::{hex, KeyMapper},
    runtime,
};

const SCAN_COUNT: usize = 256;
const PIPELINE_SIZE: usize = 128;
type Record = HashMap<String, Vec<u8>>;

#[derive(Clone)]
struct State {
    connection: ConnectionManager,
    mapper: KeyMapper,
}

pub struct RedisStorage {
    state: State,
    status: serde_json::Value,
}

impl RedisStorage {
    pub fn new(connection: ConnectionManager, mapper: KeyMapper, config: &StorageConfig) -> Self {
        Self {
            state: State { connection, mapper },
            status: config.to_json_value(),
        }
    }
}

impl State {
    async fn update(
        &mut self,
        key: Option<OwnedKeyExpr>,
        timestamp: Timestamp,
        value: Option<(ZBytes, Encoding)>,
    ) -> Result<StorageInsertionResult> {
        let bytes = codec::timestamp_bytes(&timestamp);
        let (kind, payload, id, has_schema, schema) = match value {
            Some((payload, encoding)) => {
                let (id, has_schema, schema) = codec::encoding_parts(&encoding);
                (
                    "put",
                    payload.to_bytes().into_owned(),
                    id,
                    has_schema,
                    schema,
                )
            }
            None => ("delete", Vec::new(), 0, false, Vec::new()),
        };
        let entry = self.mapper.entry(key.as_ref());
        let result: i32 = Script::new(include_str!("update.lua"))
            .key(entry)
            .arg(hex(&bytes))
            .arg(kind)
            .arg(bytes)
            .arg(payload)
            .arg(id)
            .arg(schema)
            .arg(if has_schema { 1 } else { 0 })
            .invoke_async(&mut self.connection)
            .await?;
        match result {
            0 => Ok(StorageInsertionResult::Outdated),
            1 => Ok(StorageInsertionResult::Inserted),
            2 => Ok(StorageInsertionResult::Replaced),
            3 => Ok(StorageInsertionResult::Deleted),
            _ => Err("unexpected Redis update result".into()),
        }
    }
}

fn field<'a>(record: &'a Record, name: &str) -> Result<&'a [u8]> {
    record
        .get(name)
        .map(Vec::as_slice)
        .ok_or_else(|| format!("Redis record missing field {name}").into())
}

fn record_timestamp(record: &Record) -> Result<Timestamp> {
    codec::timestamp_from_bytes(field(record, "timestamp")?)
}

fn metadata_command(key: &str) -> redis::Cmd {
    let mut command = redis::cmd("HGET");
    command.arg(key).arg("timestamp");
    command
}

async fn scan_entries(
    connection: &mut ConnectionManager,
    pattern: &str,
) -> Result<HashSet<String>> {
    let mut keys = HashSet::new();
    let mut cursor = 0u64;
    loop {
        let (next, members): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(SCAN_COUNT)
            .query_async(connection)
            .await?;
        keys.extend(members);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    Ok(keys)
}

async fn entries_from_keys(
    connection: &mut ConnectionManager,
    mapper: &KeyMapper,
    keys: HashSet<String>,
) -> Result<Vec<(Option<OwnedKeyExpr>, Timestamp)>> {
    let keys: Vec<String> = keys.into_iter().collect();
    let mut entries = Vec::with_capacity(keys.len());
    for batch in keys.chunks(PIPELINE_SIZE) {
        let mut pipeline = redis::pipe();
        for key in batch {
            mapper.decode(key)?;
            pipeline.add_command(metadata_command(key));
        }
        let records: Vec<Option<Vec<u8>>> = pipeline.query_async(connection).await?;
        for (key, mut timestamp) in batch.iter().zip(records) {
            if timestamp.is_none() {
                // Distinguish a deleted key from a corrupt hash in one atomic recheck.
                let (exists, current_timestamp): (bool, Option<Vec<u8>>) =
                    Script::new(include_str!("metadata.lua"))
                        .key(key)
                        .invoke_async(connection)
                        .await?;
                if !exists {
                    continue;
                }
                timestamp = current_timestamp;
            }
            let timestamp = timestamp.ok_or("Redis record missing field timestamp")?;
            entries.push((
                mapper.decode(key)?,
                codec::timestamp_from_bytes(&timestamp)?,
            ));
        }
    }
    Ok(entries)
}

#[async_trait]
impl Storage for RedisStorage {
    fn get_admin_status(&self) -> JsonValue {
        (&self.status).into()
    }

    async fn put(
        &mut self,
        key: Option<OwnedKeyExpr>,
        payload: ZBytes,
        encoding: Encoding,
        timestamp: Timestamp,
    ) -> Result<StorageInsertionResult> {
        let mut state = self.state.clone();
        runtime::run(async move {
            state
                .update(key, timestamp, Some((payload, encoding)))
                .await
        })
        .await
    }

    async fn delete(
        &mut self,
        key: Option<OwnedKeyExpr>,
        timestamp: Timestamp,
    ) -> Result<StorageInsertionResult> {
        let mut state = self.state.clone();
        runtime::run(async move { state.update(key, timestamp, None).await }).await
    }

    async fn get(
        &mut self,
        key: Option<OwnedKeyExpr>,
        parameters: &str,
    ) -> Result<Vec<StoredData>> {
        if !parameters.is_empty() {
            return Err("Redis backend does not support query parameters".into());
        }
        let mut state = self.state.clone();
        runtime::run(async move {
            let record: Record = state
                .connection
                .hgetall(state.mapper.entry(key.as_ref()))
                .await?;
            if record.is_empty() {
                return Ok(Vec::new());
            }
            let id = std::str::from_utf8(field(&record, "encoding_id")?)?.parse::<u16>()?;
            let has_schema = match field(&record, "has_schema")? {
                b"0" => false,
                b"1" => true,
                _ => return Err("invalid Redis encoding schema flag".into()),
            };
            Ok(vec![StoredData {
                timestamp: record_timestamp(&record)?,
                payload: ZBytes::from(field(&record, "payload")?.to_vec()),
                encoding: codec::encoding_from_parts(
                    id,
                    has_schema,
                    field(&record, "encoding_schema")?.to_vec(),
                ),
            }])
        })
        .await
    }

    async fn get_all_entries(&self) -> Result<Vec<(Option<OwnedKeyExpr>, Timestamp)>> {
        let state = self.state.clone();
        runtime::run(async move {
            let mut connection = state.connection.clone();
            let keys = scan_entries(&mut connection, &state.mapper.entries_pattern()).await?;
            entries_from_keys(&mut connection, &state.mapper, keys).await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn discovery_requests_only_required_metadata() {
        assert_eq!(
            metadata_command("example").get_packed_command(),
            b"*3\r\n$4\r\nHGET\r\n$7\r\nexample\r\n$9\r\ntimestamp\r\n"
        );
    }

    #[tokio::test]
    async fn discovery_handles_deletion_between_scan_and_fetch() -> Result<()> {
        let url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
        let mut connection = redis::Client::open(url)?.get_connection_manager().await?;
        let token = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let mapper = KeyMapper::new("scan-test", &format!("{}-{token}", std::process::id()));
        let key = mapper.entry(None);
        let _: usize = connection.hset(&key, "payload", "").await?;
        let keys = scan_entries(&mut connection, &mapper.entries_pattern()).await?;
        assert_eq!(keys, HashSet::from([key.clone()]));
        let _: usize = connection.del(&key).await?;
        assert!(entries_from_keys(&mut connection, &mapper, keys)
            .await?
            .is_empty());
        Ok(())
    }
}
