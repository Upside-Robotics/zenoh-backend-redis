use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    time::Duration,
};

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
const RECONCILE_BUDGET: usize = 256;
const STARTUP_WARNING_AFTER: Duration = Duration::from_secs(10);
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
            .key(self.mapper.active())
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

async fn scan_index(connection: &mut ConnectionManager, index: &str) -> Result<HashSet<String>> {
    let mut keys = HashSet::new();
    let mut cursor = 0u64;
    loop {
        let (next, members): (u64, Vec<String>) = redis::cmd("SSCAN")
            .arg(index)
            .arg(cursor)
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

async fn clean_member(connection: &mut ConnectionManager, index: &str, key: &str) -> Result<()> {
    let _: i64 = Script::new(include_str!("cleanup.lua"))
        .key(key)
        .key(index)
        .invoke_async(connection)
        .await?;
    Ok(())
}

#[derive(Default)]
enum ReconcilePhase {
    #[default]
    Active,
    Records,
}

#[derive(Default)]
struct Reconciliation {
    phase: ReconcilePhase,
    cursor: u64,
    page_loaded: bool,
    pending: VecDeque<String>,
}

impl Reconciliation {
    async fn run_cycle(
        &mut self,
        connection: &mut ConnectionManager,
        mapper: &KeyMapper,
        budget: usize,
    ) -> Result<(usize, bool)> {
        let mut work = 0;
        while work < budget {
            if let Some(key) = self.pending.pop_front() {
                work += 1;
                mapper.decode(&key)?;
                match self.phase {
                    ReconcilePhase::Active => {
                        clean_member(connection, &mapper.active(), &key).await?
                    }
                    ReconcilePhase::Records => {
                        let _: i64 = Script::new(include_str!("repair.lua"))
                            .key(&key)
                            .key(mapper.active())
                            .invoke_async(connection)
                            .await?;
                    }
                }
                continue;
            }

            if self.page_loaded && self.cursor == 0 {
                self.page_loaded = false;
                self.phase = match self.phase {
                    ReconcilePhase::Active => ReconcilePhase::Records,
                    ReconcilePhase::Records => {
                        self.phase = ReconcilePhase::Active;
                        return Ok((work, true));
                    }
                };
            }
            let mut command = match self.phase {
                ReconcilePhase::Active => {
                    let mut command = redis::cmd("SSCAN");
                    command.arg(mapper.active());
                    command.arg(self.cursor);
                    command
                }
                ReconcilePhase::Records => {
                    let mut command = redis::cmd("SCAN");
                    command
                        .arg(self.cursor)
                        .arg("MATCH")
                        .arg(mapper.entries_pattern());
                    command
                }
            };
            let (next, members): (u64, Vec<String>) = command
                .arg("COUNT")
                .arg(SCAN_COUNT)
                .query_async(connection)
                .await?;
            work += 1;
            self.cursor = next;
            self.page_loaded = true;
            self.pending.extend(members);
        }
        Ok((work, false))
    }
}

async fn reconcile(connection: &mut ConnectionManager, mapper: &KeyMapper) -> Result<()> {
    let mut progress = Reconciliation::default();
    loop {
        if progress
            .run_cycle(connection, mapper, RECONCILE_BUDGET)
            .await?
            .1
        {
            return Ok(());
        }
    }
}

async fn warn_if_slow<F, T>(future: F, threshold: Duration, warn: impl FnOnce()) -> T
where
    F: Future<Output = T>,
{
    let mut future = Box::pin(future);
    match tokio::time::timeout(threshold, &mut future).await {
        Ok(result) => result,
        Err(_) => {
            warn();
            future.await
        }
    }
}

pub async fn initialize_indexes(
    mut connection: ConnectionManager,
    mapper: KeyMapper,
) -> Result<()> {
    runtime::run(async move {
        warn_if_slow(
            reconcile(&mut connection, &mapper),
            STARTUP_WARNING_AFTER,
            || {
                tracing::warn!(
                    storage = %mapper.active(),
                    seconds = STARTUP_WARNING_AFTER.as_secs(),
                    "Redis startup index reconciliation is still running"
                );
            },
        )
        .await
    })
    .await
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
            let keys = scan_index(&mut connection, &state.mapper.active()).await?;
            let keys: Vec<String> = keys.into_iter().collect();
            let mut entries = Vec::with_capacity(keys.len());
            for batch in keys.chunks(PIPELINE_SIZE) {
                let mut pipeline = redis::pipe();
                for key in batch {
                    state.mapper.decode(key)?;
                    pipeline.add_command(metadata_command(key));
                }
                let records: Vec<Option<Vec<u8>>> = pipeline.query_async(&mut connection).await?;
                for (key, mut timestamp) in batch.iter().zip(records) {
                    if timestamp.is_none() {
                        // Distinguish a deleted key from a corrupt hash in one atomic recheck.
                        let (exists, current_timestamp): (bool, Option<Vec<u8>>) =
                            Script::new(include_str!("metadata.lua"))
                                .key(key)
                                .invoke_async(&mut connection)
                                .await?;
                        if !exists {
                            clean_member(&mut connection, &state.mapper.active(), key).await?;
                            continue;
                        }
                        timestamp = current_timestamp;
                    }
                    let timestamp = timestamp.ok_or("Redis record missing field timestamp")?;
                    entries.push((
                        state.mapper.decode(key)?,
                        codec::timestamp_from_bytes(&timestamp)?,
                    ));
                }
            }
            Ok(entries)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn discovery_requests_only_required_metadata() {
        assert_eq!(
            metadata_command("example").get_packed_command(),
            b"*3\r\n$4\r\nHGET\r\n$7\r\nexample\r\n$9\r\ntimestamp\r\n"
        );
    }

    #[tokio::test]
    async fn startup_warning_fires_only_when_threshold_is_exceeded() {
        let warnings = AtomicUsize::new(0);
        let result = warn_if_slow(async { 7 }, Duration::from_secs(1), || {
            warnings.fetch_add(1, Ordering::SeqCst);
        })
        .await;
        assert_eq!(result, 7);
        assert_eq!(warnings.load(Ordering::SeqCst), 0);

        let result = warn_if_slow(
            async {
                tokio::time::sleep(Duration::from_millis(30)).await;
                9
            },
            Duration::from_millis(5),
            || {
                warnings.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await;
        assert_eq!(result, 9);
        assert_eq!(warnings.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reconciliation_resumes_pages_with_a_bounded_budget() -> Result<()> {
        let url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
        let mut connection = redis::Client::open(url)?.get_connection_manager().await?;
        let token = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let mapper = KeyMapper::new("reconcile-test", &format!("{}-{token}", std::process::id()));
        for i in 0..40 {
            let key = OwnedKeyExpr::try_from(format!("stale/{i}"))?;
            let _: usize = connection
                .sadd(mapper.active(), mapper.entry(Some(&key)))
                .await?;
        }
        let orphan = mapper.entry(Some(&OwnedKeyExpr::try_from("orphan")?));
        let timestamp = codec::timestamp_bytes(&Timestamp::new(
            zenoh::time::NTP64(1),
            zenoh::time::TimestampId::try_from(1u8)?,
        ));
        let _: () = redis::cmd("HSET")
            .arg(&orphan)
            .arg("order")
            .arg(hex(&timestamp))
            .arg("timestamp")
            .arg(&timestamp)
            .arg("payload")
            .arg("")
            .arg("encoding_id")
            .arg("0")
            .arg("has_schema")
            .arg("0")
            .arg("encoding_schema")
            .arg("")
            .query_async(&mut connection)
            .await?;

        let mut progress = Reconciliation::default();
        let (work, complete) = progress.run_cycle(&mut connection, &mapper, 1).await?;
        assert_eq!(work, 1);
        assert!(!complete);
        assert!(!progress.pending.is_empty());
        let recreated = progress.pending.front().unwrap().clone();
        let _: () = redis::cmd("HSET")
            .arg(&recreated)
            .arg("order")
            .arg(hex(&timestamp))
            .arg("timestamp")
            .arg(&timestamp)
            .arg("payload")
            .arg("")
            .arg("encoding_id")
            .arg("0")
            .arg("has_schema")
            .arg("0")
            .arg("encoding_schema")
            .arg("")
            .query_async(&mut connection)
            .await?;

        let mut cycles = 1;
        loop {
            let (work, complete) = progress.run_cycle(&mut connection, &mapper, 5).await?;
            assert!(work <= 5);
            cycles += 1;
            assert!(cycles < 100, "reconciliation did not finish");
            if complete {
                break;
            }
        }
        assert!(cycles > 8, "all members must be charged to the budget");
        let members: HashSet<String> = connection.smembers(mapper.active()).await?;
        assert_eq!(members, HashSet::from([recreated.clone(), orphan.clone()]));
        let _: usize = connection
            .del(&[recreated, orphan, mapper.active()])
            .await?;
        Ok(())
    }
}
