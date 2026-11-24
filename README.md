# Zenoh Redis backend

A storage-manager backend for Eclipse Zenoh 1.10.2 and standalone Redis.
Redis Cluster and Zenoh storage-manager replication are not supported.

## Build and configure

Build using the exact Rust toolchain and Zenoh versions used to build the
target router and storage-manager plugin:

```sh
cargo build --release
```

Place the resulting `libzenoh_backend_redis` library on the storage-manager
backend search path, or configure `__path__` on the volume. Start `zenohd`
with [config/zenoh-redis.json5](config/zenoh-redis.json5) after adjusting the
address and storage namespace. The router must have
timestamping enabled. The configuration is JSON5.

Each logically distinct storage requires a unique `namespace` within its
volume's `key_prefix`. Router instances intentionally sharing one logical
storage must use the same namespace and matching settings, as described below.
`cleanup_interval_seconds` defaults to 60 seconds. Stored values do not expire
automatically, and deletes do not leave tombstones. An accepted delete removes
the record and its index entry; a delayed older put can recreate it immediately,
including after restart. Zenoh's in-memory ordering may reject some delayed
updates, but the backend does not retain delete history.
Wildcard discovery returns live keys only; wildcard puts do not select deleted
keys in a quiescent storage.
Discovery is not a snapshot, so concurrent deletes can race with wildcard puts.
The storage manager's wildcard history is not recovered after restart; delayed
wildcard operations across restarts are not guaranteed to preserve ordering.

The `private` volume object accepts an optional Redis ACL `username` and
`password`, or a password alone. URLs must not contain credentials. `rediss://`
uses TLS with system trust roots. Optional PEM CA and client certificate/key
files can be configured under `tls.private`; see the example. Config files containing passwords must be
protected: Zenoh's `private` redaction is not encryption. The Redis account
needs permission for INFO, hash, set, SCAN, SSCAN, DEL and script commands
used by the backend. A Redis connection and its cluster status are checked
when each storage is created.

For deployments requiring disk persistence, configure Redis persistence and
recovery appropriately and use `maxmemory-policy noeviction`. Redis may
still lose acknowledged data depending on persistence and failure settings.
The backend advertises volatile persistence because it cannot guarantee the
server's configuration. Restrict
other writers' access to the backend's keys. Startup reconciliation scans the
shared Redis database keyspace to completion before storage becomes available;
it logs a warning if the scan takes longer than 10 seconds. Periodic
reconciliation resumes across intervals, with at most 256 scan commands or
per-record script invocations per interval. Redis scan `COUNT` is a hint, so
an individual scan can return more members than requested; these are retained
in memory for later intervals. A full pass over a large database can take many
intervals, delaying stale-index cleanup or repair. The interval is a pause
after each cycle rather than a fixed-rate schedule.

## Simple clusters: shared Redis

For a simple Zenoh cluster that needs shared queryable storage, the recommended
starting topology is a **single active storage-manager writer** connected to a
shared standalone Redis service, rather than Zenoh storage-manager replication.
A standby router can use the same Redis data after failover. Use external leader
election and fencing to prevent multiple active writers; this backend does not
provide either mechanism. Keep storage-manager `replication` disabled.

Configure every router serving the same logical storage with the same Redis
server and database, `key_prefix`, `namespace`, `key_expr`, `strip_prefix`,
and compatible cleanup settings. Use a Redis address
reachable from every router, not `127.0.0.1` unless Redis is local to each.
Reserve a different prefix/namespace pair for each distinct logical storage.
The backend maps stripped keys into the shared namespace; mismatched
`strip_prefix` or storage expressions can cause keys to collide.

Shared Redis is not equivalent to independent storage replication or high
availability. All routers depend on Redis availability, persistence, recovery,
and network connectivity. The backend has no replay queue for writes missed
during an outage. A Redis write does not itself publish a Zenoh sample on other
routers; Zenoh still distributes live publications. Deletes leave no durable
ordering record, so delayed older writes can resurrect a deleted key.

Multiple simultaneous storage-manager writers to one namespace are
**experimental and not validated as a cluster topology**. Per-key Redis updates
are atomic, but each storage manager keeps its own wildcard and latest-update
state. Out-of-order delivery, missed events, restarts, and wildcard updates can
produce different decisions; wildcard scans are not snapshots. Prefer
concrete-key puts and deletes, and test failover, partitions, delayed updates,
wildcard behavior, and Redis outages against the actual deployment before
relying on concurrent writers. If stronger availability or consistency is
required, use a separately validated architecture rather than treating shared
Redis as a substitute for replicated Zenoh storage.

## Testing

```sh
docker compose -f tests/docker-compose.yml up -d
cargo test
```

Integration tests use `REDIS_URL` if set, otherwise `redis://127.0.0.1:6379`.
For the router end-to-end test, build the library with `cargo build --lib`,
set `ZENOH_E2E_ROUTER` to a matching `zenohd` binary and
`ZENOH_E2E_STORAGE_PLUGIN` to its storage-manager shared library, then run
`cargo test --test end_to_end -- --ignored`. `ZENOH_E2E_BACKEND` can override
the default debug backend library path.
An authenticated TLS Redis fixture can additionally be tested with
`REDIS_TEST_TLS_URL`, `REDIS_TEST_TLS_CA` (PEM CA file) and
`REDIS_TEST_TLS_PASSWORD` set, using `cargo test --test tls_test -- --ignored`.
For operational behavior and limitations see [DESIGN.md](DESIGN.md).
