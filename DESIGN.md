# Redis backend design

The backend targets Zenoh 1.10.2 and standalone Redis. A volume supplies a
Redis connection configuration; a storage owns an encoded namespace.
Redis Cluster and Zenoh storage-manager replication are explicitly rejected.

For simple clusters, the recommended starting topology is one fenced active
storage-manager writer and a standby using the same standalone Redis database
and logical storage namespace. Leader election and fencing are external
responsibilities. All instances sharing that namespace must use the same
`key_prefix`, `namespace`, `key_expr`, and `strip_prefix`;
otherwise stripped-key mappings can conflict. Distinct
logical storages must use distinct prefix/namespace pairs.

Redis is shared state, not independent Zenoh storage replication: its
availability and persistence are common dependencies, it does not replay
missed writes or publish Zenoh samples on behalf of other routers. Deletes have
no tombstone, so delayed older writes can immediately resurrect values. Multiple
simultaneous storage managers sharing a namespace are experimental and not
validated as a cluster topology. Their in-memory wildcard and latest-update
state is not shared, even though individual Redis mutations are atomic.
Wildcard behavior, partitions, restarts, and concurrent writers require
deployment-specific testing.

Each storage has one set of active record keys.
Each record is a hash containing the full timestamp, payload and encoding
metadata. Names are versioned and hex-encode namespace
components and stripped key-expression bytes; the exact stripped prefix uses a
distinct representation. This separates keys, indexes and storages without
delimiter collisions.

Put and delete use a Redis script to compare the complete timestamp and change
the record and active index atomically with respect to other clients.
Timestamp ordering uses big-endian time followed by the ID's native byte order,
matching Zenoh's timestamp ordering. Older and equal timestamps are rejected.
Scripts validate key types before writes. Redis scripts are not rollback
transactions; unexpected server-side failures after a write are surfaced and
require operator investigation. Startup and periodic reconciliation scan
the storage's record-key namespace and repair missing or incorrect index
members atomically. Startup waits for a complete pass and warns after 10
seconds. Periodic work is limited to 256 scan calls and per-record script
invocations combined per cycle, with cursors and unprocessed scan results
retained in memory until the next cycle. Each cycle waits the configured
interval after the preceding cycle completes; errors are logged and failed
members are retried on the next complete pass. Redis scan COUNT is a hint,
not a hard response-size or server-time bound. This can recover discoverability of a complete record,
but cannot recover data lost by Redis or repair a corrupt partial record.
Redis SCAN traverses the database keyspace even with MATCH, so reconciliation
cost grows with the shared database; the interval should be sized accordingly.

There is no backend-managed expiration or tombstone. Deletion removes the
record and its index member; after that, no stored timestamp remains to reject
an older put. Atomic timestamp comparison protects only currently live records.
Reads and periodic reconciliation remove stale members after an atomic check,
and repair missing index members after partial failures or external changes.
SSCAN is not a snapshot; concurrent updates may appear or disappear from an
enumeration. The storage-manager uses `get_all_entries` to discover wildcard
query and update targets. It scans the active index, fetches only timestamps,
and returns live records. Wildcard puts and deletes do not target deleted keys
in a quiescent storage; a concrete-key put can recreate a deleted key.
Discovery and subsequent updates are not atomic, so a concurrent delete after
discovery can still race with a wildcard put.

Zenoh 1.10.2 reconstructs every stored entry as a put when it initializes its
replication state. Because deletes have no durable representation, the storage
manager cannot recover or propagate delete events after restart. The backend
still rejects replication.
The storage manager's in-memory wildcard history is not persisted by the
backend, so delayed wildcard operations across restarts are not guaranteed to
preserve their original ordering.

Redis persistence, eviction, network security and backups are administrator
responsibilities. Use no-eviction and a persistence policy appropriate to the
required durability. Restrict writes to backend-owned keys; wrong-type records
or indexes and malformed record metadata are surfaced as errors.
