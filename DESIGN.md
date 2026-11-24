# Redis backend design

The backend targets Zenoh 1.10.2 and standalone Redis. A volume supplies a
Redis connection configuration; a storage owns an encoded namespace.
Redis Cluster and Zenoh storage-manager replication are explicitly rejected.

For simple clusters, the recommended starting topology is one fenced active
storage-manager writer and a standby using the same standalone Redis database
and logical storage namespace. Leader election and fencing are external
responsibilities. All instances sharing that namespace must use the same
`key_prefix`, `namespace`, `key_expr`, `strip_prefix`, and retention settings;
otherwise stripped-key mappings or expiry behavior can conflict. Distinct
logical storages must use distinct prefix/namespace pairs.

Redis is shared state, not independent Zenoh storage replication: its
availability and persistence are common dependencies, it does not replay
missed writes or publish Zenoh samples on behalf of other routers, and finite
tombstone retention permits eventual resurrection of older updates. Multiple
simultaneous storage managers sharing a namespace are experimental and not
validated as a cluster topology. Their in-memory wildcard and latest-update
state is not shared, even though individual Redis mutations are atomic.
Wildcard behavior, partitions, restarts, and concurrent writers require
deployment-specific testing.

Each storage has two sets, of active record keys and retained tombstone keys.
Each record is a hash containing a kind, full timestamp, and, for live records,
payload and encoding metadata. Names are versioned and hex-encode namespace
components and stripped key-expression bytes; the exact stripped prefix uses a
distinct representation. This separates keys, indexes and storages without
delimiter collisions.

Put and delete use a Redis script to compare the complete timestamp and change
the record, TTL and both indexes atomically with respect to other clients.
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

Deletion replaces the value with a finite-TTL tombstone. During its lifetime,
older updates cannot resurrect it. After expiration that guarantee is lost.
Live values can also have a TTL. Redis does not expire set members, so reads
and a periodic background scan remove stale members after an atomic check.
SSCAN is not a snapshot; concurrent updates may appear or disappear from an
enumeration. The storage-manager uses `get_all_entries` to discover wildcard
query and update targets. It scans the active index, fetches only record kind
and timestamp metadata, and returns only live records, including when a stale
active-index member now refers to a tombstone.
Wildcard puts and deletes therefore do not target retained tombstones in a
quiescent storage; a newer concrete-key put can still replace a tombstone.
Discovery and subsequent updates are not atomic, so a concurrent delete after
discovery can still race with a wildcard put.

Zenoh 1.10.2 reconstructs every stored entry as a put when it initializes its
replication state; its backend trait cannot report a tombstone's kind. Excluding
tombstones avoids reconstructing them as puts, but also means that the storage
manager cannot recover or propagate their delete events after restart. The
backend still rejects replication. Redis retains tombstones independently of
discovery and compares concrete-key updates against them until they expire.
The storage manager's in-memory wildcard history is not persisted by the
backend, so delayed wildcard operations across restarts are not guaranteed to
preserve their original ordering.

Redis persistence, eviction, network security and backups are administrator
responsibilities. Use no-eviction and a persistence policy appropriate to the
required durability. Restrict writes to backend-owned keys; wrong-type records
or indexes and malformed record metadata are surfaced as errors.
