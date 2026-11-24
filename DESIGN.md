# Redis backend design

The backend targets Zenoh 1.10.2 and a standalone Redis instance. 

Configuring a redis storage backend in zenoh's config.json5 loads this plugin, and tries 
to connect to the server, but the actual redis server must already exist and be running.

Not currently supported (both are rejected when a storage is created):

- Redis Cluster: the backend does not handle cluster redirects (`MOVED`/`ASK`), scanning a cluster (`SCAN` only covers one node), or `HGET` batching on a cluster.
- Zenoh storage-manager replication

For simple zenoh clusters, it may be adequate to have just one router provide the storage connection to redis. This plugin also *should* behave with multiple routers accessing the same redis server, so long as all instances sharing that namespace use the same `key_prefix`, `namespace`, `key_expr`, and `strip_prefix` (otherwise stripped-key mappings can conflict). 

Each distinct storage must use their own unique prefix/namespace pairs or very weird things will happen. 
This goes doubly so for prefixes+namespaces that overlap/nest.

Each storage stores zero or more Redis hashes containing the full timestamp,
payload and encoding metadata for each stored record.

## Zenoh uri -> Redis key mapping

Redis names start with `zenoh-redis:v1`, where `v1` identifies the naming
format. The storage's configured prefix and namespace are escaped separately
and joined with `:` to build a shared base name. Escaping leaves ASCII letters,
digits, `-`, `_`, `.`, `~`, and `/` unchanged. Every other UTF-8 byte is
represented by `%` followed by two uppercase hexadecimal digits. For example,
`a:b` becomes `a%3Ab`, and `a%3Ab` becomes `a%253Ab`. This also escapes Redis
glob characters in the prefix and namespace so discovery scans only match the
intended storage.

Each stored record gets its own name by adding `:entry:` and then either `k` plus the escaped
stripped key-expression, or `n` when the key is exactly the stripped prefix and
therefore has no remaining bytes. Escaping the separator and `%` keeps all
components unambiguous and reversible.

## Atomic updates

Put and delete use a Redis lua script to compare the complete timestamp and change
the record atomically with respect to other clients.

Timestamps are ordered as big-endian time followed by the ID's native byte
order, matching Zenoh. Older and equal timestamps are rejected. The scripts
validate key types before writing, but Redis scripts do not roll back: an
unexpected server-side failure after a write requires operator investigation.

## Discovery & Wildcards

The storage manager uses `get_all_entries` to discover wildcard query and update
targets. The backend uses Redis `SCAN` to find record keys and fetches only their
timestamps. `SCAN` is not a snapshot, so concurrent updates can appear or
disappear from an enumeration.

Zenoh wildcard requests require listing all storage keys, but Redis must scan every
key in the database even when matching a prefix. Tens of thousands of entries
should be practical in typical deployments, but wildcard performance will be
poor when the database contains millions of keys. Actual latency also depends
on workload, network and hardware; benchmark representative deployments.
Redis `COUNT` is a hint, not a hard limit on response size or server time, so wildcard
queries can also block the entire Redis server, although in practice this should be
fast enough to not cause issues.

## Deletion

There is no backend-managed expiration or tombstone. Deletion removes the record,
leaving no timestamp to reject an older put. Timestamp
comparison therefore protects only live records.

Wildcard puts and deletes do not target deleted keys in a quiescent storage,
but a concrete-key put can recreate one. Discovery and subsequent updates are
not atomic, so a wildcard put can still race with a concurrent delete after
discovery.

# Caveats

## Delete+put race conditions

Zenoh 1.10.2 reconstructs every stored entry as a PUT when it initializes its
replication state. Because there is no deleted value tombstone type, the storage
manager cannot recover or propagate delete events after restart, leading to several
potential race conditions around clustered server restarts and wildcard puts/deltes. I initially tried to handle this more robustly, but trying to do so rapidly balooned the complexity of this plugin without even handling every corner case.

## Wildcard order of operations

The storage manager's in-memory wildcard history is not persisted by the
backend, so delayed wildcard operations across restarts are not guaranteed to
preserve their original ordering.

## Actual persistance

Redis persistence, eviction, network security and backups are determined by the selected redis configuration. If you need Redis to persist on disk, have backups, and be robust across server restarts, please make sure that you have it configured to do so!
