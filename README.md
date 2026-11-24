# Zenoh Redis storage backend

A storage-manager backend for Eclipse Zenoh 1.10.2 and a standalone Redis instance.

The design goal is to allow fast and robust zenoh persistant storage by leveraging Redis as a very performant caching+persistance layer (provided that Redis is configured for persistance). ValKey should work as well, but is untested.

Redis Cluster is rejected at storage creation because the backend does not currently handle cluster redirects, scanning a cluster, or HGET batching on a cluster. Zenoh storage-manager replication is also rejected.

## Build and configure

Build using the exact Rust toolchain and Zenoh versions used to build the
target router and storage-manager plugin:

```sh
cargo build --release
```

Place the resulting `libzenoh_backend_redis` library on the storage-manager
backend search path, or configure `__path__` on the volume. The router must
have timestamping enabled.

For example, save this configuration as `zenoh-redis.json5`:

```json5
{
  plugins: {
    storage_manager: {
      volumes: {
        redis: {
          url: "redis://127.0.0.1:6379",
          key_prefix: "zenoh",
        },
      },
      storages: {
        telemetry: {
          key_expr: "site1/telemetry/**",
          strip_prefix: "site1/telemetry",
          volume: {
            id: "redis",
            namespace: "telemetry",
          },
        },
      },
    },
  },
}
```

Adjust the Redis address and storage namespace for your deployment, then start
the router with `zenohd -c zenoh-redis.json5`. See
[config/zenoh-redis.json5](config/zenoh-redis.json5) for optional authentication
and TLS settings.

Each logically distinct storage requires a unique `namespace` within its
volume's `key_prefix`. Router instances intentionally sharing one logical
storage must use the same namespace and matching settings. 
This is not well tested but should work fine.

### Implementation notes:

* Stored values do not expire.
* An accepted delete removes the record, meaning a delayed older put can recreate it immediately,
including after restart. 
* Zenoh's in-memory ordering may reject some delayed updates, but the backend does not retain delete history.
* Wildcard discovery returns live keys only
* Wildcard puts do not select deleted keys
* Concurrent deletes can race with wildcard puts
* The storage manager's wildcard history is not recovered after restart; delayed
wildcard operations across restarts are not guaranteed to preserve ordering.
* In Zenoh 1.10.2, a delayed older put overridden by a wildcard delete may
instead be stored as an empty value at the delete's timestamp. This is a
storage-manager issue, tracked upstream in
[eclipse-zenoh/zenoh#2649](https://github.com/eclipse-zenoh/zenoh/issues/2649).

### Configuration notes:
The `private` volume object accepts an optional Redis ACL `username` and
`password`, or a password alone. URLs must not contain credentials. `rediss://`
uses TLS with system trust roots. Optional PEM CA and client certificate/key
files can be configured under `tls.private`; see the example. 

Config files containing passwords should be protected. 

The Redis account needs permission for INFO, hash, SCAN, DEL, TYPE, EXISTS and
script commands used by the backend. A Redis connection and its cluster status
are checked when each storage is created.

The backend expects Redis to use `maxmemory-policy noeviction`. Eviction can
remove records and cause data loss. That said, the backend does not enforce this setting.

Avoid modifying or delete backend keys directly through Redis, since that is not a tested path for this plugin.

For deployments requiring disk persistence, configure Redis persistence and
recovery appropriately. Redis may still lose acknowledged data depending on
persistence and failure settings. The backend advertises volatile persistence
because it cannot guarantee the server's configuration.

Wildcard discovery scans the Redis database keyspace; see
[DESIGN.md](DESIGN.md) for performance considerations.

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
