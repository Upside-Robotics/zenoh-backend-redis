# Zenoh Redis storage backend

## License

This project is licensed under the Apache License, Version 2.0.

Copyright 2026 Upside Robotics

A storage-manager backend for Eclipse Zenoh 1.10.2 and a standalone Redis instance.

The design goal is to allow fast and robust zenoh persistant storage by leveraging Redis as a very performant caching+persistance layer (provided that Redis is configured for persistance). ValKey should work as well, but is untested.

Redis Cluster is rejected at storage creation because the backend does not currently handle cluster redirects, scanning a cluster, or HGET batching on a cluster. Zenoh storage-manager replication is also rejected.

## Debian packages

Download the `.deb` matching your server's architecture (`amd64` or `arm64`)
from [GitHub Releases](https://github.com/Upside-Robotics/zenoh-backend-redis/releases).
Packages are built on Debian Bookworm using the pinned Rust toolchain.
The matching `zenoh-plugin-storage-manager` version must be available from
your configured APT repositories.

```sh
sudo apt install ./zenoh-backend-redis_1.10.2-1_amd64.deb
```

The package installs `/usr/lib/libzenoh_backend_redis.so`, documentation, and
example configurations under `/usr/share/doc/zenoh-backend-redis/examples/`.
It does not install or configure a Redis server, modify the router configuration,
or restart Zenoh. Configure a Redis volume and storage as described below,
then restart your router to load the backend. Set the volume's
`__path__: ["/usr/lib/libzenoh_backend_redis.so"]` if it is not found automatically.

### Publishing a release

The Debian release workflow runs when a `v*` tag is pushed. The tag must be
`v` followed by the version in `Cargo.toml`. After both architecture builds
and their unit/Redis integration tests succeed, the workflow creates or updates
the tag's GitHub Release and attaches both `.deb` files. The TLS and router
end-to-end tests remain separate; see [Testing](#testing).

```sh
git tag v1.10.2
git push origin v1.10.2
```

The workflow can also be rerun manually from GitHub Actions with an existing
version tag. It uses GitHub's built-in `GITHUB_TOKEN` with `contents: write`
only in the publishing job; no custom tokens or Eclipse credentials are required.
Organization policies must allow the referenced actions and release writes.
Keep the Rust toolchain, Zenoh dependencies, and Debian storage-manager dependency
in sync with the intended router build.

To build a package locally on Debian with Rust, `dpkg-dev`, and `pkg-config`:

```sh
cargo install cargo-deb --version 3.8.0 --locked
cargo deb --locked
```

Packages are written to `target/debian/`.

## Build and configure

Build using the exact Rust toolchain and Zenoh versions used to build the
target router and storage-manager plugin. The default build uses the pinned
Rust toolchain and matches the official Zenoh 1.10.2 Debian packages:

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
script commands used by the backend, plus SELECT when using a non-default
database. A Redis connection and its cluster status are checked when each
storage is created.

The backend expects Redis to use `maxmemory-policy noeviction`. Eviction can
remove records and cause data loss. That said, the backend does not enforce this setting.

Avoid modifying or delete backend keys directly through Redis, since that is not a tested path for this plugin.

For deployments requiring disk persistence, configure Redis persistence and
recovery appropriately. Redis may still lose acknowledged data depending on
persistence and failure settings. The backend advertises volatile persistence
because it cannot guarantee the server's configuration.

An example standalone Redis server configuration is provided in
[config/redis.conf](config/redis.conf). It uses AOF with an RDB-formatted
base after AOF rewrites and `appendfsync everysec`, which can lose recent
writes on a crash. Adjust the memory limit and ensure Redis's working directory
is writable and on persistent storage. The example listens only on localhost;
configure authentication and TLS if remote access is needed.

Wildcard discovery scans the Redis database keyspace; see
[DESIGN.md](DESIGN.md) for performance considerations.

## Testing

```sh
docker compose -f tests/docker-compose.yml up -d
cargo test
```

Integration tests use `REDIS_URL` if set, otherwise `redis://127.0.0.1:6379`.
To run the TLS and router end-to-end tests in an isolated container:

```sh
docker compose -f tests/docker-compose.yml run --rm --build isolated-tests
```

The fixture uses the official Zenoh 1.10.2 Debian packages on AMD64 or ARM64,
starts disposable Redis servers, and generates a temporary CA and server
certificate. No ports are published. Docker and network access to build the
image are required.

To run these tests against existing fixtures instead:
For the router end-to-end test, build the library with `cargo build --lib`,
set `ZENOH_E2E_ROUTER` to a matching `zenohd` binary and
`ZENOH_E2E_STORAGE_PLUGIN` to its storage-manager shared library, then run
`cargo test --test end_to_end -- --ignored`. `ZENOH_E2E_BACKEND` can override
the default debug backend library path.
An authenticated TLS Redis fixture can additionally be tested with
`REDIS_TEST_TLS_URL`, `REDIS_TEST_TLS_CA` (PEM CA file) and
`REDIS_TEST_TLS_PASSWORD` set, using `cargo test --test tls_test -- --ignored`.
For operational behavior and limitations see [DESIGN.md](DESIGN.md).
