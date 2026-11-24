#!/bin/sh
set -eu

fixture_dir=$(mktemp -d)
redis_pid=
tls_pid=

cleanup() {
    for pid in "$redis_pid" "$tls_pid"; do
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    rm -f "$fixture_dir/ca.key" "$fixture_dir/ca.crt" "$fixture_dir/ca.srl" \
        "$fixture_dir/server.key" "$fixture_dir/server.csr" "$fixture_dir/server.crt" \
        "$fixture_dir/redis.log" "$fixture_dir/tls.log" "$fixture_dir/openssl.log"
    rmdir "$fixture_dir"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$fixture_dir/ca.key" -out "$fixture_dir/ca.crt" -days 1 \
    -subj '/CN=Redis Test CA' -addext 'basicConstraints=critical,CA:TRUE' \
    -addext 'keyUsage=critical,keyCertSign,cRLSign' 2>"$fixture_dir/openssl.log" || {
    cat "$fixture_dir/openssl.log" >&2
    exit 1
}
openssl req -new -newkey rsa:2048 -nodes \
    -keyout "$fixture_dir/server.key" -out "$fixture_dir/server.csr" \
    -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
    -addext 'extendedKeyUsage=serverAuth' 2>"$fixture_dir/openssl.log" || {
    cat "$fixture_dir/openssl.log" >&2
    exit 1
}
openssl x509 -req -in "$fixture_dir/server.csr" -CA "$fixture_dir/ca.crt" \
    -CAkey "$fixture_dir/ca.key" -CAcreateserial -out "$fixture_dir/server.crt" \
    -days 1 -copy_extensions copy

redis-server --bind 127.0.0.1 --port 16379 --save '' --appendonly no \
    --dir "$fixture_dir" --logfile "$fixture_dir/redis.log" &
redis_pid=$!
redis-server --bind 127.0.0.1 --port 0 --tls-port 16380 \
    --tls-cert-file "$fixture_dir/server.crt" --tls-key-file "$fixture_dir/server.key" \
    --tls-ca-cert-file "$fixture_dir/ca.crt" --tls-auth-clients no \
    --requirepass tls-fixture-password --save '' --appendonly no \
    --dir "$fixture_dir" --logfile "$fixture_dir/tls.log" &
tls_pid=$!

ready=false
for attempt in $(seq 1 50); do
    if redis-cli -h 127.0.0.1 -p 16379 ping >/dev/null 2>&1 \
        && REDISCLI_AUTH=tls-fixture-password redis-cli --tls --cacert "$fixture_dir/ca.crt" \
            -h localhost -p 16380 ping >/dev/null 2>&1; then
        ready=true
        break
    fi
    if ! kill -0 "$redis_pid" 2>/dev/null || ! kill -0 "$tls_pid" 2>/dev/null; then
        break
    fi
    sleep 0.1
done
if [ "$ready" != true ]; then
    echo "Redis fixtures failed to become ready" >&2
    cat "$fixture_dir/redis.log" "$fixture_dir/tls.log" >&2
    exit 1
fi

export REDIS_URL=redis://127.0.0.1:16379
export REDIS_TEST_TLS_URL=rediss://localhost:16380
export REDIS_TEST_TLS_CA="$fixture_dir/ca.crt"
export REDIS_TEST_TLS_PASSWORD=tls-fixture-password
export ZENOH_E2E_ROUTER=/opt/zenoh/usr/bin/zenohd
export ZENOH_E2E_STORAGE_PLUGIN=/opt/zenoh/usr/lib/libzenoh_plugin_storage_manager.so
export ZENOH_E2E_BACKEND=/workspace/target/debug/libzenoh_backend_redis.so

cargo test --locked --test tls_test --test end_to_end -- --ignored --nocapture
