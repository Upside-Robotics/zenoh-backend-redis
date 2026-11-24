use std::{
    collections::HashMap,
    fs,
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use redis::AsyncCommands;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Router(Child);

impl Drop for Router {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_router(binary: &str, config: &Path) -> TestResult<Router> {
    Ok(Router(
        Command::new(binary)
            .arg("-c")
            .arg(config)
            .env("RUST_LOG", "warn")
            .spawn()?,
    ))
}

async fn connect(endpoint: &str, router: &mut Router) -> TestResult<zenoh::Session> {
    for _ in 0..100 {
        if let Some(status) = router.0.try_wait()? {
            return Err(format!("zenohd exited during startup: {status}").into());
        }
        if TcpStream::connect(endpoint).is_ok() {
            let mut config = zenoh::Config::default();
            config.insert_json5("mode", "\"client\"")?;
            config.insert_json5(
                "connect/endpoints",
                &serde_json::to_string(&[format!("tcp/{endpoint}")])?,
            )?;
            config.insert_json5("scouting/multicast/enabled", "false")?;
            return zenoh::open(config).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("zenohd did not start listening".into())
}

async fn query(session: &zenoh::Session, expression: &str) -> TestResult<HashMap<String, Vec<u8>>> {
    let replies = session
        .get(expression)
        .timeout(Duration::from_millis(500))
        .await?;
    let mut result = HashMap::new();
    while let Ok(reply) = replies.recv_async().await {
        let sample = reply.into_result()?;
        result.insert(
            sample.key_expr().to_string(),
            sample.payload().to_bytes().into_owned(),
        );
    }
    Ok(result)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Zenoh 1.10.2 router, storage-manager plugin, built backend and Redis"]
async fn router_storage_wildcards_and_restart() -> TestResult {
    let binary = std::env::var("ZENOH_E2E_ROUTER")?;
    let storage_plugin = std::env::var("ZENOH_E2E_STORAGE_PLUGIN")?;
    let backend = std::env::var("ZENOH_E2E_BACKEND").unwrap_or_else(|_| {
        format!(
            "{}/target/debug/libzenoh_backend_redis.so",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
    let token = format!(
        "e2e{}{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let root = format!("test/{token}");
    let first = format!("{root}/a");
    let sibling = format!("{root}/c");
    let second = format!("{root}/a/b");
    let encoded = |s: &str| s.bytes().map(|b| format!("{b:02x}")).collect::<String>();
    let base = format!("zenoh-redis:v1:{}:{}", encoded("e2e"), encoded(&token));
    let first_entry = format!("{base}:entry:k{}", encoded("a"));
    let mut conn = redis::Client::open(redis_url.as_str())?
        .get_multiplexed_async_connection()
        .await?;
    let endpoint = {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.local_addr()?.to_string()
    };
    let config_file: PathBuf = std::env::temp_dir().join(format!("zenoh-redis-{token}.json5"));
    let config = serde_json::json!({
        "mode": "router",
        "listen": {"endpoints": [format!("tcp/{endpoint}")]},
        "scouting": {"multicast": {"enabled": false}},
        "plugins": {
            "storage_manager": {
                "__required__": true,
                "__path__": [storage_plugin],
                "volumes": {"redis": {"__required__": true, "__path__": [backend], "url": redis_url, "key_prefix": "e2e"}},
                "storages": {"test": {
                    "key_expr": format!("{root}/**"), "strip_prefix": root,
                    "volume": {"id": "redis", "namespace": token}
                }}
            }
        }
    });
    fs::write(&config_file, serde_json::to_vec(&config)?)?;
    let mut router = start_router(&binary, &config_file)?;
    let session = connect(&endpoint, &mut router).await?;
    let mut ready = false;
    for _ in 0..20 {
        session.put(first.as_str(), "first").await?;
        if query(&session, &first).await?.get(&first) == Some(&b"first".to_vec()) {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(ready, "backend did not store a publication");
    session.put(root.as_str(), "prefix").await?;
    let mut prefix_ready = false;
    for _ in 0..20 {
        if query(&session, &root).await?.get(&root) == Some(&b"prefix".to_vec()) {
            prefix_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(prefix_ready, "exact strip_prefix key was not stored");
    session.put(second.as_str(), "second").await?;
    for _ in 0..20 {
        if query(&session, &second).await?.contains_key(&second) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let single = query(&session, &format!("{root}/*")).await?;
    assert_eq!(single.len(), 1);
    assert_eq!(single.get(&first), Some(&b"first".to_vec()));
    let recursive = query(&session, &format!("{root}/**")).await?;
    assert_eq!(recursive.len(), 3);
    assert_eq!(recursive.get(&second), Some(&b"second".to_vec()));
    assert_eq!(recursive.get(&root), Some(&b"prefix".to_vec()));
    session.delete(format!("{root}/*")).await?;
    for _ in 0..20 {
        if !query(&session, &first).await?.contains_key(&first) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!query(&session, &first).await?.contains_key(&first));
    session.put(sibling.as_str(), "sibling").await?;
    let mut sibling_ready = false;
    for _ in 0..20 {
        if query(&session, &sibling).await?.get(&sibling) == Some(&b"sibling".to_vec()) {
            sibling_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(sibling_ready, "live wildcard target was not stored");
    session.put(format!("{root}/*"), "wildcard").await?;
    let mut updated = false;
    for _ in 0..20 {
        if query(&session, &sibling).await?.get(&sibling) == Some(&b"wildcard".to_vec()) {
            updated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(updated, "wildcard put did not update live target");
    assert!(!query(&session, &first).await?.contains_key(&first));
    let exists: bool = conn.exists(&first_entry).await?;
    assert!(!exists);
    drop(session);
    drop(router);
    let mut router = start_router(&binary, &config_file)?;
    let session = connect(&endpoint, &mut router).await?;
    let mut recovered = false;
    for _ in 0..20 {
        if query(&session, &second).await?.get(&second) == Some(&b"second".to_vec()) {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(recovered, "live data was not recovered after restart");
    assert!(!query(&session, &first).await?.contains_key(&first));
    session.put(format!("{root}/*"), "after-restart").await?;
    let mut updated = false;
    for _ in 0..20 {
        if query(&session, &sibling).await?.get(&sibling) == Some(&b"after-restart".to_vec()) {
            updated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        updated,
        "wildcard put did not update live target after restart"
    );
    assert!(!query(&session, &first).await?.contains_key(&first));
    let exists: bool = conn.exists(&first_entry).await?;
    assert!(!exists);
    session.delete(format!("{root}/*")).await?;
    for _ in 0..20 {
        if !query(&session, &sibling).await?.contains_key(&sibling) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!query(&session, &sibling).await?.contains_key(&sibling));
    assert!(!query(&session, &first).await?.contains_key(&first));
    let exists: bool = conn.exists(&first_entry).await?;
    assert!(!exists);
    assert_eq!(
        query(&session, &second).await?.get(&second),
        Some(&b"second".to_vec())
    );
    session.put(sibling.as_str(), "restored").await?;
    let mut sibling_ready = false;
    for _ in 0..20 {
        if query(&session, &sibling).await?.get(&sibling) == Some(&b"restored".to_vec()) {
            sibling_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(sibling_ready, "live wildcard target was not restored");
    session.put(format!("{root}/*"), "after-restore").await?;
    let mut updated = false;
    for _ in 0..20 {
        if query(&session, &sibling).await?.get(&sibling) == Some(&b"after-restore".to_vec()) {
            updated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(updated, "wildcard put did not update restored live target");
    assert!(!query(&session, &first).await?.contains_key(&first));
    drop(session);
    drop(router);
    fs::remove_file(config_file)?;

    for index in [format!("{base}:active")] {
        let members: Vec<String> = redis::cmd("SMEMBERS")
            .arg(&index)
            .query_async(&mut conn)
            .await?;
        for member in members {
            let _: usize = redis::cmd("DEL").arg(member).query_async(&mut conn).await?;
        }
        let _: usize = redis::cmd("DEL").arg(index).query_async(&mut conn).await?;
    }
    Ok(())
}
