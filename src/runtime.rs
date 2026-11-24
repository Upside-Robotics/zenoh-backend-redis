use std::{future::Future, sync::OnceLock};
use tokio::runtime::{Builder, Runtime};
use zenoh::Result;

fn runtime() -> Result<&'static Runtime> {
    static RUNTIME: OnceLock<std::result::Result<Runtime, String>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|e| e.to_string())
        })
        .as_ref()
        .map_err(|e| format!("cannot create Redis backend runtime: {e}").into())
}

pub fn spawn<F>(future: F) -> Result<tokio::task::JoinHandle<F::Output>>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    Ok(runtime()?.spawn(future))
}

pub async fn run<F, T>(future: F) -> Result<T>
where
    F: Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    spawn(future)?
        .await
        .map_err(|e| format!("Redis backend task failed: {e}"))?
}
