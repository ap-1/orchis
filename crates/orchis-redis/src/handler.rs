//! Type-erased task handler and typed registration wrapper.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use orchis_core::task::Task;

/// A type-erased async handler: takes serialized input, returns serialized output.
pub type BoxedHandler =
    Arc<dyn Fn(Bytes) -> Pin<Box<dyn Future<Output = Result<Bytes, String>> + Send>> + Send + Sync>;

/// Wrap a typed async closure into a [`BoxedHandler`].
///
/// Deserialization of `T::Input` and serialization of `T::Output`
/// happen inside the wrapper. Serde errors become `Err(String)`.
pub fn wrap_handler<T, F, Fut>(f: F) -> BoxedHandler
where
    T: Task,
    F: Fn(T::Input) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = T::Output> + Send + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |payload: Bytes| {
        let f = Arc::clone(&f);
        Box::pin(async move {
            let input: T::Input = serde_json::from_slice(&payload)
                .map_err(|e| format!("failed to deserialize input: {e}"))?;
            let output = f(input).await;
            let bytes = serde_json::to_vec(&output)
                .map_err(|e| format!("failed to serialize output: {e}"))?;
            Ok(Bytes::from(bytes))
        })
    })
}
