//! Broker trait: the interface between pipeline logic and message backends.

use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use bytes::Bytes;

use crate::error::BrokerError;
use crate::result::TaskResult;

/// Abstraction over a distributed task broker.
///
/// A broker can dispatch tasks, store results, and generate unique
/// identifiers.  Concrete implementations live in adapter crates
/// (e.g. `orchis-redis`, `orchis-apalis`).
pub trait Broker: Send + Sync + 'static {
    /// Generate a unique task identifier.
    fn generate_id(&self) -> String;

    /// Dispatch a task to the broker.
    ///
    /// # Arguments
    ///
    /// * `task_name`: Fully-qualified task name.
    /// * `task_id`: Pre-generated unique identifier for this invocation.
    /// * `payload`: Serialised task input (JSON bytes).
    /// * `labels`: Metadata for routing, tracking, and pipeline state.
    fn dispatch(
        &self,
        task_name: &str,
        task_id: &str,
        payload: Bytes,
        labels: HashMap<String, String>,
    ) -> impl Future<Output = Result<(), BrokerError>> + Send;

    /// Persist a task result so it can be retrieved later.
    fn set_result(
        &self,
        task_id: &str,
        result: TaskResult,
    ) -> impl Future<Output = Result<(), BrokerError>> + Send;

    /// Retrieve a task result by identifier.
    ///
    /// Returns `None` when the result is not yet available.
    fn get_result(
        &self,
        task_id: &str,
    ) -> impl Future<Output = Result<Option<TaskResult>, BrokerError>> + Send;

    /// Wait until a task result is available.
    ///
    /// Implementations should use an efficient mechanism such as
    /// pub/sub or blocking pop rather than polling.
    ///
    /// # Arguments
    ///
    /// * `task_id`: The task whose result to wait for.
    /// * `timeout`: Maximum time to wait.  `None` waits indefinitely.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Timeout`] if the deadline elapses.
    fn wait_result(
        &self,
        task_id: &str,
        timeout: Option<Duration>,
    ) -> impl Future<Output = Result<TaskResult, BrokerError>> + Send;
}
