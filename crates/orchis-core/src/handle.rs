//! Handle to a dispatched pipeline.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;

use crate::broker::Broker;
use crate::error::PipelineError;

/// A handle to a running pipeline.
///
/// The type parameter `T` is the pipeline's final output type,
/// carried at the type level for safe deserialisation in
/// [`wait_result`](Self::wait_result).
pub struct TaskHandle<B: Broker, T> {
    broker: Arc<B>,
    task_id: String,
    _output: PhantomData<T>,
}

impl<B: Broker, T> TaskHandle<B, T> {
    /// Create a new handle.
    pub(crate) fn new(broker: Arc<B>, task_id: String) -> Self {
        Self {
            broker,
            task_id,
            _output: PhantomData,
        }
    }

    /// The task identifier of the pipeline's final step.
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Wait indefinitely for the pipeline to complete and deserialise
    /// the result.
    ///
    /// # Errors
    ///
    /// * [`PipelineError::TaskFailed`] if a task in the pipeline errored.
    /// * [`PipelineError::Broker`] on broker communication failure.
    /// * [`PipelineError::Serialization`] if the result cannot be
    ///   deserialised into `T`.
    pub async fn wait_result(self) -> Result<T, PipelineError>
    where
        T: DeserializeOwned,
    {
        self.wait_impl(None).await
    }

    /// Wait for the pipeline to complete with a timeout.
    ///
    /// # Errors
    ///
    /// Same as [`wait_result`](Self::wait_result), plus
    /// [`PipelineError::Broker`] wrapping [`BrokerError::Timeout`](crate::error::BrokerError::Timeout).
    pub async fn wait_result_timeout(self, timeout: Duration) -> Result<T, PipelineError>
    where
        T: DeserializeOwned,
    {
        self.wait_impl(Some(timeout)).await
    }

    async fn wait_impl(self, timeout: Option<Duration>) -> Result<T, PipelineError>
    where
        T: DeserializeOwned,
    {
        let result = self.broker.wait_result(&self.task_id, timeout).await?;
        match result.payload {
            Ok(data) => Ok(serde_json::from_slice(&data)?),
            Err(message) => Err(PipelineError::TaskFailed(message)),
        }
    }
}
