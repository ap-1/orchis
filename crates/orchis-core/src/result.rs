//! Task result type that flows between pipeline steps.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// The outcome of a single task execution.
///
/// The pipeline middleware passes this between steps without
/// interpreting the payload. Deserialisation happens at the
/// boundaries when dispatching and waiting for results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    /// Serialised return value on success, or error message on failure.
    pub payload: Result<Bytes, String>,

    /// Wall-clock execution time in seconds.
    pub execution_time: f64,
}

impl TaskResult {
    /// Build a successful result from serialised data.
    pub fn success(data: Bytes) -> Self {
        Self {
            payload: Ok(data),
            execution_time: 0.0,
        }
    }

    /// Build an error result.
    pub fn error(message: String) -> Self {
        Self {
            payload: Err(message),
            execution_time: 0.0,
        }
    }

    /// The serialised data bytes, if the task succeeded.
    ///
    /// Returns `None` when the task failed.
    pub fn data(&self) -> Option<&Bytes> {
        self.payload.as_ref().ok()
    }

    /// The serialised data bytes, or an error suitable for pipeline
    /// middleware that has already filtered out error results.
    pub fn expect_data(&self) -> Result<&Bytes, crate::error::PipelineError> {
        self.payload.as_ref().map_err(|msg| {
            crate::error::PipelineError::Internal(format!(
                "expected successful result but got error: {msg}"
            ))
        })
    }

    /// Deserialise a successful payload into a concrete type.
    ///
    /// # Errors
    ///
    /// Returns [`serde_json::Error`] if the task failed or
    /// deserialisation fails.
    pub fn deserialize_payload<T: for<'de> Deserialize<'de>>(
        &self,
    ) -> Result<T, serde_json::Error> {
        match &self.payload {
            Ok(data) => serde_json::from_slice(data),
            Err(message) => Err(serde::de::Error::custom(message)),
        }
    }
}
