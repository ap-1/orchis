//! Error types for the orchis pipeline library.

use thiserror::Error;

/// Errors from pipeline construction or execution.
#[derive(Debug, Error)]
pub enum PipelineError {
    /// The pipeline has no operations.
    #[error("pipeline is empty")]
    Empty,

    /// The first operation is not a `ThenOp`.
    #[error("first operation must be a then step")]
    InvalidFirstOp,

    /// A broker operation failed.
    #[error(transparent)]
    Broker(#[from] BrokerError),

    /// A barrier operation failed.
    #[error(transparent)]
    Barrier(#[from] BarrierError),

    /// Serialization or deserialization failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// A task in the pipeline returned an error.
    #[error("task failed: {0}")]
    TaskFailed(String),

    /// An internal invariant was violated (corrupted pipeline state).
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<serde_json::Error> for PipelineError {
    fn from(err: serde_json::Error) -> Self {
        Self::Serialization(err.to_string())
    }
}

/// Errors from broker operations.
#[derive(Debug, Error)]
pub enum BrokerError {
    /// Timed out waiting for a task result.
    #[error("timeout waiting for result")]
    Timeout,

    /// The broker connection failed.
    #[error("connection error: {0}")]
    Connection(String),

    /// The requested result was not found.
    #[error("result not found for task {0}")]
    NotFound(String),

    /// Serialization failed within the broker.
    #[error("serialization error: {0}")]
    Serialization(String),
}

/// Errors from barrier coordination operations.
#[derive(Debug, Error)]
pub enum BarrierError {
    /// The referenced barrier does not exist.
    #[error("barrier not found: {0}")]
    NotFound(String),

    /// The barrier backend connection failed.
    #[error("connection error: {0}")]
    Connection(String),

    /// The barrier counter reached an invalid state.
    #[error("barrier counter inconsistency: {0}")]
    Inconsistent(String),
}
