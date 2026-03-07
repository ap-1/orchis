//! Sequential step that dispatches a single task with the previous result.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Retry policy for a task step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts (not counting the initial try).
    pub max_retries: u32,
}

/// A sequential pipeline step.
///
/// Dispatches one task whose input is the serialised output of the
/// preceding step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ThenOp {
    /// Pre-generated unique task identifier.
    pub task_id: String,

    /// Fully-qualified task name for dispatch.
    pub task_name: String,

    /// Retry policy applied when the task returns an error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryPolicy>,

    /// Labels propagated to the dispatched task message.
    #[serde(default)]
    pub extra_labels: HashMap<String, String>,
}
