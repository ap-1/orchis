//! Fire-and-forget terminal step.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Dispatches a task with the current result but does not wait for
/// its completion. The pipeline resolves immediately with an empty
/// payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SinkOp {
    /// Pre-generated unique task identifier.
    pub task_id: String,

    /// Fully-qualified task name for dispatch.
    pub task_name: String,

    /// Labels propagated to the dispatched task message.
    #[serde(default)]
    pub extra_labels: HashMap<String, String>,
}
