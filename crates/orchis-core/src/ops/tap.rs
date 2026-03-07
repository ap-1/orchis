//! Fire-and-forget side task that doesn't affect the pipeline result.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Dispatches a side task with the current result but forwards
/// the original result unchanged to the next step.
///
/// Side tasks carry no pipeline labels and run independently.
/// Their success or failure does not affect the main pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TapOp {
    /// Pre-generated unique task identifier.
    pub task_id: String,

    /// Fully-qualified side task name for dispatch.
    pub task_name: String,

    /// Labels propagated to the dispatched side task message.
    #[serde(default)]
    pub extra_labels: HashMap<String, String>,
}
