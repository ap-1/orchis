//! Sequential fold terminator for fan-out results.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// After the barrier fires, sequentially folds the gathered results
/// by dispatching a reducer task for each `(accumulator, item)` pair.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReduceOp {
    /// Pre-generated unique task identifier.
    pub task_id: String,

    /// Fully-qualified reducer task name.
    pub task_name: String,

    /// Serialised initial accumulator value.
    pub initial: serde_json::Value,

    /// Labels propagated to reducer task messages.
    #[serde(default)]
    pub extra_labels: HashMap<String, String>,
}
