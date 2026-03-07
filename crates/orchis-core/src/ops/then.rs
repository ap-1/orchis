//! Sequential step that dispatches a single task with the previous result.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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

    /// Labels propagated to the dispatched task message.
    #[serde(default)]
    pub extra_labels: HashMap<String, String>,
}
