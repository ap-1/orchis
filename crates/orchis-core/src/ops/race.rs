//! First-wins fan-out terminator.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Resolves on the first fan-out child to complete, discarding
/// all other results. The winning child's result is forwarded
/// to the next sequential step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaceOp {
    /// Pre-generated unique task identifier (doubles as barrier ID).
    pub task_id: String,

    /// Labels propagated when forwarding the winner's result.
    #[serde(default)]
    pub extra_labels: HashMap<String, String>,
}
