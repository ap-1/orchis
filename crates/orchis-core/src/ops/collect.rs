//! Barrier-gather terminator that collects fan-out results into a list.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Gathers all fan-out child results into a single JSON array and
/// forwards it to the next pipeline step (or stores it as the final
/// result).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CollectOp {
    /// Pre-generated unique task identifier (doubles as barrier ID).
    pub task_id: String,

    /// Labels propagated when forwarding the gathered result.
    #[serde(default)]
    pub extra_labels: HashMap<String, String>,
}
