//! Flat-map step for nested fan-outs within a single barrier.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Label: presence indicates this child belongs to a flat-map barrier.
pub const FLAT_MAP_LABEL: &str = "_pipeline_flat_map";

/// A per-item step whose return value is an iterable that spawns
/// additional children within the same barrier.
///
/// `FlatMapOp::execute()` is never called directly. Expansion is
/// handled entirely by the middleware.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlatMapOp {
    /// Pre-generated unique task identifier.
    pub task_id: String,

    /// Fully-qualified task name for dispatch.
    pub task_name: String,

    /// Labels propagated to the dispatched task message.
    #[serde(default)]
    pub extra_labels: HashMap<String, String>,
}
