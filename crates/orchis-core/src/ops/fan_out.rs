//! Structural scatter operation that fans an iterable into parallel children.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Label: barrier identifier (the terminator's `task_id`).
pub const BARRIER_LABEL: &str = "_pipeline_fan_out_barrier";

/// Label: child index within a simple (non-flat-map) fan-out.
pub const INDEX_LABEL: &str = "_pipeline_fan_out_index";

/// Label: total number of children in a simple fan-out.
pub const TOTAL_LABEL: &str = "_pipeline_fan_out_total";

/// Label: JSON-encoded list of all pre-generated child task IDs.
pub const IDS_LABEL: &str = "_pipeline_fan_out_ids";

/// Label: pipeline index of the fan-out terminator.
pub const TERMINATOR_INDEX_LABEL: &str = "_pipeline_fan_out_terminator_index";

/// A structural scatter operation.
///
/// `FanOutOp` has no task of its own.  When executed by the middleware
/// it deserialises the previous result as a JSON array, then dispatches
/// one instance of the next per-item step for each element.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FanOutOp {
    /// Pre-generated unique identifier (used for labelling, not dispatch).
    pub task_id: String,

    /// Pipeline index of the matching terminator (`CollectOp`, `ReduceOp`,
    /// or `SinkOp`).  Set to `0` as a placeholder during construction and
    /// backfilled when the terminator is appended.
    pub terminator_index: usize,

    /// Labels propagated to all child task messages.
    #[serde(default)]
    pub extra_labels: HashMap<String, String>,
}
