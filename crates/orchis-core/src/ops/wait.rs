//! Barrier-only fan-out terminator that discards gathered results.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Waits for all fan-out children to complete but discards their
/// results. The next sequential step receives an empty payload.
///
/// Unlike [`CollectOp`](super::CollectOp), no results are gathered
/// or forwarded.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WaitOp {
    /// Pre-generated unique task identifier (doubles as barrier ID).
    pub task_id: String,

    /// Labels propagated when forwarding the empty result.
    #[serde(default)]
    pub extra_labels: HashMap<String, String>,
}
