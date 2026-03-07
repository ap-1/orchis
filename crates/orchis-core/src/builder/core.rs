//! Shared mutable state for all pipeline builder stages.

use std::collections::HashMap;
use std::sync::Arc;

use crate::broker::Broker;
use crate::ops::StepOp;

/// Internal state shared across builder typestate transitions.
pub(crate) struct PipelineCore<B: Broker> {
    /// Broker used for ID generation and eventual dispatch.
    pub broker: Arc<B>,

    /// Accumulated list of pipeline operations.
    pub ops: Vec<StepOp>,

    /// Index of the active [`FanOutOp`](crate::ops::FanOutOp) in
    /// `ops`, or `None` when outside a fan-out context.
    pub fan_out_index: Option<usize>,

    /// Labels propagated to every step operation.
    pub extra_labels: HashMap<String, String>,
}

impl<B: Broker> PipelineCore<B> {
    /// Create a new core with an empty ops list.
    pub fn new(broker: Arc<B>, extra_labels: HashMap<String, String>) -> Self {
        Self {
            broker,
            ops: Vec::new(),
            fan_out_index: None,
            extra_labels,
        }
    }
}

/// Backfill the active `FanOutOp` with the index of its terminator.
///
/// Must be called exactly once when a terminator (`CollectOp`,
/// `ReduceOp`, or `SinkOp`) is appended to `core.ops`.
///
/// # Panics
///
/// Panics if `fan_out_index` is `None` or does not point to a
/// `FanOutOp`.
pub(crate) fn backfill_fan_out<B: Broker>(core: &mut PipelineCore<B>) {
    let idx = core
        .fan_out_index
        .take()
        .expect("backfill_fan_out called outside fan-out context");
    let terminator_index = core.ops.len() - 1;
    match &mut core.ops[idx] {
        StepOp::FanOut(op) => op.terminator_index = terminator_index,
        _ => panic!("fan_out_index does not point to a FanOutOp"),
    }
}
