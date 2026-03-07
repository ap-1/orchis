//! Runtime context passed to the middleware during pipeline advancement.

use std::collections::HashMap;
use std::sync::Arc;

use crate::barrier::BarrierBackend;
use crate::broker::Broker;
use crate::ops::StepOp;

/// Execution context for a single pipeline advancement step.
///
/// All reference-counted fields are shared across step transitions
/// via [`at_index`](Self::at_index) without cloning.
pub struct PipelineContext<B: Broker, BB: BarrierBackend> {
    /// Broker used for dispatching continuations and storing results.
    pub broker: Arc<B>,

    /// Barrier backend for fan-out coordination.
    ///
    /// `None` when the pipeline has no fan-out steps.
    pub barrier: Option<Arc<BB>>,

    /// The serialised pipeline JSON, reused verbatim in dispatch labels.
    pub raw_pipeline: Arc<str>,

    /// The full list of deserialised operations.
    pub steps: Arc<[StepOp]>,

    /// Index of the operation currently being executed.
    pub current_index: usize,

    /// Labels from the source task message (for tracking propagation).
    pub source_labels: Arc<HashMap<String, String>>,
}

impl<B: Broker, BB: BarrierBackend> PipelineContext<B, BB> {
    /// Derive a new context pointing at a different step index.
    pub fn at_index(&self, index: usize) -> Self {
        Self {
            broker: Arc::clone(&self.broker),
            barrier: self.barrier.as_ref().map(Arc::clone),
            raw_pipeline: Arc::clone(&self.raw_pipeline),
            steps: Arc::clone(&self.steps),
            current_index: index,
            source_labels: Arc::clone(&self.source_labels),
        }
    }
}
