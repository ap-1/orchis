//! Pipeline entry point. Only `.first()` is available.

use std::collections::HashMap;
use std::sync::Arc;

use crate::broker::Broker;
use crate::ops::{StepOp, ThenOp};
use crate::task::Task;

use super::core::PipelineCore;
use super::sequential::Sequential;

/// Entry point for building a typed pipeline.
///
/// Only [`first`](Self::first) is available at this stage.
///
/// # Example
///
/// ```ignore
/// let handle = Pipeline::new(broker)
///     .first::<FindTargets>()
///     .then::<ProcessResults>()
///     .dispatch(())
///     .await?;
/// ```
pub struct Pipeline<B: Broker> {
    core: PipelineCore<B>,
}

impl<B: Broker> Pipeline<B> {
    /// Create a new pipeline builder.
    pub fn new(broker: Arc<B>) -> Self {
        Self {
            core: PipelineCore::new(broker, HashMap::new()),
        }
    }

    /// Create a new pipeline builder with extra labels propagated to
    /// every step.
    pub fn with_labels(broker: Arc<B>, extra_labels: HashMap<String, String>) -> Self {
        Self {
            core: PipelineCore::new(broker, extra_labels),
        }
    }

    /// Set the first task in the pipeline.
    ///
    /// Transitions to [`Sequential<B, T::Input, T::Output>`].
    pub fn first<T: Task>(mut self) -> Sequential<B, T::Input, T::Output> {
        let task_id = self.core.broker.generate_id();
        self.core.ops.push(StepOp::Then(ThenOp {
            task_id,
            task_name: T::NAME.to_owned(),
            retry: None,
            extra_labels: self.core.extra_labels.clone(),
        }));
        Sequential::new(self.core)
    }
}
