//! Terminated builder state. Only `.dispatch()` is available.

use std::marker::PhantomData;

use serde::Serialize;

use crate::broker::Broker;
use crate::error::PipelineError;
use crate::handle::TaskHandle;

use super::core::PipelineCore;
use super::dispatch::dispatch_pipeline;

/// Builder state after a terminal step (e.g. `.sink()`).
///
/// Only [`dispatch`](Self::dispatch) is available.
pub struct Terminated<B: Broker, In> {
    core: PipelineCore<B>,
    _phantom: PhantomData<fn(In)>,
}

impl<B: Broker, In> Terminated<B, In> {
    /// Create from the shared builder core.
    pub(crate) fn new(core: PipelineCore<B>) -> Self {
        Self {
            core,
            _phantom: PhantomData,
        }
    }

    /// Dispatch the pipeline with the given input.
    ///
    /// The output type is `()` because the pipeline ends with a
    /// fire-and-forget sink.
    pub async fn dispatch(self, input: In) -> Result<TaskHandle<B, ()>, PipelineError>
    where
        In: Serialize,
    {
        dispatch_pipeline(self.core, input).await
    }
}
