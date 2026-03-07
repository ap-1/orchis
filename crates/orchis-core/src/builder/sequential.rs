//! Sequential builder state, reached after `.first()` or `.collect()`.

use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::broker::Broker;
use crate::error::PipelineError;
use crate::handle::TaskHandle;
use crate::ops::then::RetryPolicy;
use crate::ops::{FanOutOp, SinkOp, StepOp, TapOp, ThenOp};
use crate::task::Task;

use super::core::PipelineCore;
use super::dispatch::dispatch_pipeline;
use super::fan_out::FanOutHead;
use super::terminated::Terminated;

/// Builder state where the next step is sequential.
///
/// Available methods: [`then`](Self::then), [`fan_out`](Self::fan_out),
/// [`sink`](Self::sink), [`dispatch`](Self::dispatch).
///
/// # Type Parameters
///
/// * `B`: Broker implementation.
/// * `In`: Input type of the pipeline's first task (needed for
///   [`dispatch`](Self::dispatch)).
/// * `Out`: Output type of the most recently appended task.
pub struct Sequential<B: Broker, In, Out> {
    core: PipelineCore<B>,
    _phantom: PhantomData<fn(In) -> Out>,
}

impl<B: Broker, In, Out> Sequential<B, In, Out> {
    /// Create from the shared builder core.
    pub(crate) fn new(core: PipelineCore<B>) -> Self {
        Self {
            core,
            _phantom: PhantomData,
        }
    }

    /// Set a retry policy on the most recently appended step.
    ///
    /// When the task returns an error, the middleware re-dispatches it
    /// up to `max_retries` times before aborting the pipeline.
    pub fn retry(mut self, max_retries: u32) -> Self {
        if let Some(StepOp::Then(op)) = self.core.ops.last_mut() {
            op.retry = Some(RetryPolicy { max_retries });
        }
        self
    }

    /// Append a sequential task.
    ///
    /// A mismatch between `T::Input` and `Out` is a compile error.
    pub fn then<T: Task<Input = Out>>(mut self) -> Sequential<B, In, T::Output> {
        let task_id = self.core.broker.generate_id();
        self.core.ops.push(StepOp::Then(ThenOp {
            task_id,
            task_name: T::NAME.to_owned(),
            retry: None,
            extra_labels: self.core.extra_labels.clone(),
        }));
        Sequential::new(self.core)
    }

    /// Dispatch a fire-and-forget side task without affecting the pipeline.
    ///
    /// The side task receives the current result but runs independently.
    /// The pipeline continues with the original result unchanged.
    pub fn tap<T: Task<Input = Out>>(mut self) -> Sequential<B, In, Out> {
        let task_id = self.core.broker.generate_id();
        self.core.ops.push(StepOp::Tap(TapOp {
            task_id,
            task_name: T::NAME.to_owned(),
            extra_labels: self.core.extra_labels.clone(),
        }));
        Sequential::new(self.core)
    }

    /// Open a fan-out context.
    ///
    /// `Out` must be iterable.  Subsequent per-item steps operate on
    /// each element in parallel.
    pub fn fan_out<Item>(mut self) -> FanOutHead<B, In, Item>
    where
        Out: IntoIterator<Item = Item>,
    {
        let task_id = self.core.broker.generate_id();
        let idx = self.core.ops.len();
        self.core.ops.push(StepOp::FanOut(FanOutOp {
            task_id,
            terminator_index: 0,
            chunk_size: None,
            extra_labels: self.core.extra_labels.clone(),
        }));
        self.core.fan_out_index = Some(idx);
        FanOutHead::new(self.core)
    }

    /// Open a chunked fan-out context.
    ///
    /// Items are grouped into batches of `chunk_size` before
    /// dispatching. Each per-item task receives `Vec<Item>`.
    pub fn fan_out_chunked<Item>(mut self, chunk_size: usize) -> FanOutHead<B, In, Vec<Item>>
    where
        Out: IntoIterator<Item = Item>,
    {
        let task_id = self.core.broker.generate_id();
        let idx = self.core.ops.len();
        self.core.ops.push(StepOp::FanOut(FanOutOp {
            task_id,
            terminator_index: 0,
            chunk_size: Some(chunk_size),
            extra_labels: self.core.extra_labels.clone(),
        }));
        self.core.fan_out_index = Some(idx);
        FanOutHead::new(self.core)
    }

    /// Append a fire-and-forget terminal step.
    ///
    /// `T::Input` must equal `Out`.
    pub fn sink<T: Task<Input = Out>>(mut self) -> Terminated<B, In> {
        let task_id = self.core.broker.generate_id();
        self.core.ops.push(StepOp::Sink(SinkOp {
            task_id,
            task_name: T::NAME.to_owned(),
            extra_labels: self.core.extra_labels.clone(),
        }));
        Terminated::new(self.core)
    }

    /// Dispatch the pipeline with the given input.
    ///
    /// Returns a [`TaskHandle`] that resolves with the pipeline's
    /// final output.
    pub async fn dispatch(self, input: In) -> Result<TaskHandle<B, Out>, PipelineError>
    where
        In: Serialize,
        Out: DeserializeOwned,
    {
        dispatch_pipeline(self.core, input).await
    }
}
