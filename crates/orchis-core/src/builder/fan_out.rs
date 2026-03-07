//! Fan-out builder states.

use std::marker::PhantomData;

use serde::Serialize;

use crate::broker::Broker;
use crate::ops::then::RetryPolicy;
use crate::ops::{CollectOp, FlatMapOp, RaceOp, ReduceOp, SinkOp, StepOp, TapOp, ThenOp, WaitOp};
use crate::task::Task;

use super::core::{PipelineCore, backfill_fan_out};
use super::sequential::Sequential;
use super::terminated::Terminated;

/// Builder state immediately after `.fan_out()`.
///
/// At least one per-item step must be added before terminating.
///
/// # Type Parameters
///
/// * `B`: Broker implementation.
/// * `In`: Pipeline input type.
/// * `Item`: Element type of the iterable being fanned out.
pub struct FanOutHead<B: Broker, In, Item> {
    core: PipelineCore<B>,
    _phantom: PhantomData<fn(In) -> Item>,
}

impl<B: Broker, In, Item> FanOutHead<B, In, Item> {
    /// Create from the shared builder core.
    pub(crate) fn new(core: PipelineCore<B>) -> Self {
        Self {
            core,
            _phantom: PhantomData,
        }
    }

    /// Add the first per-item task.
    ///
    /// `T::Input` must equal `Item`.
    pub fn then<T: Task<Input = Item>>(mut self) -> InFanOut<B, In, Item, T::Output> {
        let task_id = self.core.broker.generate_id();
        self.core.ops.push(StepOp::Then(ThenOp {
            task_id,
            task_name: T::NAME.to_owned(),
            retry: None,
            extra_labels: self.core.extra_labels.clone(),
        }));
        InFanOut::new(self.core)
    }

    /// Add a flat-map step as the first per-item operation.
    ///
    /// The task must return an iterable.  Each element is dispatched
    /// as a new child within the same barrier.
    pub fn flat_map<T, NewItem>(mut self) -> InFanOut<B, In, Item, NewItem>
    where
        T: Task<Input = Item>,
        T::Output: IntoIterator<Item = NewItem>,
    {
        let task_id = self.core.broker.generate_id();
        self.core.ops.push(StepOp::FlatMap(FlatMapOp {
            task_id,
            task_name: T::NAME.to_owned(),
            extra_labels: self.core.extra_labels.clone(),
        }));
        InFanOut::new(self.core)
    }

    /// Wait for all children without collecting results.
    ///
    /// Closes the fan-out context. The next step receives an empty payload.
    pub fn wait(mut self) -> Sequential<B, In, ()> {
        let task_id = self.core.broker.generate_id();
        self.core.ops.push(StepOp::Wait(WaitOp {
            task_id,
            extra_labels: self.core.extra_labels.clone(),
        }));
        backfill_fan_out(&mut self.core);
        Sequential::new(self.core)
    }

    /// Resolve on the first child to complete, discarding the rest.
    ///
    /// Closes the fan-out context. The winning child's result is forwarded.
    pub fn race(mut self) -> Sequential<B, In, Item> {
        let task_id = self.core.broker.generate_id();
        self.core.ops.push(StepOp::Race(RaceOp {
            task_id,
            extra_labels: self.core.extra_labels.clone(),
        }));
        backfill_fan_out(&mut self.core);
        Sequential::new(self.core)
    }

    /// Terminate the fan-out with a fire-and-forget sink.
    ///
    /// `T::Input` must equal `Item`.
    pub fn sink<T: Task<Input = Item>>(mut self) -> Terminated<B, In> {
        let task_id = self.core.broker.generate_id();
        self.core.ops.push(StepOp::Sink(SinkOp {
            task_id,
            task_name: T::NAME.to_owned(),
            extra_labels: self.core.extra_labels.clone(),
        }));
        backfill_fan_out(&mut self.core);
        Terminated::new(self.core)
    }
}

/// Builder state inside a fan-out context with at least one per-item step.
///
/// Available methods: [`then`](Self::then), [`flat_map`](Self::flat_map),
/// [`collect`](Self::collect), [`reduce`](Self::reduce),
/// [`sink`](Self::sink).
///
/// # Type Parameters
///
/// * `B`: Broker implementation.
/// * `In`: Pipeline input type.
/// * `Item`: Original fan-out element type, not used in bounds.
/// * `Out`: Output type of the current per-item chain tail.
pub struct InFanOut<B: Broker, In, Item, Out> {
    core: PipelineCore<B>,
    _phantom: PhantomData<fn(In, Item) -> Out>,
}

impl<B: Broker, In, Item, Out> InFanOut<B, In, Item, Out> {
    /// Create from the shared builder core.
    pub(crate) fn new(core: PipelineCore<B>) -> Self {
        Self {
            core,
            _phantom: PhantomData,
        }
    }

    /// Set a retry policy on the most recently appended per-item step.
    pub fn retry(mut self, max_retries: u32) -> Self {
        if let Some(StepOp::Then(op)) = self.core.ops.last_mut() {
            op.retry = Some(RetryPolicy { max_retries });
        }
        self
    }

    /// Add another per-item task.
    ///
    /// `T::Input` must equal `Out`.
    pub fn then<T: Task<Input = Out>>(mut self) -> InFanOut<B, In, Item, T::Output> {
        let task_id = self.core.broker.generate_id();
        self.core.ops.push(StepOp::Then(ThenOp {
            task_id,
            task_name: T::NAME.to_owned(),
            retry: None,
            extra_labels: self.core.extra_labels.clone(),
        }));
        InFanOut::new(self.core)
    }

    /// Dispatch a fire-and-forget side task for each fan-out item.
    ///
    /// The side task receives the per-item result but runs independently.
    /// The fan-out continues with the original result unchanged.
    pub fn tap<T: Task<Input = Out>>(mut self) -> InFanOut<B, In, Item, Out> {
        let task_id = self.core.broker.generate_id();
        self.core.ops.push(StepOp::Tap(TapOp {
            task_id,
            task_name: T::NAME.to_owned(),
            extra_labels: self.core.extra_labels.clone(),
        }));
        InFanOut::new(self.core)
    }

    /// Add a flat-map expansion step.
    ///
    /// The task must return an iterable. Each element becomes a new
    /// child within the same barrier.
    pub fn flat_map<T, NewItem>(mut self) -> InFanOut<B, In, Item, NewItem>
    where
        T: Task<Input = Out>,
        T::Output: IntoIterator<Item = NewItem>,
    {
        let task_id = self.core.broker.generate_id();
        self.core.ops.push(StepOp::FlatMap(FlatMapOp {
            task_id,
            task_name: T::NAME.to_owned(),
            extra_labels: self.core.extra_labels.clone(),
        }));
        InFanOut::new(self.core)
    }

    /// Gather all parallel results into a `Vec<Out>`.
    ///
    /// Closes the fan-out context and returns to sequential mode.
    pub fn collect(mut self) -> Sequential<B, In, Vec<Out>> {
        let task_id = self.core.broker.generate_id();
        self.core.ops.push(StepOp::Collect(CollectOp {
            task_id,
            extra_labels: self.core.extra_labels.clone(),
        }));
        backfill_fan_out(&mut self.core);
        Sequential::new(self.core)
    }

    /// Gather results and sequentially fold them with a reducer task.
    ///
    /// The reducer receives `(Acc, Out)` and returns `Acc`.
    ///
    /// # Arguments
    ///
    /// * `initial`: Serialisable initial accumulator value.
    pub fn reduce<T, Acc>(mut self, initial: Acc) -> Sequential<B, In, Acc>
    where
        T: Task<Input = (Acc, Out)>,
        Acc: Serialize,
    {
        let task_id = self.core.broker.generate_id();
        let initial_json =
            serde_json::to_value(&initial).expect("initial accumulator must be serialisable");
        self.core.ops.push(StepOp::Reduce(ReduceOp {
            task_id,
            task_name: T::NAME.to_owned(),
            initial: initial_json,
            extra_labels: self.core.extra_labels.clone(),
        }));
        backfill_fan_out(&mut self.core);
        Sequential::new(self.core)
    }

    /// Wait for all children without collecting results.
    ///
    /// Closes the fan-out context. The next step receives an empty payload.
    pub fn wait(mut self) -> Sequential<B, In, ()> {
        let task_id = self.core.broker.generate_id();
        self.core.ops.push(StepOp::Wait(WaitOp {
            task_id,
            extra_labels: self.core.extra_labels.clone(),
        }));
        backfill_fan_out(&mut self.core);
        Sequential::new(self.core)
    }

    /// Resolve on the first child to complete, discarding the rest.
    ///
    /// Closes the fan-out context. The winning child's result is forwarded.
    pub fn race(mut self) -> Sequential<B, In, Out> {
        let task_id = self.core.broker.generate_id();
        self.core.ops.push(StepOp::Race(RaceOp {
            task_id,
            extra_labels: self.core.extra_labels.clone(),
        }));
        backfill_fan_out(&mut self.core);
        Sequential::new(self.core)
    }

    /// Terminate the fan-out with a fire-and-forget sink.
    ///
    /// `T::Input` must equal `Out`.
    pub fn sink<T: Task<Input = Out>>(mut self) -> Terminated<B, In> {
        let task_id = self.core.broker.generate_id();
        self.core.ops.push(StepOp::Sink(SinkOp {
            task_id,
            task_name: T::NAME.to_owned(),
            extra_labels: self.core.extra_labels.clone(),
        }));
        backfill_fan_out(&mut self.core);
        Terminated::new(self.core)
    }
}
