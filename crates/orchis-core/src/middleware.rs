//! Pipeline advancement middleware.
//!
//! [`PipelineMiddleware`] contains the broker-agnostic logic that
//! drives pipeline execution.  Adapter crates hook
//! [`post_save`](PipelineMiddleware::post_save) and
//! [`on_error`](PipelineMiddleware::on_error) into their framework's
//! task-completion lifecycle.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::try_join_all;

use crate::barrier::BarrierBackend;
use crate::broker::Broker;
use crate::context::PipelineContext;
use crate::error::PipelineError;
use crate::labels;
use crate::ops::fan_out::{
    BARRIER_LABEL, IDS_LABEL, INDEX_LABEL, TERMINATOR_INDEX_LABEL, TOTAL_LABEL,
};
use crate::ops::flat_map::FLAT_MAP_LABEL;
use crate::ops::{self, StepOp};
use crate::result::TaskResult;

/// Core pipeline advancement logic.
///
/// Generic over the broker and barrier backend. Adapter crates are
/// responsible for calling its methods at the right time in their
/// task-completion lifecycle.
pub struct PipelineMiddleware<B: Broker, BB: BarrierBackend> {
    broker: Arc<B>,
    barrier: Option<Arc<BB>>,
}

impl<B: Broker, BB: BarrierBackend> PipelineMiddleware<B, BB> {
    /// Create a new middleware instance.
    ///
    /// # Arguments
    ///
    /// * `broker`: Shared broker for dispatching continuations.
    /// * `barrier`: Barrier backend for fan-out coordination.
    ///   `None` is valid when no fan-out steps exist, but will cause
    ///   a runtime error if a fan-out is encountered.
    pub fn new(broker: Arc<B>, barrier: Option<Arc<BB>>) -> Self {
        Self { broker, barrier }
    }

    /// Called after a task result has been persisted.
    ///
    /// Determines whether the completed task is part of a pipeline
    /// and, if so, advances to the next step.
    ///
    /// # Arguments
    ///
    /// * `task_id`: Identifier of the completed task.
    /// * `labels`: Labels from the completed task's message.
    /// * `result`: The persisted task result.
    #[tracing::instrument(skip(self, labels, result), fields(pipeline_step))]
    pub async fn post_save(
        &self,
        task_id: &str,
        labels: &HashMap<String, String>,
        result: &TaskResult,
    ) -> Result<(), PipelineError> {
        if result.payload.is_err() {
            return self.abort_pipeline(labels, result).await;
        }

        if labels.contains_key(BARRIER_LABEL) {
            return self.handle_fan_out_child(task_id, labels, result).await;
        }

        let Some(raw_pipeline) = labels.get(labels::PIPELINE) else {
            return Ok(());
        };
        let Some(raw_step) = labels.get(labels::PIPELINE_STEP) else {
            return Ok(());
        };

        let current_index: usize = raw_step.parse().map_err(|_| {
            PipelineError::Internal(format!("unparseable pipeline step index: {raw_step}"))
        })?;
        let steps: Arc<[StepOp]> = ops::deserialize_ops(raw_pipeline)?.into();
        let next_index = current_index + 1;

        if next_index >= steps.len() {
            return Ok(());
        }

        let ctx = PipelineContext {
            broker: Arc::clone(&self.broker),
            barrier: self.barrier.as_ref().map(Arc::clone),
            raw_pipeline: Arc::from(raw_pipeline.as_str()),
            steps,
            current_index: next_index,
            source_labels: Arc::new(labels.clone()),
        };

        self.execute_step(&ctx, result).await
    }

    /// Called when a task raises an unhandled error.
    ///
    /// Writes the error result to the final step's identifier so
    /// that callers waiting on the pipeline receive the failure.
    pub async fn on_error(
        &self,
        labels: &HashMap<String, String>,
        result: &TaskResult,
    ) -> Result<(), PipelineError> {
        self.abort_pipeline(labels, result).await
    }

    /// Execute the operation at `ctx.current_index`.
    #[tracing::instrument(skip_all, fields(step_index = ctx.current_index))]
    fn execute_step<'a>(
        &'a self,
        ctx: &'a PipelineContext<B, BB>,
        result: &'a TaskResult,
    ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>> {
        Box::pin(async move {
            match &ctx.steps[ctx.current_index] {
                StepOp::Then(op) => self.execute_then(ctx, op, result).await,
                StepOp::Tap(op) => self.execute_tap(ctx, op, result).await,
                StepOp::FanOut(op) => self.execute_fan_out(ctx, op, result).await,
                StepOp::Collect(op) => self.execute_collect(ctx, op, result).await,
                StepOp::Wait(op) => self.execute_wait(ctx, op, result).await,
                StepOp::Race(op) => self.execute_race(ctx, op, result).await,
                StepOp::Reduce(op) => self.execute_reduce(ctx, op, result).await,
                StepOp::Sink(op) => self.execute_sink(ctx, op, result).await,
                StepOp::FlatMap(_) => Err(PipelineError::Internal(
                    "FlatMapOp.execute() called directly".to_owned(),
                )),
            }
        })
    }

    /// Dispatch the next sequential task.
    async fn execute_then(
        &self,
        ctx: &PipelineContext<B, BB>,
        op: &ops::ThenOp,
        result: &TaskResult,
    ) -> Result<(), PipelineError> {
        let mut dispatch_labels =
            self.new_dispatch_labels(&ctx.source_labels, 3 + op.extra_labels.len());
        dispatch_labels.insert(labels::PIPELINE.to_owned(), (*ctx.raw_pipeline).to_owned());
        dispatch_labels.insert(
            labels::PIPELINE_STEP.to_owned(),
            ctx.current_index.to_string(),
        );
        dispatch_labels.extend(op.extra_labels.clone());

        self.broker
            .dispatch(
                &op.task_name,
                &op.task_id,
                result.expect_data()?.clone(),
                dispatch_labels,
            )
            .await?;
        Ok(())
    }

    /// Dispatch a fire-and-forget side task, then forward the original
    /// result to the next step.
    async fn execute_tap(
        &self,
        ctx: &PipelineContext<B, BB>,
        op: &ops::TapOp,
        result: &TaskResult,
    ) -> Result<(), PipelineError> {
        let mut side_labels =
            self.new_dispatch_labels(&ctx.source_labels, 1 + op.extra_labels.len());
        side_labels.extend(op.extra_labels.clone());

        self.broker
            .dispatch(
                &op.task_name,
                &op.task_id,
                result.expect_data()?.clone(),
                side_labels,
            )
            .await?;

        let next_index = ctx.current_index + 1;
        if next_index >= ctx.steps.len() {
            self.broker
                .set_result(op.task_id.as_str(), result.clone())
                .await?;
            return Ok(());
        }

        let next_ctx = ctx.at_index(next_index);
        self.execute_step(&next_ctx, result).await
    }

    /// First-wins: forward the winning child's result to the next step.
    async fn execute_race(
        &self,
        ctx: &PipelineContext<B, BB>,
        op: &ops::RaceOp,
        result: &TaskResult,
    ) -> Result<(), PipelineError> {
        let next_index = ctx.current_index + 1;
        if next_index >= ctx.steps.len() {
            self.broker.set_result(&op.task_id, result.clone()).await?;
            return Ok(());
        }

        let next_ctx = ctx.at_index(next_index);
        self.execute_step(&next_ctx, result).await
    }

    /// Barrier-discard: forward an empty result to the next step.
    async fn execute_wait(
        &self,
        ctx: &PipelineContext<B, BB>,
        op: &ops::WaitOp,
        _result: &TaskResult,
    ) -> Result<(), PipelineError> {
        let empty = TaskResult::success(Bytes::from_static(b"null"));

        let next_index = ctx.current_index + 1;
        if next_index >= ctx.steps.len() {
            self.broker.set_result(&op.task_id, empty).await?;
            return Ok(());
        }

        let next_ctx = ctx.at_index(next_index);
        self.execute_step(&next_ctx, &empty).await
    }

    /// Fan out an iterable into parallel children.
    async fn execute_fan_out(
        &self,
        ctx: &PipelineContext<B, BB>,
        op: &ops::FanOutOp,
        result: &TaskResult,
    ) -> Result<(), PipelineError> {
        let raw_items: Vec<serde_json::Value> = serde_json::from_slice(result.expect_data()?)?;
        let items: Vec<serde_json::Value> = match op.chunk_size {
            Some(size) if size > 0 => raw_items
                .chunks(size)
                .map(|chunk| serde_json::Value::Array(chunk.to_vec()))
                .collect(),
            _ => raw_items,
        };
        let terminator = &ctx.steps[op.terminator_index];

        if items.is_empty() {
            return self
                .handle_empty_fan_out(ctx, op.terminator_index, terminator)
                .await;
        }

        let has_flat_map = ctx.steps[ctx.current_index + 1..op.terminator_index]
            .iter()
            .any(|s| matches!(s, StepOp::FlatMap(_)));

        if has_flat_map {
            self.dispatch_fan_out_flat_map(ctx, op, &items).await
        } else {
            self.dispatch_fan_out_simple(ctx, op, &items).await
        }
    }

    /// Simple fan-out: pre-generate child IDs, dispatch concurrently.
    async fn dispatch_fan_out_simple(
        &self,
        ctx: &PipelineContext<B, BB>,
        op: &ops::FanOutOp,
        items: &[serde_json::Value],
    ) -> Result<(), PipelineError> {
        let first_per_item_index = ctx.current_index + 1;
        let first_per_item = match &ctx.steps[first_per_item_index] {
            StepOp::Then(t) => t,
            other => {
                return Err(PipelineError::Internal(format!(
                    "expected ThenOp as first per-item step, got {:?}",
                    std::mem::discriminant(other)
                )));
            }
        };

        let terminator = &ctx.steps[op.terminator_index];
        let child_ids: Vec<String> = items.iter().map(|_| self.broker.generate_id()).collect();
        let child_ids_json: Arc<str> = Arc::from(serde_json::to_string(&child_ids)?.as_str());
        let total_str: Arc<str> = Arc::from(items.len().to_string().as_str());
        let step_str: Arc<str> = Arc::from(first_per_item_index.to_string().as_str());
        let terminator_str: Arc<str> = Arc::from(op.terminator_index.to_string().as_str());
        let barrier_id: Arc<str> = Arc::from(terminator.task_id());
        let label_count = 8 + first_per_item.extra_labels.len();

        let futs = items
            .iter()
            .zip(&child_ids)
            .enumerate()
            .map(|(idx, (item, child_id))| {
                let child_ids_json = Arc::clone(&child_ids_json);
                let total_str = Arc::clone(&total_str);
                let step_str = Arc::clone(&step_str);
                let terminator_str = Arc::clone(&terminator_str);
                let barrier_id = Arc::clone(&barrier_id);
                async move {
                    let payload = serde_json::to_vec(item)?;
                    let mut dispatch_labels =
                        self.new_dispatch_labels(&ctx.source_labels, label_count);
                    dispatch_labels
                        .insert(labels::PIPELINE.to_owned(), (*ctx.raw_pipeline).to_owned());
                    dispatch_labels
                        .insert(labels::PIPELINE_STEP.to_owned(), (*step_str).to_owned());
                    dispatch_labels.insert(BARRIER_LABEL.to_owned(), (*barrier_id).to_owned());
                    dispatch_labels.insert(INDEX_LABEL.to_owned(), idx.to_string());
                    dispatch_labels.insert(TOTAL_LABEL.to_owned(), (*total_str).to_owned());
                    dispatch_labels.insert(IDS_LABEL.to_owned(), (*child_ids_json).to_owned());
                    dispatch_labels.insert(
                        TERMINATOR_INDEX_LABEL.to_owned(),
                        (*terminator_str).to_owned(),
                    );
                    dispatch_labels.extend(first_per_item.extra_labels.clone());

                    self.broker
                        .dispatch(
                            &first_per_item.task_name,
                            child_id,
                            Bytes::from(payload),
                            dispatch_labels,
                        )
                        .await?;
                    Ok::<_, PipelineError>(())
                }
            });

        try_join_all(futs).await?;
        Ok(())
    }

    /// Flat-map fan-out: initialise pending counter, dispatch children concurrently.
    async fn dispatch_fan_out_flat_map(
        &self,
        ctx: &PipelineContext<B, BB>,
        op: &ops::FanOutOp,
        items: &[serde_json::Value],
    ) -> Result<(), PipelineError> {
        let barrier = self.require_barrier()?;

        let terminator = &ctx.steps[op.terminator_index];
        let barrier_id = terminator.task_id();

        barrier.init_flat_map(barrier_id, items.len()).await?;

        let first_per_item_index = ctx.current_index + 1;
        let first_per_item_name = match &ctx.steps[first_per_item_index] {
            StepOp::Then(t) => &t.task_name,
            StepOp::FlatMap(f) => &f.task_name,
            other => {
                return Err(PipelineError::Internal(format!(
                    "expected ThenOp or FlatMapOp as first per-item step, got {:?}",
                    std::mem::discriminant(other)
                )));
            }
        };

        let step_str: Arc<str> = Arc::from(first_per_item_index.to_string().as_str());
        let terminator_str: Arc<str> = Arc::from(op.terminator_index.to_string().as_str());
        let barrier_arc: Arc<str> = Arc::from(barrier_id);

        let child_ids: Vec<String> = items.iter().map(|_| self.broker.generate_id()).collect();

        let futs = items.iter().zip(&child_ids).map(|(item, child_id)| {
            let step_str = Arc::clone(&step_str);
            let terminator_str = Arc::clone(&terminator_str);
            let barrier_arc = Arc::clone(&barrier_arc);
            async move {
                let payload = serde_json::to_vec(item)?;
                let mut dispatch_labels = self.new_dispatch_labels(&ctx.source_labels, 6);
                dispatch_labels.insert(labels::PIPELINE.to_owned(), (*ctx.raw_pipeline).to_owned());
                dispatch_labels.insert(labels::PIPELINE_STEP.to_owned(), (*step_str).to_owned());
                dispatch_labels.insert(BARRIER_LABEL.to_owned(), (*barrier_arc).to_owned());
                dispatch_labels.insert(
                    TERMINATOR_INDEX_LABEL.to_owned(),
                    (*terminator_str).to_owned(),
                );
                dispatch_labels.insert(FLAT_MAP_LABEL.to_owned(), "1".to_owned());

                self.broker
                    .dispatch(
                        first_per_item_name,
                        child_id,
                        Bytes::from(payload),
                        dispatch_labels,
                    )
                    .await?;
                Ok::<_, PipelineError>(())
            }
        });

        try_join_all(futs).await?;
        Ok(())
    }

    /// Handle an empty fan-out by synthesising an empty gathered result.
    async fn handle_empty_fan_out(
        &self,
        ctx: &PipelineContext<B, BB>,
        terminator_index: usize,
        terminator: &StepOp,
    ) -> Result<(), PipelineError> {
        let empty_payload = match terminator {
            StepOp::Collect(_) | StepOp::Reduce(_) => Bytes::from_static(b"[]"),
            _ => Bytes::from_static(b"null"),
        };
        let synthetic = TaskResult::success(empty_payload);

        let next_after_terminator = terminator_index + 1;
        if next_after_terminator >= ctx.steps.len() {
            self.broker
                .set_result(terminator.task_id(), synthetic)
                .await?;
            return Ok(());
        }

        let next_ctx = ctx.at_index(next_after_terminator);
        self.execute_step(&next_ctx, &synthetic).await
    }

    /// Forward gathered results or store as final.
    async fn execute_collect(
        &self,
        ctx: &PipelineContext<B, BB>,
        op: &ops::CollectOp,
        result: &TaskResult,
    ) -> Result<(), PipelineError> {
        let next_index = ctx.current_index + 1;
        if next_index >= ctx.steps.len() {
            self.broker.set_result(&op.task_id, result.clone()).await?;
            return Ok(());
        }

        let next_ctx = ctx.at_index(next_index);
        self.execute_step(&next_ctx, result).await
    }

    /// Sequentially fold gathered results with a reducer task.
    async fn execute_reduce(
        &self,
        ctx: &PipelineContext<B, BB>,
        op: &ops::ReduceOp,
        result: &TaskResult,
    ) -> Result<(), PipelineError> {
        let items: Vec<serde_json::Value> = serde_json::from_slice(result.expect_data()?)?;
        let mut accumulator: serde_json::Value = op.initial.clone();

        for item in &items {
            let pair = serde_json::Value::Array(vec![accumulator, item.clone()]);
            let payload = serde_json::to_vec(&pair)?;
            let fold_id = self.broker.generate_id();

            let mut dispatch_labels =
                self.new_dispatch_labels(&ctx.source_labels, 1 + op.extra_labels.len());
            dispatch_labels.extend(op.extra_labels.clone());

            self.broker
                .dispatch(
                    &op.task_name,
                    &fold_id,
                    Bytes::from(payload),
                    dispatch_labels,
                )
                .await?;

            let fold_result = self.broker.wait_result(&fold_id, None).await?;
            if fold_result.payload.is_err() {
                let final_id = ctx.steps.last().expect("steps non-empty").task_id();
                self.broker.set_result(final_id, fold_result).await?;
                tracing::warn!("reduce aborted due to task error");
                return Ok(());
            }

            accumulator = serde_json::from_slice(fold_result.expect_data()?)?;
        }

        let final_payload = serde_json::to_vec(&accumulator)?;
        let acc_result = TaskResult::success(Bytes::from(final_payload));

        let next_index = ctx.current_index + 1;
        if next_index >= ctx.steps.len() {
            self.broker.set_result(&op.task_id, acc_result).await?;
            return Ok(());
        }

        let next_ctx = ctx.at_index(next_index);
        self.execute_step(&next_ctx, &acc_result).await
    }

    /// Fire-and-forget dispatch, store empty result immediately.
    async fn execute_sink(
        &self,
        ctx: &PipelineContext<B, BB>,
        op: &ops::SinkOp,
        result: &TaskResult,
    ) -> Result<(), PipelineError> {
        let mut dispatch_labels =
            self.new_dispatch_labels(&ctx.source_labels, 1 + op.extra_labels.len());
        dispatch_labels.extend(op.extra_labels.clone());

        self.broker
            .dispatch(
                &op.task_name,
                &op.task_id,
                result.expect_data()?.clone(),
                dispatch_labels,
            )
            .await?;

        let empty = TaskResult::success(Bytes::from_static(b"null"));
        self.broker.set_result(&op.task_id, empty).await?;
        Ok(())
    }

    /// Handle completion of a fan-out child task.
    #[tracing::instrument(skip(self, labels, result))]
    async fn handle_fan_out_child(
        &self,
        task_id: &str,
        labels: &HashMap<String, String>,
        result: &TaskResult,
    ) -> Result<(), PipelineError> {
        let raw_pipeline = labels.get(labels::PIPELINE).ok_or(PipelineError::Empty)?;
        let current_index: usize = labels
            .get(labels::PIPELINE_STEP)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| {
                PipelineError::Internal("missing or invalid pipeline step index".to_owned())
            })?;
        let terminator_index: usize = labels
            .get(TERMINATOR_INDEX_LABEL)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| {
                PipelineError::Internal("missing or invalid terminator index".to_owned())
            })?;

        let steps: Arc<[StepOp]> = ops::deserialize_ops(raw_pipeline)?.into();
        let raw_pipeline: Arc<str> = Arc::from(raw_pipeline.as_str());

        // If the completed task is a FlatMap step, expand its result
        // into new children before advancing.
        if matches!(&steps[current_index], StepOp::FlatMap(_)) {
            return self
                .handle_flat_map_expansion(
                    task_id,
                    labels,
                    &raw_pipeline,
                    &steps,
                    current_index,
                    result,
                )
                .await;
        }

        let next_per_item_index = current_index + 1;

        if next_per_item_index < terminator_index {
            // Dispatch any consecutive TapOps as side tasks, then
            // continue with the first non-tap op.
            let mut idx = next_per_item_index;
            while idx < terminator_index {
                if let StepOp::Tap(tap_op) = &steps[idx] {
                    self.dispatch_side_task(labels, tap_op, result).await?;
                    idx += 1;
                } else {
                    break;
                }
            }

            if idx >= terminator_index {
                // All remaining per-item ops were taps; fall through to barrier.
            } else {
                let next_op = &steps[idx];
                match next_op {
                    StepOp::FlatMap(_) => {
                        return self
                            .handle_flat_map_expansion(
                                task_id,
                                labels,
                                &raw_pipeline,
                                &steps,
                                idx,
                                result,
                            )
                            .await;
                    }
                    StepOp::Then(then_op) => {
                        return self
                            .dispatch_per_item_continuation(
                                task_id,
                                labels,
                                &raw_pipeline,
                                then_op,
                                idx,
                                result,
                            )
                            .await;
                    }
                    other => {
                        return Err(PipelineError::Internal(format!(
                            "unexpected op type at per-item index {idx}: {:?}",
                            std::mem::discriminant(other)
                        )));
                    }
                }
            }
        }

        self.dispatch_to_terminator(
            task_id,
            labels,
            &raw_pipeline,
            &steps,
            terminator_index,
            result,
        )
        .await
    }

    /// Dispatch the next per-item continuation task.
    /// Route a completed fan-out child to the appropriate terminator strategy.
    async fn dispatch_to_terminator(
        &self,
        task_id: &str,
        labels: &HashMap<String, String>,
        raw_pipeline: &Arc<str>,
        steps: &Arc<[StepOp]>,
        terminator_index: usize,
        result: &TaskResult,
    ) -> Result<(), PipelineError> {
        match &steps[terminator_index] {
            StepOp::Sink(sink_op) => {
                let sink_op = sink_op.clone();
                let ctx = PipelineContext {
                    broker: Arc::clone(&self.broker),
                    barrier: self.barrier.as_ref().map(Arc::clone),
                    raw_pipeline: Arc::clone(raw_pipeline),
                    steps: Arc::clone(steps),
                    current_index: terminator_index,
                    source_labels: Arc::new(labels.clone()),
                };
                self.execute_sink(&ctx, &sink_op, result).await
            }
            StepOp::Race(_) => {
                self.handle_race_completion(
                    task_id,
                    labels,
                    raw_pipeline,
                    steps,
                    terminator_index,
                    result,
                )
                .await
            }
            _ if labels.contains_key(FLAT_MAP_LABEL) => {
                self.handle_flat_map_barrier_completion(
                    task_id,
                    labels,
                    raw_pipeline,
                    steps,
                    terminator_index,
                )
                .await
            }
            _ => {
                self.handle_simple_barrier_completion(labels, raw_pipeline, steps, terminator_index)
                    .await
            }
        }
    }

    async fn dispatch_per_item_continuation(
        &self,
        child_id: &str,
        source_labels: &HashMap<String, String>,
        raw_pipeline: &str,
        next_op: &ops::ThenOp,
        next_index: usize,
        result: &TaskResult,
    ) -> Result<(), PipelineError> {
        let mut dispatch_labels =
            self.new_dispatch_labels(source_labels, 8 + next_op.extra_labels.len());
        dispatch_labels.insert(labels::PIPELINE.to_owned(), raw_pipeline.to_owned());
        dispatch_labels.insert(labels::PIPELINE_STEP.to_owned(), next_index.to_string());

        for key in &[BARRIER_LABEL, TERMINATOR_INDEX_LABEL] {
            if let Some(val) = source_labels.get(*key) {
                dispatch_labels.insert((*key).to_owned(), val.clone());
            }
        }

        if source_labels.contains_key(FLAT_MAP_LABEL) {
            dispatch_labels.insert(FLAT_MAP_LABEL.to_owned(), "1".to_owned());
        } else {
            for key in &[INDEX_LABEL, TOTAL_LABEL, IDS_LABEL] {
                if let Some(val) = source_labels.get(*key) {
                    dispatch_labels.insert((*key).to_owned(), val.clone());
                }
            }
        }

        dispatch_labels.extend(next_op.extra_labels.clone());

        self.broker
            .dispatch(
                &next_op.task_name,
                child_id,
                result.expect_data()?.clone(),
                dispatch_labels,
            )
            .await?;
        Ok(())
    }

    /// Handle flat-map expansion: adjust pending counter, dispatch children.
    async fn handle_flat_map_expansion(
        &self,
        _task_id: &str,
        source_labels: &HashMap<String, String>,
        raw_pipeline: &str,
        steps: &[StepOp],
        flat_map_index: usize,
        result: &TaskResult,
    ) -> Result<(), PipelineError> {
        let barrier = self.require_barrier()?;

        let flat_map_op = match &steps[flat_map_index] {
            StepOp::FlatMap(op) => op,
            _ => unreachable!("index verified by caller"),
        };

        let barrier_id = source_labels
            .get(BARRIER_LABEL)
            .ok_or(PipelineError::Empty)?;
        let terminator_index: usize = source_labels
            .get(TERMINATOR_INDEX_LABEL)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| {
                PipelineError::Internal("missing or invalid terminator index".to_owned())
            })?;

        let items: Vec<serde_json::Value> = serde_json::from_slice(result.expect_data()?)?;
        let pending = barrier.expand_flat_map(barrier_id, items.len()).await?;

        if items.is_empty() && pending == 0 {
            return self
                .fire_flat_map_barrier(
                    barrier_id,
                    raw_pipeline,
                    steps,
                    terminator_index,
                    source_labels,
                )
                .await;
        }

        // Dispatch expanded children to the step after the flat-map.
        let next_index = flat_map_index + 1;
        let next_task_name = match steps.get(next_index) {
            Some(StepOp::Then(op)) => &op.task_name,
            Some(other) => {
                return Err(PipelineError::Internal(format!(
                    "expected ThenOp after FlatMapOp, got {:?}",
                    std::mem::discriminant(other)
                )));
            }
            None => {
                return Err(PipelineError::Internal(
                    "no step after FlatMapOp".to_owned(),
                ));
            }
        };

        let step_str: Arc<str> = Arc::from(next_index.to_string().as_str());
        let terminator_str: Arc<str> = Arc::from(terminator_index.to_string().as_str());
        let barrier_arc: Arc<str> = Arc::from(barrier_id.as_ref());
        let child_ids: Vec<String> = items.iter().map(|_| self.broker.generate_id()).collect();

        let futs = items.iter().zip(&child_ids).map(|(item, child_id)| {
            let step_str = Arc::clone(&step_str);
            let terminator_str = Arc::clone(&terminator_str);
            let barrier_arc = Arc::clone(&barrier_arc);
            async move {
                let payload = serde_json::to_vec(item)?;
                let mut dispatch_labels = self.new_dispatch_labels(source_labels, 6);
                dispatch_labels.insert(labels::PIPELINE.to_owned(), raw_pipeline.to_owned());
                dispatch_labels.insert(labels::PIPELINE_STEP.to_owned(), (*step_str).to_owned());
                dispatch_labels.insert(BARRIER_LABEL.to_owned(), (*barrier_arc).to_owned());
                dispatch_labels.insert(
                    TERMINATOR_INDEX_LABEL.to_owned(),
                    (*terminator_str).to_owned(),
                );
                dispatch_labels.insert(FLAT_MAP_LABEL.to_owned(), "1".to_owned());

                self.broker
                    .dispatch(
                        next_task_name,
                        child_id,
                        Bytes::from(payload),
                        dispatch_labels,
                    )
                    .await?;
                Ok::<_, PipelineError>(())
            }
        });

        try_join_all(futs).await?;

        Ok(())
    }

    /// Race barrier completion: first child to win claims the race.
    async fn handle_race_completion(
        &self,
        task_id: &str,
        labels: &HashMap<String, String>,
        raw_pipeline: &Arc<str>,
        steps: &Arc<[StepOp]>,
        terminator_index: usize,
        result: &TaskResult,
    ) -> Result<(), PipelineError> {
        let barrier = self.require_barrier()?;
        let terminator = &steps[terminator_index];
        let barrier_id = terminator.task_id();

        let won = barrier.try_win_race(barrier_id, task_id).await?;
        if !won {
            return Ok(());
        }

        barrier.delete(barrier_id).await?;

        let ctx = PipelineContext {
            broker: Arc::clone(&self.broker),
            barrier: self.barrier.as_ref().map(Arc::clone),
            raw_pipeline: Arc::clone(raw_pipeline),
            steps: Arc::clone(steps),
            current_index: terminator_index,
            source_labels: Arc::new(labels.clone()),
        };
        self.execute_step(&ctx, result).await
    }

    /// Simple barrier completion: INCR, check total, gather results.
    async fn handle_simple_barrier_completion(
        &self,
        labels: &HashMap<String, String>,
        raw_pipeline: &Arc<str>,
        steps: &Arc<[StepOp]>,
        terminator_index: usize,
    ) -> Result<(), PipelineError> {
        let barrier = self.require_barrier()?;

        let terminator = &steps[terminator_index];
        let barrier_id = terminator.task_id();
        let total: usize = labels
            .get(TOTAL_LABEL)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| {
                PipelineError::Internal("missing or invalid terminator index".to_owned())
            })?;

        let completed = barrier.increment_simple(barrier_id).await?;
        if completed < total {
            return Ok(());
        }

        barrier.delete(barrier_id).await?;

        let child_ids: Vec<String> = labels
            .get(IDS_LABEL)
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        let gathered = self.gather_results(&child_ids).await?;
        let gathered_payload = serde_json::to_vec(&gathered)?;
        let gathered_result = TaskResult::success(Bytes::from(gathered_payload));

        let ctx = PipelineContext {
            broker: Arc::clone(&self.broker),
            barrier: self.barrier.as_ref().map(Arc::clone),
            raw_pipeline: Arc::clone(raw_pipeline),
            steps: Arc::clone(steps),
            current_index: terminator_index,
            source_labels: Arc::new(labels.clone()),
        };
        self.execute_step(&ctx, &gathered_result).await
    }

    /// Flat-map barrier completion: push leaf ID, decrement pending.
    async fn handle_flat_map_barrier_completion(
        &self,
        task_id: &str,
        labels: &HashMap<String, String>,
        raw_pipeline: &Arc<str>,
        steps: &Arc<[StepOp]>,
        terminator_index: usize,
    ) -> Result<(), PipelineError> {
        let barrier = self.require_barrier()?;

        let barrier_id = labels.get(BARRIER_LABEL).ok_or(PipelineError::Empty)?;

        let pending = barrier.complete_leaf(barrier_id, task_id).await?;
        if pending > 0 {
            return Ok(());
        }

        self.fire_flat_map_barrier(barrier_id, raw_pipeline, steps, terminator_index, labels)
            .await
    }

    /// Fire a flat-map barrier: gather leaf results, execute terminator.
    async fn fire_flat_map_barrier(
        &self,
        barrier_id: &str,
        raw_pipeline: &str,
        steps: &[StepOp],
        terminator_index: usize,
        source_labels: &HashMap<String, String>,
    ) -> Result<(), PipelineError> {
        let barrier = self.require_barrier()?;

        let leaf_ids = barrier.get_leaf_ids(barrier_id).await?;
        barrier.delete(barrier_id).await?;

        let gathered = self.gather_results(&leaf_ids).await?;
        let gathered_payload = serde_json::to_vec(&gathered)?;
        let gathered_result = TaskResult::success(Bytes::from(gathered_payload));

        let ctx = PipelineContext {
            broker: Arc::clone(&self.broker),
            barrier: self.barrier.as_ref().map(Arc::clone),
            raw_pipeline: Arc::from(raw_pipeline),
            steps: Arc::from(steps),
            current_index: terminator_index,
            source_labels: Arc::new(source_labels.clone()),
        };
        self.execute_step(&ctx, &gathered_result).await
    }

    /// Fetch results for a list of task IDs concurrently.
    ///
    /// Called after a barrier fires, so all results are already stored.
    /// Uses `get_result` instead of `wait_result` to avoid pub/sub
    /// overhead for data that is already available.
    #[tracing::instrument(skip(self), fields(child_count = task_ids.len()))]
    async fn gather_results(
        &self,
        task_ids: &[String],
    ) -> Result<Vec<serde_json::Value>, PipelineError> {
        let futs = task_ids.iter().map(|id| {
            let id = id.clone();
            async move {
                let result = self.broker.get_result(&id).await?.ok_or_else(|| {
                    PipelineError::Internal(format!(
                        "barrier fired but result missing for task {id}"
                    ))
                })?;
                let value: serde_json::Value = serde_json::from_slice(result.expect_data()?)?;
                Ok::<_, PipelineError>(value)
            }
        });
        try_join_all(futs).await
    }

    /// Write an error result to the final step so waiters see the failure.
    async fn abort_pipeline(
        &self,
        labels: &HashMap<String, String>,
        result: &TaskResult,
    ) -> Result<(), PipelineError> {
        let Some(raw_pipeline) = labels.get(labels::PIPELINE) else {
            return Ok(());
        };
        let steps = ops::deserialize_ops(raw_pipeline)?;
        if steps.is_empty() {
            return Ok(());
        }
        let final_id = steps.last().expect("steps non-empty").task_id();
        self.broker.set_result(final_id, result.clone()).await?;
        tracing::warn!(final_task_id = final_id, "pipeline aborted");
        Ok(())
    }

    /// Dispatch a fire-and-forget side task (no pipeline labels).
    async fn dispatch_side_task(
        &self,
        source_labels: &HashMap<String, String>,
        op: &ops::TapOp,
        result: &TaskResult,
    ) -> Result<(), PipelineError> {
        let mut side_labels = self.new_dispatch_labels(source_labels, 1 + op.extra_labels.len());
        side_labels.extend(op.extra_labels.clone());

        self.broker
            .dispatch(
                &op.task_name,
                &op.task_id,
                result.expect_data()?.clone(),
                side_labels,
            )
            .await?;
        Ok(())
    }

    /// Get the barrier backend or return an error.
    fn require_barrier(&self) -> Result<&Arc<BB>, PipelineError> {
        self.barrier.as_ref().ok_or(PipelineError::Barrier(
            crate::error::BarrierError::Connection("no barrier backend configured".to_owned()),
        ))
    }

    /// Build a pre-sized label map seeded with tracking labels from the source.
    fn new_dispatch_labels(
        &self,
        source: &HashMap<String, String>,
        capacity: usize,
    ) -> HashMap<String, String> {
        let mut labels = HashMap::with_capacity(capacity);
        if let Some(run_id) = source.get(labels::RUN_ID) {
            labels.insert(labels::RUN_ID.to_owned(), run_id.clone());
        }
        labels
    }
}
