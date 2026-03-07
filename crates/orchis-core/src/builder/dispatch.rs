//! Shared dispatch logic used by [`Sequential`] and [`Terminated`].

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::broker::Broker;
use crate::error::PipelineError;
use crate::handle::TaskHandle;
use crate::labels;
use crate::ops::StepOp;

use super::core::PipelineCore;

/// Serialise the pipeline and dispatch the first step.
///
/// Returns a [`TaskHandle`] whose `task_id` is the **last** step's
/// identifier so that `wait_result()` resolves with the pipeline's
/// final output.
pub(crate) async fn dispatch_pipeline<B, In, Out>(
    core: PipelineCore<B>,
    input: In,
) -> Result<TaskHandle<B, Out>, PipelineError>
where
    B: Broker,
    In: Serialize,
    Out: DeserializeOwned,
{
    if core.ops.is_empty() {
        return Err(PipelineError::Empty);
    }

    let first_op = match &core.ops[0] {
        StepOp::Then(op) => op,
        _ => return Err(PipelineError::InvalidFirstOp),
    };

    let serialized_ops = crate::ops::serialize_ops(&core.ops)?;
    let payload = serde_json::to_vec(&input)?;

    let mut dispatch_labels: HashMap<String, String> = HashMap::new();
    dispatch_labels.insert(labels::PIPELINE.to_owned(), serialized_ops);
    dispatch_labels.insert(labels::PIPELINE_STEP.to_owned(), "0".to_owned());
    dispatch_labels.insert(labels::RUN_ID.to_owned(), uuid::Uuid::new_v4().to_string());
    dispatch_labels.extend(first_op.extra_labels.clone());

    core.broker
        .dispatch(
            &first_op.task_name,
            &first_op.task_id,
            Bytes::from(payload),
            dispatch_labels,
        )
        .await?;

    let final_task_id = core
        .ops
        .last()
        .expect("ops is non-empty")
        .task_id()
        .to_owned();
    Ok(TaskHandle::new(Arc::clone(&core.broker), final_task_id))
}
