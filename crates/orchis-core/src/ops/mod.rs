//! Pipeline operation types and serialisation helpers.
//!
//! Each pipeline is a sequence of [`StepOp`] values that are serialised
//! to JSON and carried in task message labels.  The middleware
//! deserialises them on each task completion to determine the next
//! action.

pub mod collect;
pub mod fan_out;
pub mod flat_map;
pub mod race;
pub mod reduce;
pub mod sink;
pub mod tap;
pub mod then;
pub mod wait;

use serde::{Deserialize, Serialize};

pub use self::collect::CollectOp;
pub use self::fan_out::FanOutOp;
pub use self::flat_map::FlatMapOp;
pub use self::race::RaceOp;
pub use self::reduce::ReduceOp;
pub use self::sink::SinkOp;
pub use self::tap::TapOp;
pub use self::then::ThenOp;
pub use self::wait::WaitOp;

/// Discriminated union of all pipeline operations.
///
/// Serialised with a `"op_type"` tag so each variant can be
/// deserialised from JSON without external context.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op_type", rename_all = "snake_case")]
pub enum StepOp {
    /// Sequential step.
    Then(ThenOp),
    /// Fire-and-forget side task (result unchanged).
    Tap(TapOp),
    /// Structural scatter.
    FanOut(FanOutOp),
    /// Nested fan-out expansion.
    FlatMap(FlatMapOp),
    /// Barrier-gather terminator.
    Collect(CollectOp),
    /// Barrier-discard terminator.
    Wait(WaitOp),
    /// First-wins terminator.
    Race(RaceOp),
    /// Sequential fold terminator.
    Reduce(ReduceOp),
    /// Fire-and-forget terminator.
    Sink(SinkOp),
}

impl StepOp {
    /// The pre-generated task identifier for this operation.
    pub fn task_id(&self) -> &str {
        match self {
            Self::Then(op) => &op.task_id,
            Self::Tap(op) => &op.task_id,
            Self::FanOut(op) => &op.task_id,
            Self::FlatMap(op) => &op.task_id,
            Self::Collect(op) => &op.task_id,
            Self::Wait(op) => &op.task_id,
            Self::Race(op) => &op.task_id,
            Self::Reduce(op) => &op.task_id,
            Self::Sink(op) => &op.task_id,
        }
    }

    /// Extra labels attached to this operation.
    pub fn extra_labels(&self) -> &std::collections::HashMap<String, String> {
        match self {
            Self::Then(op) => &op.extra_labels,
            Self::Tap(op) => &op.extra_labels,
            Self::FanOut(op) => &op.extra_labels,
            Self::FlatMap(op) => &op.extra_labels,
            Self::Collect(op) => &op.extra_labels,
            Self::Wait(op) => &op.extra_labels,
            Self::Race(op) => &op.extra_labels,
            Self::Reduce(op) => &op.extra_labels,
            Self::Sink(op) => &op.extra_labels,
        }
    }
}

/// Serialise a slice of operations to a JSON string.
///
/// # Errors
///
/// Returns a [`serde_json::Error`] if serialisation fails.
pub fn serialize_ops(ops: &[StepOp]) -> Result<String, serde_json::Error> {
    serde_json::to_string(ops)
}

/// Deserialise a JSON string into a list of operations.
///
/// # Errors
///
/// Returns a [`serde_json::Error`] if the input is invalid.
pub fn deserialize_ops(raw: &str) -> Result<Vec<StepOp>, serde_json::Error> {
    serde_json::from_str(raw)
}
