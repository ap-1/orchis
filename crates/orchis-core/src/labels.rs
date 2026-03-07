//! Well-known label keys carried in task message metadata.

/// Serialised pipeline definition (JSON).
pub const PIPELINE: &str = "_pipeline";

/// Current step index within the pipeline.
pub const PIPELINE_STEP: &str = "_pipeline_step";

/// Run identifier shared by all tasks in a single pipeline invocation.
pub const RUN_ID: &str = "_run_id";
