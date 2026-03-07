//! Well-known label keys carried in task message metadata.

/// Serialised pipeline definition (JSON).
pub const PIPELINE: &str = "_pipeline";

/// Current step index within the pipeline.
pub const PIPELINE_STEP: &str = "_pipeline_step";

/// Run identifier shared by all tasks in a single pipeline invocation.
pub const RUN_ID: &str = "_run_id";

/// Number of retry attempts remaining for the current step.
pub const RETRY_REMAINING: &str = "_retry_remaining";
