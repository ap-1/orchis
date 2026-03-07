//! Well-known label keys carried in task message metadata.

/// Serialised pipeline definition (JSON).
pub const PIPELINE: &str = "_pipeline";

/// Current step index within the pipeline.
pub const PIPELINE_STEP: &str = "_pipeline_step";

/// Run identifier shared by all tasks in a single pipeline invocation.
pub const RUN_ID: &str = "_run_id";

/// Number of retry attempts remaining for the current step.
pub const RETRY_REMAINING: &str = "_retry_remaining";

/// Current fold item index for reduce operations.
pub const REDUCE_INDEX: &str = "_reduce_index";

/// Total number of items to fold in a reduce operation.
pub const REDUCE_TOTAL: &str = "_reduce_total";

/// Redis key where gathered items are stored for reduce operations.
pub const REDUCE_ITEMS_KEY: &str = "_reduce_items_key";

/// Current accumulator value (JSON) for reduce operations.
pub const REDUCE_ACCUMULATOR: &str = "_reduce_accumulator";
