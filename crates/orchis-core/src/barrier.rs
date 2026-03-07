//! Barrier backend trait for fan-out synchronisation.

use std::future::Future;

use crate::error::BarrierError;

/// Coordination backend for fan-out barrier synchronisation.
///
/// Provides atomic operations for two barrier protocols:
///
/// **Simple barrier:** fixed child count, counter-based completion.
/// **Flat-map barrier:** dynamic child count with a pending counter
/// that is adjusted as expanders spawn new children.
pub trait BarrierBackend: Send + Sync + 'static {
    /// Record completion of one child in a simple barrier.
    ///
    /// Returns the number of completed children after this increment.
    fn increment_simple(
        &self,
        barrier_id: &str,
    ) -> impl Future<Output = Result<usize, BarrierError>> + Send;

    /// Initialise a flat-map barrier with an initial pending count.
    ///
    /// # Arguments
    ///
    /// * `barrier_id`: Unique identifier for this barrier instance.
    /// * `pending`: Initial number of pending items (root fan-out size).
    fn init_flat_map(
        &self,
        barrier_id: &str,
        pending: usize,
    ) -> impl Future<Output = Result<(), BarrierError>> + Send;

    /// Atomically adjust the pending counter after a flat-map expansion.
    ///
    /// An expander consumes itself (-1) and spawns `num_children` new
    /// items (+N), so the net delta is `num_children - 1`.
    ///
    /// Returns the pending count after adjustment.
    fn expand_flat_map(
        &self,
        barrier_id: &str,
        num_children: usize,
    ) -> impl Future<Output = Result<usize, BarrierError>> + Send;

    /// Atomically record a terminal leaf and decrement the pending counter.
    ///
    /// The leaf's `task_id` is stored so its result can be gathered once
    /// the barrier fires.
    ///
    /// Returns the pending count after decrement.
    fn complete_leaf(
        &self,
        barrier_id: &str,
        task_id: &str,
    ) -> impl Future<Output = Result<usize, BarrierError>> + Send;

    /// Retrieve all terminal leaf task identifiers.
    ///
    /// Called after the pending counter reaches zero.
    fn get_leaf_ids(
        &self,
        barrier_id: &str,
    ) -> impl Future<Output = Result<Vec<String>, BarrierError>> + Send;

    /// Atomically attempt to claim a race barrier.
    ///
    /// Returns `true` if this caller is the first (the winner),
    /// `false` if another task already won.
    fn try_win_race(
        &self,
        barrier_id: &str,
        winner_task_id: &str,
    ) -> impl Future<Output = Result<bool, BarrierError>> + Send;

    /// Remove all state associated with a barrier.
    fn delete(&self, barrier_id: &str) -> impl Future<Output = Result<(), BarrierError>> + Send;
}
