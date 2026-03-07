//! Task definition trait for compile-time type capture.

use serde::Serialize;
use serde::de::DeserializeOwned;

/// A distributed task with known input and output types.
///
/// Implementations are marker structs that capture the type signature
/// and name of a remote task. The actual handler function lives in
/// the worker binary. This trait only provides enough information for
/// the pipeline builder to enforce type safety at compile time.
///
/// # Example
///
/// ```
/// use orchis_core::Task;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Target {
///     url: String,
/// }
///
/// #[derive(Serialize, Deserialize)]
/// struct ScanResult {
///     findings: Vec<String>,
/// }
///
/// struct ScanTargetTask;
///
/// impl Task for ScanTargetTask {
///     type Input = Target;
///     type Output = ScanResult;
///     const NAME: &'static str = "scanner::scan_target";
/// }
/// ```
pub trait Task: Send + Sync + 'static {
    /// The input parameter type.
    type Input: Serialize + DeserializeOwned + Send + 'static;

    /// The return type.
    type Output: Serialize + DeserializeOwned + Send + 'static;

    /// Fully-qualified task name used for dispatch and routing.
    const NAME: &'static str;
}
