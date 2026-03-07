//! Orchis: typed pipeline orchestration for distributed task queues.
//!
//! `orchis-core` provides the broker-agnostic core: traits, operation
//! types, a typestate pipeline builder with compile-time type
//! threading, and middleware that drives execution between steps.
//!
//! Adapter crates (`orchis-redis`, `orchis-apalis`, ...) implement the
//! [`Broker`] and [`BarrierBackend`] traits for concrete backends.

pub mod barrier;
pub mod broker;
pub mod builder;
pub mod context;
pub mod error;
pub mod handle;
pub mod labels;
pub mod middleware;
pub mod ops;
pub mod result;
pub mod task;

pub use barrier::BarrierBackend;
pub use broker::Broker;
pub use builder::Pipeline;
pub use error::{BarrierError, BrokerError, PipelineError};
pub use handle::TaskHandle;
pub use middleware::PipelineMiddleware;
pub use result::TaskResult;
pub use task::Task;
