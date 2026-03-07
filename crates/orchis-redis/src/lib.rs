//! Redis backend for orchis, implementing [`Broker`], [`BarrierBackend`],
//! and a stream-based task worker.
//!
//! # Usage
//!
//! ```ignore
//! use orchis_redis::{RedisBroker, RedisBarrier, RedisConfig, RedisWorkerBuilder, WorkerConfig};
//!
//! let config = RedisConfig::new("redis://127.0.0.1:6379");
//! let broker = Arc::new(RedisBroker::connect(config.clone()).await?);
//! let barrier = Arc::new(RedisBarrier::new(broker.client(), config));
//!
//! let mut builder = RedisWorkerBuilder::new(broker, barrier, WorkerConfig::default());
//! builder.register::<MyTask>(|input| async move { process(input) });
//! let worker = builder.build();
//! worker.run(shutdown_token).await?;
//! ```

pub mod barrier;
pub mod broker;
pub mod config;
mod error;
pub mod handler;
pub mod worker;

pub use barrier::RedisBarrier;
pub use broker::RedisBroker;
pub use config::RedisConfig;
pub use worker::{RedisWorker, RedisWorkerBuilder, WorkerConfig};
