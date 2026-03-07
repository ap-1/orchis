//! Redis stream consumer that executes tasks and advances pipelines.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use fred::interfaces::StreamsInterface;
use fred::types::streams::XReadResponse;
use orchis_core::broker::Broker;
use orchis_core::error::PipelineError;
use orchis_core::middleware::PipelineMiddleware;
use orchis_core::result::TaskResult;
use orchis_core::task::Task;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::barrier::RedisBarrier;
use crate::broker::RedisBroker;
use crate::error::to_broker_error;
use crate::handler::{BoxedHandler, wrap_handler};

/// Configuration for a [`RedisWorker`].
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Consumer group name.
    ///
    /// Defaults to `"orchis"`.
    pub consumer_group: String,

    /// Unique consumer name within the group.
    ///
    /// Defaults to a random UUID.
    pub consumer_name: String,

    /// Milliseconds to block on XREADGROUP when no messages are available.
    ///
    /// Defaults to 2000.
    pub block_ms: u64,

    /// Maximum messages to fetch per XREADGROUP call.
    ///
    /// Defaults to 100.
    pub count: usize,

    /// Maximum number of tasks processed concurrently.
    ///
    /// Defaults to 10.
    pub max_concurrency: usize,

    /// How often to claim stale messages from other consumers, in
    /// milliseconds.
    ///
    /// Defaults to 30000 (30 seconds).
    pub claim_interval_ms: u64,

    /// Minimum idle time in milliseconds before a pending message can
    /// be claimed from another consumer.
    ///
    /// Defaults to 600000 (10 minutes).
    pub claim_idle_ms: u64,

    /// Maximum number of stale messages to claim per cycle.
    ///
    /// Defaults to 100.
    pub claim_batch_size: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            consumer_group: "orchis".to_owned(),
            consumer_name: uuid::Uuid::new_v4().to_string(),
            block_ms: 2000,
            count: 100,
            max_concurrency: 10,
            claim_interval_ms: 30_000,
            claim_idle_ms: 600_000,
            claim_batch_size: 100,
        }
    }
}

/// Shared state for concurrent message processing.
struct WorkerInner {
    broker: Arc<RedisBroker>,
    middleware: PipelineMiddleware<RedisBroker, RedisBarrier>,
    handlers: HashMap<String, BoxedHandler>,
}

/// A worker that consumes tasks from a Redis stream, executes
/// registered handlers, stores results, and advances pipelines.
///
/// Messages are processed concurrently up to
/// [`max_concurrency`](WorkerConfig::max_concurrency). Stale messages
/// from crashed consumers are periodically claimed via XAUTOCLAIM.
pub struct RedisWorker {
    inner: Arc<WorkerInner>,
    config: WorkerConfig,
}

impl RedisWorker {
    /// Create a new worker.
    ///
    /// # Arguments
    ///
    /// * `broker`: Shared broker for result storage and dispatch.
    /// * `barrier`: Barrier backend for fan-out coordination.
    /// * `config`: Worker configuration.
    pub fn new(broker: Arc<RedisBroker>, barrier: Arc<RedisBarrier>, config: WorkerConfig) -> Self {
        let middleware =
            PipelineMiddleware::new(Arc::clone(&broker) as Arc<RedisBroker>, Some(barrier));
        Self {
            inner: Arc::new(WorkerInner {
                broker,
                middleware,
                handlers: HashMap::new(),
            }),
            config,
        }
    }

    /// Register a typed handler for a task.
    ///
    /// The task name comes from `T::NAME`. The closure receives
    /// the deserialized `T::Input` and returns `T::Output`.
    /// Serialization is handled internally.
    ///
    /// Must be called before [`run`](Self::run). Panics if the inner
    /// state is already shared (i.e. `run` was called).
    pub fn register<T, F, Fut>(&mut self, handler: F)
    where
        T: Task,
        F: Fn(T::Input) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = T::Output> + Send + 'static,
    {
        Arc::get_mut(&mut self.inner)
            .expect("register must be called before run")
            .handlers
            .insert(T::NAME.to_owned(), wrap_handler::<T, F, Fut>(handler));
    }

    /// Run the worker loop until the cancellation token is triggered.
    ///
    /// Creates the consumer group if it doesn't exist, then
    /// continuously reads from the stream and processes messages
    /// concurrently. A background task periodically claims stale
    /// messages from other consumers.
    pub async fn run(&self, shutdown: CancellationToken) -> Result<(), PipelineError> {
        let client = self.inner.broker.client();
        let queue = self.inner.broker.config().queue_name.clone();

        self.ensure_consumer_group(&client, &queue).await?;

        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrency));

        let claim_shutdown = shutdown.clone();
        let claim_inner = Arc::clone(&self.inner);
        let claim_client = client.clone();
        let claim_queue = queue.clone();
        let claim_config = self.config.clone();
        let claim_semaphore = Arc::clone(&semaphore);
        let claim_task = tokio::spawn(async move {
            Self::claim_loop(
                claim_inner,
                claim_client,
                &claim_queue,
                &claim_config,
                claim_semaphore,
                claim_shutdown,
            )
            .await;
        });

        let result = self.read_loop(&client, &queue, &semaphore, &shutdown).await;

        shutdown.cancel();
        let _ = claim_task.await;
        result
    }

    /// Main read loop: XREADGROUP + spawn processing tasks.
    async fn read_loop(
        &self,
        client: &fred::clients::Client,
        queue: &str,
        semaphore: &Arc<Semaphore>,
        shutdown: &CancellationToken,
    ) -> Result<(), PipelineError> {
        loop {
            let messages = tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("worker shutting down");
                    return Ok(());
                }
                result = self.read_messages(client, queue) => result,
            };

            let messages = match messages {
                Ok(msgs) => msgs,
                Err(e) => {
                    tracing::error!("XREADGROUP failed: {e}, retrying in 1s");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            for (stream_id, fields) in messages {
                let permit = Arc::clone(semaphore).acquire_owned().await;
                let Ok(permit) = permit else {
                    return Ok(());
                };

                let inner = Arc::clone(&self.inner);
                let client = client.clone();
                let queue = queue.to_owned();
                let group = self.config.consumer_group.clone();
                tokio::spawn(async move {
                    Self::process_message(&inner, &client, &queue, &group, &stream_id, fields)
                        .await;
                    drop(permit);
                });
            }
        }
    }

    /// Background loop: periodically claim stale messages.
    async fn claim_loop(
        inner: Arc<WorkerInner>,
        client: fred::clients::Client,
        queue: &str,
        config: &WorkerConfig,
        semaphore: Arc<Semaphore>,
        shutdown: CancellationToken,
    ) {
        let interval = Duration::from_millis(config.claim_interval_ms);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(interval) => {}
            }

            let claimed = Self::claim_stale_messages(&client, queue, config).await;
            let messages = match claimed {
                Ok(msgs) => msgs,
                Err(e) => {
                    tracing::error!("XAUTOCLAIM failed: {e}");
                    continue;
                }
            };

            for (stream_id, fields) in messages {
                let permit = Arc::clone(&semaphore).acquire_owned().await;
                let Ok(permit) = permit else {
                    return;
                };

                let inner = Arc::clone(&inner);
                let client = client.clone();
                let queue = queue.to_owned();
                let group = config.consumer_group.clone();
                tokio::spawn(async move {
                    Self::process_message(&inner, &client, &queue, &group, &stream_id, fields)
                        .await;
                    drop(permit);
                });
            }
        }
    }

    /// Claim stale messages from other consumers via XAUTOCLAIM.
    async fn claim_stale_messages(
        client: &fred::clients::Client,
        queue: &str,
        config: &WorkerConfig,
    ) -> Result<Vec<(String, HashMap<String, String>)>, PipelineError> {
        type ClaimResponse = (String, Vec<(String, HashMap<String, String>)>, Vec<String>);
        let response: ClaimResponse = client
            .xautoclaim(
                queue,
                &config.consumer_group,
                &config.consumer_name,
                config.claim_idle_ms,
                "0-0",
                Some(config.claim_batch_size as u64),
                false,
            )
            .await
            .map_err(|e| PipelineError::Broker(to_broker_error(e)))?;

        Ok(response.1)
    }

    /// Create the consumer group, ignoring "already exists" errors.
    async fn ensure_consumer_group(
        &self,
        client: &fred::clients::Client,
        queue: &str,
    ) -> Result<(), PipelineError> {
        let result: Result<(), fred::error::Error> = client
            .xgroup_create(queue, &self.config.consumer_group, "0", true)
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(e) if e.to_string().contains("BUSYGROUP") => Ok(()),
            Err(e) => Err(PipelineError::Broker(to_broker_error(e))),
        }
    }

    /// Read a batch of messages from the stream.
    async fn read_messages(
        &self,
        client: &fred::clients::Client,
        queue: &str,
    ) -> Result<Vec<(String, HashMap<String, String>)>, PipelineError> {
        let response: XReadResponse<String, String, String, String> = client
            .xreadgroup_map(
                &self.config.consumer_group,
                &self.config.consumer_name,
                Some(self.config.count as u64),
                Some(self.config.block_ms),
                false,
                queue,
                ">",
            )
            .await
            .map_err(|e| PipelineError::Broker(to_broker_error(e)))?;

        let mut result = Vec::new();
        for (_stream_key, entries) in response {
            for (stream_id, fields) in entries {
                result.push((stream_id, fields));
            }
        }
        Ok(result)
    }

    /// ACK a message to remove it from the pending entries list.
    async fn ack(client: &fred::clients::Client, queue: &str, group: &str, stream_id: &str) {
        let result: Result<(), fred::error::Error> = client.xack(queue, group, stream_id).await;
        if let Err(e) = result {
            tracing::error!(stream_id, "XACK failed: {e}");
        }
    }

    /// Process a single stream message.
    async fn process_message(
        inner: &WorkerInner,
        client: &fred::clients::Client,
        queue: &str,
        group: &str,
        stream_id: &str,
        fields: HashMap<String, String>,
    ) {
        let Some(task_name) = fields.get("task_name") else {
            tracing::error!(
                stream_id,
                "message missing task_name, acking to prevent redelivery"
            );
            Self::ack(client, queue, group, stream_id).await;
            return;
        };
        let Some(task_id) = fields.get("task_id") else {
            tracing::error!(
                stream_id,
                "message missing task_id, acking to prevent redelivery"
            );
            Self::ack(client, queue, group, stream_id).await;
            return;
        };
        let Some(payload_str) = fields.get("payload") else {
            tracing::error!(
                stream_id,
                task_name,
                "message missing payload, acking to prevent redelivery"
            );
            Self::ack(client, queue, group, stream_id).await;
            return;
        };

        let labels: HashMap<String, String> = match fields.get("labels") {
            Some(s) => serde_json::from_str(s).unwrap_or_else(|e| {
                tracing::error!(
                    stream_id,
                    task_id,
                    "malformed labels JSON: {e}, using empty"
                );
                HashMap::new()
            }),
            None => HashMap::new(),
        };

        let Some(handler) = inner.handlers.get(task_name.as_str()) else {
            tracing::warn!(
                task_name,
                task_id,
                "no handler registered, acking to prevent redelivery"
            );
            Self::ack(client, queue, group, stream_id).await;
            return;
        };

        let payload = Bytes::from(payload_str.as_bytes().to_vec());
        let start = Instant::now();

        let result = match handler(payload).await {
            Ok(data) => TaskResult {
                payload: Ok(data),
                execution_time: start.elapsed().as_secs_f64(),
            },
            Err(msg) => TaskResult {
                payload: Err(msg),
                execution_time: start.elapsed().as_secs_f64(),
            },
        };

        if let Err(e) = inner.broker.set_result(task_id, result.clone()).await {
            tracing::error!(task_id, "failed to store result: {e}");
            return;
        }

        if let Err(e) = inner.middleware.post_save(task_id, &labels, &result).await {
            tracing::error!(task_id, "post_save failed: {e}, not acking");
            return;
        }

        Self::ack(client, queue, group, stream_id).await;
    }
}
