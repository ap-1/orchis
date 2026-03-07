//! Integration tests for orchis-redis.
//!
//! These require a running Redis instance at `ORCHIS_TEST_REDIS_URL`
//! (defaults to `redis://127.0.0.1:6379`).
//!
//! Run with:
//!   ORCHIS_TEST_REDIS=1 cargo test -p orchis-redis --test redis_integration

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use orchis_core::barrier::BarrierBackend;
use orchis_core::broker::Broker;
use orchis_core::result::TaskResult;
use orchis_core::task::Task;
use orchis_redis::{RedisBarrier, RedisBroker, RedisConfig, RedisWorkerBuilder, WorkerConfig};
use serde::{Deserialize, Serialize};

fn should_run() -> bool {
    std::env::var("ORCHIS_TEST_REDIS").is_ok()
}

fn test_config() -> RedisConfig {
    let url = std::env::var("ORCHIS_TEST_REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
    let id = uuid::Uuid::new_v4();
    let mut config = RedisConfig::new(url);
    config.key_prefix = format!("orchis_test:{id}");
    config.queue_name = format!("orchis_test:queue:{id}");
    config
}

#[tokio::test]
async fn broker_set_get_result_round_trip() {
    if !should_run() {
        return;
    }

    let config = test_config();
    let broker = RedisBroker::connect(config).await.unwrap();

    let task_id = broker.generate_id();
    let payload = serde_json::to_vec(&42u64).unwrap();
    let result = TaskResult::success(Bytes::from(payload));

    broker.set_result(&task_id, result).await.unwrap();

    let fetched = broker.get_result(&task_id).await.unwrap();
    assert!(fetched.is_some());

    let fetched = fetched.unwrap();
    assert!(fetched.payload.is_ok());
    let value: u64 = fetched.deserialize_payload().unwrap();
    assert_eq!(value, 42);
}

#[tokio::test]
async fn broker_get_result_missing() {
    if !should_run() {
        return;
    }

    let config = test_config();
    let broker = RedisBroker::connect(config).await.unwrap();

    let result = broker.get_result("nonexistent-id").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn broker_wait_result_immediate() {
    if !should_run() {
        return;
    }

    let config = test_config();
    let broker = RedisBroker::connect(config).await.unwrap();

    let task_id = broker.generate_id();
    let payload = serde_json::to_vec(&"hello").unwrap();
    let result = TaskResult::success(Bytes::from(payload));

    broker.set_result(&task_id, result).await.unwrap();

    let fetched = broker
        .wait_result(&task_id, Some(Duration::from_secs(1)))
        .await
        .unwrap();
    assert!(fetched.payload.is_ok());
    let value: String = fetched.deserialize_payload().unwrap();
    assert_eq!(value, "hello");
}

#[tokio::test]
async fn broker_wait_result_with_notification() {
    if !should_run() {
        return;
    }

    let config = test_config();
    let broker = Arc::new(RedisBroker::connect(config).await.unwrap());

    let task_id = broker.generate_id();
    let task_id_clone = task_id.clone();
    let broker_clone = Arc::clone(&broker);

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let payload = serde_json::to_vec(&vec![1, 2, 3]).unwrap();
        let result = TaskResult::success(Bytes::from(payload));
        broker_clone
            .set_result(&task_id_clone, result)
            .await
            .unwrap();
    });

    let fetched = broker
        .wait_result(&task_id, Some(Duration::from_secs(5)))
        .await
        .unwrap();
    let value: Vec<i32> = fetched.deserialize_payload().unwrap();
    assert_eq!(value, vec![1, 2, 3]);
}

#[tokio::test]
async fn broker_wait_result_timeout() {
    if !should_run() {
        return;
    }

    let config = test_config();
    let broker = RedisBroker::connect(config).await.unwrap();

    let result = broker
        .wait_result("never-set", Some(Duration::from_millis(200)))
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn broker_dispatch_writes_to_stream() {
    if !should_run() {
        return;
    }

    let config = test_config();
    let broker = RedisBroker::connect(config.clone()).await.unwrap();

    let mut labels = HashMap::new();
    labels.insert("queue_name".to_owned(), config.queue_name.clone());

    let payload = serde_json::to_vec(&"test-payload").unwrap();
    broker
        .dispatch("test::task", "tid-1", Bytes::from(payload), labels)
        .await
        .unwrap();

    // Verify something was written to the stream.
    let len: i64 = fred::interfaces::StreamsInterface::xlen(&broker.client(), &config.queue_name)
        .await
        .unwrap();
    assert!(len >= 1);
}

#[tokio::test]
async fn broker_error_result_round_trip() {
    if !should_run() {
        return;
    }

    let config = test_config();
    let broker = RedisBroker::connect(config).await.unwrap();

    let task_id = broker.generate_id();
    let result = TaskResult::error("something went wrong".to_owned());

    broker.set_result(&task_id, result).await.unwrap();

    let fetched = broker.get_result(&task_id).await.unwrap().unwrap();
    assert!(fetched.payload.is_err());
    assert_eq!(fetched.payload.unwrap_err(), "something went wrong");
}

async fn test_barrier(config: RedisConfig) -> RedisBarrier {
    let fred_config = fred::prelude::Config::from_url(&config.url).unwrap();
    let client = fred::clients::Client::new(fred_config, None, None, None);
    fred::interfaces::ClientLike::init(&client).await.unwrap();
    RedisBarrier::new(client, config)
}

#[tokio::test]
async fn barrier_simple_increment() {
    if !should_run() {
        return;
    }

    let config = test_config();
    let barrier = test_barrier(config).await;
    let id = uuid::Uuid::new_v4().to_string();

    assert_eq!(barrier.increment_simple(&id).await.unwrap(), 1);
    assert_eq!(barrier.increment_simple(&id).await.unwrap(), 2);
    assert_eq!(barrier.increment_simple(&id).await.unwrap(), 3);

    barrier.delete(&id).await.unwrap();
}

#[tokio::test]
async fn barrier_flat_map_lifecycle() {
    if !should_run() {
        return;
    }

    let config = test_config();
    let barrier = test_barrier(config).await;
    let id = uuid::Uuid::new_v4().to_string();

    barrier.init_flat_map(&id, 2).await.unwrap();

    let pending = barrier.expand_flat_map(&id, 3).await.unwrap();
    assert_eq!(pending, 4);

    assert_eq!(barrier.complete_leaf(&id, "leaf-a").await.unwrap(), 3);
    assert_eq!(barrier.complete_leaf(&id, "leaf-b").await.unwrap(), 2);
    assert_eq!(barrier.complete_leaf(&id, "leaf-c").await.unwrap(), 1);

    let pending = barrier.expand_flat_map(&id, 1).await.unwrap();
    assert_eq!(pending, 1);

    assert_eq!(barrier.complete_leaf(&id, "leaf-d").await.unwrap(), 0);

    let leaves = barrier.get_leaf_ids(&id).await.unwrap();
    assert_eq!(leaves.len(), 4);
    assert!(leaves.contains(&"leaf-a".to_owned()));
    assert!(leaves.contains(&"leaf-d".to_owned()));

    barrier.delete(&id).await.unwrap();
}

#[tokio::test]
async fn barrier_delete_cleans_all_keys() {
    if !should_run() {
        return;
    }

    let config = test_config();
    let barrier = test_barrier(config.clone()).await;
    let id = uuid::Uuid::new_v4().to_string();

    barrier.increment_simple(&id).await.unwrap();
    barrier.init_flat_map(&id, 1).await.unwrap();
    barrier.complete_leaf(&id, "x").await.unwrap();

    barrier.delete(&id).await.unwrap();

    // Verify keys are gone.
    let broker = RedisBroker::connect(config.clone()).await.unwrap();
    let key = format!("{}:barrier:{}:completed", config.key_prefix, id);
    let exists: bool = fred::interfaces::KeysInterface::exists(&broker.client(), &key)
        .await
        .unwrap();
    assert!(!exists);
}

#[derive(Serialize, Deserialize)]
struct DoubleInput(u64);

#[derive(Serialize, Deserialize)]
struct DoubleOutput(u64);

struct DoubleTask;
impl Task for DoubleTask {
    type Input = DoubleInput;
    type Output = DoubleOutput;
    const NAME: &'static str = "test::double";
}

#[tokio::test]
async fn worker_processes_dispatched_task() {
    if !should_run() {
        return;
    }

    let config = test_config();
    let broker = Arc::new(RedisBroker::connect(config.clone()).await.unwrap());
    let barrier = Arc::new(test_barrier(config).await);

    let worker_config = WorkerConfig {
        consumer_group: format!("test-group-{}", uuid::Uuid::new_v4()),
        block_ms: 500,
        ..WorkerConfig::default()
    };

    let mut builder = RedisWorkerBuilder::new(Arc::clone(&broker), barrier, worker_config);
    builder.register::<DoubleTask, _, _>(
        |input: DoubleInput| async move { DoubleOutput(input.0 * 2) },
    );
    let worker = builder.build();

    let task_id = broker.generate_id();
    let payload = serde_json::to_vec(&DoubleInput(21)).unwrap();
    broker
        .dispatch(
            "test::double",
            &task_id,
            Bytes::from(payload),
            HashMap::new(),
        )
        .await
        .unwrap();

    let shutdown = tokio_util::sync::CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let worker_handle = tokio::spawn(async move { worker.run(shutdown_clone).await });

    let result = broker
        .wait_result(&task_id, Some(Duration::from_secs(5)))
        .await
        .unwrap();

    shutdown.cancel();
    let _ = worker_handle.await;

    assert!(result.payload.is_ok());
    let output: DoubleOutput = result.deserialize_payload().unwrap();
    assert_eq!(output.0, 42);
}

#[tokio::test]
async fn barrier_race_first_wins() {
    if !should_run() {
        return;
    }

    let config = test_config();
    let barrier = test_barrier(config).await;
    let id = uuid::Uuid::new_v4().to_string();

    assert!(barrier.try_win_race(&id, "task-a").await.unwrap());
    assert!(!barrier.try_win_race(&id, "task-b").await.unwrap());
    assert!(!barrier.try_win_race(&id, "task-c").await.unwrap());

    barrier.delete(&id).await.unwrap();
}
