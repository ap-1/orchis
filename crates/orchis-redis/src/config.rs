//! Configuration for the Redis broker and barrier backend.

/// Configuration for [`RedisBroker`](crate::RedisBroker) and
/// [`RedisBarrier`](crate::RedisBarrier).
#[derive(Debug, Clone)]
pub struct RedisConfig {
    /// Redis connection URL (e.g. `redis://127.0.0.1:6379`).
    pub url: String,

    /// Stream key used for task dispatch.
    ///
    /// Defaults to `"orchis:queue:default"`.
    pub queue_name: String,

    /// Prefix for all Redis keys (results, barriers, notifications).
    ///
    /// Defaults to `"orchis"`.
    pub key_prefix: String,

    /// TTL in seconds for result and notification keys.
    ///
    /// Defaults to 86400 (1 day).
    pub result_ttl_secs: i64,

    /// TTL in seconds for barrier keys.
    ///
    /// Defaults to 86400 (1 day).
    pub barrier_ttl_secs: i64,
}

impl RedisConfig {
    /// Create a config with the given Redis URL and sensible defaults.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            queue_name: "orchis:queue:default".to_owned(),
            key_prefix: "orchis".to_owned(),
            result_ttl_secs: 86400,
            barrier_ttl_secs: 86400,
        }
    }

    /// Build the result key for a given task ID.
    pub(crate) fn result_key(&self, task_id: &str) -> String {
        format!("{}:result:{}", self.key_prefix, task_id)
    }

    /// Build the notification key for a given task ID.
    pub(crate) fn notify_key(&self, task_id: &str) -> String {
        format!("{}:notify:{}", self.key_prefix, task_id)
    }

    /// Build the barrier key prefix for a given barrier ID.
    pub(crate) fn barrier_key(&self, barrier_id: &str, suffix: &str) -> String {
        format!("{}:barrier:{}:{}", self.key_prefix, barrier_id, suffix)
    }

    /// Build the dispatch stream key, incorporating an optional
    /// queue name override from labels.
    pub(crate) fn queue_key(&self, labels: &std::collections::HashMap<String, String>) -> String {
        labels
            .get("queue_name")
            .cloned()
            .unwrap_or_else(|| self.queue_name.clone())
    }
}
