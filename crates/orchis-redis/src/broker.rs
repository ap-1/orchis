//! Redis-backed broker for task dispatch and result storage.

use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use fred::interfaces::{
    ClientLike, EventInterface, KeysInterface, PubsubInterface, StreamsInterface,
};
use fred::prelude::{Builder, Config, ReconnectPolicy};
use orchis_core::broker::Broker;
use orchis_core::error::BrokerError;
use orchis_core::result::TaskResult;

use crate::config::RedisConfig;
use crate::error::to_broker_error;

/// [`Broker`] implementation backed by Redis (via fred).
///
/// Tasks are dispatched via Redis Streams (`XADD`). Results are stored
/// as plain string keys. `wait_result` uses pub/sub for instant
/// wakeup without polling or blocking commands.
pub struct RedisBroker {
    client: fred::clients::Client,
    subscriber: fred::clients::SubscriberClient,
    config: RedisConfig,
}

impl RedisBroker {
    /// Create a new broker by connecting to Redis.
    ///
    /// # Errors
    ///
    /// Returns a [`BrokerError`] if the connection fails.
    pub async fn connect(config: RedisConfig) -> Result<Self, BrokerError> {
        let fred_config = Config::from_url(&config.url).map_err(to_broker_error)?;

        let client = fred::clients::Client::new(
            fred_config.clone(),
            None,
            None,
            Some(ReconnectPolicy::default()),
        );
        client.init().await.map_err(to_broker_error)?;

        let subscriber = Builder::from_config(fred_config)
            .build_subscriber_client()
            .map_err(to_broker_error)?;
        subscriber.init().await.map_err(to_broker_error)?;
        subscriber.manage_subscriptions();

        Ok(Self {
            client,
            subscriber,
            config,
        })
    }

    /// Get a reference to the config.
    pub fn config(&self) -> &RedisConfig {
        &self.config
    }

    /// Get a clone of the underlying fred client (for sharing with
    /// [`RedisBarrier`](crate::RedisBarrier)).
    pub fn client(&self) -> fred::clients::Client {
        self.client.clone()
    }
}

impl Broker for RedisBroker {
    fn generate_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }

    fn dispatch(
        &self,
        task_name: &str,
        task_id: &str,
        payload: Bytes,
        labels: HashMap<String, String>,
    ) -> impl Future<Output = Result<(), BrokerError>> + Send {
        let queue = self.config.queue_key(&labels);
        let task_name = task_name.to_owned();
        let task_id = task_id.to_owned();
        let client = self.client.clone();
        async move {
            let labels_json = serde_json::to_string(&labels)
                .map_err(|e| BrokerError::Serialization(e.to_string()))?;
            let payload_str = std::str::from_utf8(&payload).map_err(|e| {
                BrokerError::Serialization(format!("payload is not valid UTF-8: {e}"))
            })?;

            let _: String = client
                .xadd(
                    &queue,
                    false,
                    None,
                    "*",
                    vec![
                        ("task_name", task_name.as_str()),
                        ("task_id", task_id.as_str()),
                        ("payload", payload_str),
                        ("labels", labels_json.as_str()),
                    ],
                )
                .await
                .map_err(to_broker_error)?;
            Ok(())
        }
    }

    fn set_result(
        &self,
        task_id: &str,
        result: TaskResult,
    ) -> impl Future<Output = Result<(), BrokerError>> + Send {
        let result_key = self.config.result_key(task_id);
        let notify_channel = self.config.notify_key(task_id);
        let ttl = self.config.result_ttl_secs;
        let client = self.client.clone();
        async move {
            let serialized = serde_json::to_string(&result)
                .map_err(|e| BrokerError::Serialization(e.to_string()))?;

            let _: () = client
                .set(
                    &result_key,
                    &serialized,
                    Some(fred::types::Expiration::EX(ttl)),
                    None,
                    false,
                )
                .await
                .map_err(to_broker_error)?;

            // Notify any waiters via pub/sub.
            let _: i64 = client
                .publish(&notify_channel, "1")
                .await
                .map_err(to_broker_error)?;

            Ok(())
        }
    }

    fn get_result(
        &self,
        task_id: &str,
    ) -> impl Future<Output = Result<Option<TaskResult>, BrokerError>> + Send {
        let key = self.config.result_key(task_id);
        let client = self.client.clone();
        async move {
            let raw: Option<String> = client.get(&key).await.map_err(to_broker_error)?;
            match raw {
                Some(s) => {
                    let result: TaskResult = serde_json::from_str(&s)
                        .map_err(|e| BrokerError::Serialization(e.to_string()))?;
                    Ok(Some(result))
                }
                None => Ok(None),
            }
        }
    }

    fn wait_result(
        &self,
        task_id: &str,
        timeout: Option<Duration>,
    ) -> impl Future<Output = Result<TaskResult, BrokerError>> + Send {
        let result_key = self.config.result_key(task_id);
        let notify_channel = self.config.notify_key(task_id);
        let client = self.client.clone();
        let subscriber = self.subscriber.clone();
        async move {
            // Subscribe BEFORE checking GET to avoid the race where
            // PUBLISH fires between our GET and SUBSCRIBE.
            subscriber
                .subscribe(&notify_channel)
                .await
                .map_err(to_broker_error)?;
            let mut rx = subscriber.message_rx();

            // Fast path: result may already exist.
            if let Some(s) = client
                .get::<Option<String>, _>(&result_key)
                .await
                .map_err(to_broker_error)?
            {
                let _ = subscriber.unsubscribe(&notify_channel).await;
                return serde_json::from_str(&s)
                    .map_err(|e| BrokerError::Serialization(e.to_string()));
            }

            // Wait for a pub/sub message on our specific channel.
            // The subscriber is shared, so we must filter by channel name.
            let wait_fut = async {
                loop {
                    match rx.recv().await {
                        Ok(msg) => {
                            if msg.channel.as_bytes() == notify_channel.as_bytes() {
                                return Ok(());
                            }
                        }
                        Err(_) => {
                            return Err(BrokerError::Connection(
                                "subscriber channel closed".to_owned(),
                            ));
                        }
                    }
                }
            };

            let result = if let Some(dur) = timeout {
                match tokio::time::timeout(dur, wait_fut).await {
                    Ok(inner) => inner,
                    Err(_) => Err(BrokerError::Timeout),
                }
            } else {
                wait_fut.await
            };

            let _ = subscriber.unsubscribe(&notify_channel).await;
            result?;

            // Notification received, fetch the result.
            let raw: Option<String> = client.get(&result_key).await.map_err(to_broker_error)?;
            match raw {
                Some(s) => {
                    serde_json::from_str(&s).map_err(|e| BrokerError::Serialization(e.to_string()))
                }
                None => Err(BrokerError::NotFound(task_id.to_owned())),
            }
        }
    }
}
