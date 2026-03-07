//! Redis-backed barrier backend for fan-out synchronisation.

use fred::interfaces::{KeysInterface, ListInterface, TransactionInterface};
use orchis_core::barrier::BarrierBackend;
use orchis_core::error::BarrierError;

use crate::config::RedisConfig;
use crate::error::to_barrier_error;

/// [`BarrierBackend`] implementation backed by Redis (via fred).
pub struct RedisBarrier {
    client: fred::clients::Client,
    config: RedisConfig,
}

impl RedisBarrier {
    /// Create a new barrier backend from an existing fred client.
    pub fn new(client: fred::clients::Client, config: RedisConfig) -> Self {
        Self { client, config }
    }

    fn completed_key(&self, barrier_id: &str) -> String {
        self.config.barrier_key(barrier_id, "completed")
    }

    fn pending_key(&self, barrier_id: &str) -> String {
        self.config.barrier_key(barrier_id, "pending")
    }

    fn leaves_key(&self, barrier_id: &str) -> String {
        self.config.barrier_key(barrier_id, "leaves")
    }

    fn winner_key(&self, barrier_id: &str) -> String {
        self.config.barrier_key(barrier_id, "winner")
    }
}

impl BarrierBackend for RedisBarrier {
    fn increment_simple(
        &self,
        barrier_id: &str,
    ) -> impl Future<Output = Result<usize, BarrierError>> + Send {
        let key = self.completed_key(barrier_id);
        let ttl = self.config.barrier_ttl_secs;
        let client = self.client.clone();
        async move {
            let val: i64 = client.incr(&key).await.map_err(to_barrier_error)?;
            if val == 1 {
                let _: bool = client
                    .expire(&key, ttl, None)
                    .await
                    .map_err(to_barrier_error)?;
            }
            Ok(val as usize)
        }
    }

    fn init_flat_map(
        &self,
        barrier_id: &str,
        pending: usize,
    ) -> impl Future<Output = Result<(), BarrierError>> + Send {
        let pending_key = self.pending_key(barrier_id);
        let leaves_key = self.leaves_key(barrier_id);
        let ttl = self.config.barrier_ttl_secs;
        let client = self.client.clone();
        async move {
            let trx = client.multi();
            let _: () = trx.del(&leaves_key).await.map_err(to_barrier_error)?;
            let _: () = trx
                .set(
                    &pending_key,
                    pending as i64,
                    Some(fred::types::Expiration::EX(ttl)),
                    None,
                    false,
                )
                .await
                .map_err(to_barrier_error)?;
            let _: ((), ()) = trx.exec(true).await.map_err(to_barrier_error)?;
            Ok(())
        }
    }

    fn expand_flat_map(
        &self,
        barrier_id: &str,
        num_children: usize,
    ) -> impl Future<Output = Result<usize, BarrierError>> + Send {
        let pending_key = self.pending_key(barrier_id);
        let leaves_key = self.leaves_key(barrier_id);
        let delta = num_children as i64 - 1;
        let ttl = self.config.barrier_ttl_secs;
        let client = self.client.clone();
        async move {
            let val: i64 = client
                .incr_by(&pending_key, delta)
                .await
                .map_err(to_barrier_error)?;
            if val < 0 {
                return Err(BarrierError::Inconsistent(format!(
                    "pending counter went negative ({val}) for barrier {pending_key}"
                )));
            }
            let _: bool = client
                .expire(&pending_key, ttl, None)
                .await
                .map_err(to_barrier_error)?;
            let _: bool = client
                .expire(&leaves_key, ttl, None)
                .await
                .map_err(to_barrier_error)?;
            Ok(val as usize)
        }
    }

    fn complete_leaf(
        &self,
        barrier_id: &str,
        task_id: &str,
    ) -> impl Future<Output = Result<usize, BarrierError>> + Send {
        let leaves_key = self.leaves_key(barrier_id);
        let pending_key = self.pending_key(barrier_id);
        let ttl = self.config.barrier_ttl_secs;
        let task_id = task_id.to_owned();
        let client = self.client.clone();
        async move {
            let trx = client.multi();
            let _: () = trx
                .lpush(&leaves_key, &task_id)
                .await
                .map_err(to_barrier_error)?;
            let _: () = trx
                .decr_by(&pending_key, 1)
                .await
                .map_err(to_barrier_error)?;
            let _: () = trx
                .expire(&pending_key, ttl, None)
                .await
                .map_err(to_barrier_error)?;
            let _: () = trx
                .expire(&leaves_key, ttl, None)
                .await
                .map_err(to_barrier_error)?;
            let (_, pending, _, _): (i64, i64, bool, bool) =
                trx.exec(true).await.map_err(to_barrier_error)?;
            if pending < 0 {
                return Err(BarrierError::Inconsistent(format!(
                    "pending counter went negative ({pending}) after complete_leaf"
                )));
            }
            Ok(pending as usize)
        }
    }

    fn get_leaf_ids(
        &self,
        barrier_id: &str,
    ) -> impl Future<Output = Result<Vec<String>, BarrierError>> + Send {
        let key = self.leaves_key(barrier_id);
        let client = self.client.clone();
        async move {
            let ids: Vec<String> = client.lrange(&key, 0, -1).await.map_err(to_barrier_error)?;
            Ok(ids)
        }
    }

    fn try_win_race(
        &self,
        barrier_id: &str,
        winner_task_id: &str,
    ) -> impl Future<Output = Result<bool, BarrierError>> + Send {
        let key = self.winner_key(barrier_id);
        let ttl = self.config.barrier_ttl_secs;
        let winner_task_id = winner_task_id.to_owned();
        let client = self.client.clone();
        async move {
            // SET NX EX returns OK when the key was set, nil otherwise.
            let result: Option<String> = client
                .set(
                    &key,
                    &winner_task_id,
                    Some(fred::types::Expiration::EX(ttl)),
                    Some(fred::types::SetOptions::NX),
                    false,
                )
                .await
                .map_err(to_barrier_error)?;
            Ok(result.is_some())
        }
    }

    fn delete(&self, barrier_id: &str) -> impl Future<Output = Result<(), BarrierError>> + Send {
        let keys = vec![
            self.completed_key(barrier_id),
            self.pending_key(barrier_id),
            self.leaves_key(barrier_id),
            self.winner_key(barrier_id),
        ];
        let client = self.client.clone();
        async move {
            let _: () = client.del(keys).await.map_err(to_barrier_error)?;
            Ok(())
        }
    }
}
