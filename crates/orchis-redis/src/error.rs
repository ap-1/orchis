//! Error conversion from fred errors to orchis error types.

use orchis_core::error::{BarrierError, BrokerError};

/// Convert a fred error into a [`BrokerError`].
pub(crate) fn to_broker_error(err: fred::error::Error) -> BrokerError {
    BrokerError::Connection(err.to_string())
}

/// Convert a fred error into a [`BarrierError`].
pub(crate) fn to_barrier_error(err: fred::error::Error) -> BarrierError {
    BarrierError::Connection(err.to_string())
}
