//! Module providing single-node [`super::Partition`] implementation.

use std::{ops::Deref, time::Duration};

/// Default record expury duration for single-node partition implementations.
pub const DEFAULT_EXPIRY_DURATION: Duration = Duration::from_secs(86400 * 7);

/// Request enumeration for single-node [`super::Partition`] requests.
pub enum PartitionRequest<T: Deref<Target = [u8]>> {
    /// Remove expired records in partition.
    RemoveExpired {
        expiry_duration: Duration,
    },

    Read {
        offset: u64,
    },
    Append {
        record_bytes: T,
    },

    LowestOffset,
    HighestOffset,
}

#[doc(hidden)]
pub mod cached;

pub mod commit_log;
pub mod in_memory;
