use std::{borrow::Cow, time::Duration};

pub const DEFAULT_EXPIRY_DURATION: Duration = Duration::from_secs(86400 * 7);

pub enum PartitionRequest {
    RemoveExpired { expiry_duration: Duration },

    Read { offset: u64 },
    Append { record_bytes: Cow<'static, [u8]> },

    LowestOffset,
    HighestOffset,
}

pub mod cached;
pub mod commit_log;
pub mod in_memory;
