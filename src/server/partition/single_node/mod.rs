use std::{borrow::Cow, time::Duration};

pub enum PartitionRequest {
    RemoveExpired { expiry_duration: Duration },

    Read { offset: u64 },
    Append { record_bytes: Cow<'static, [u8]> },

    LowestOffset,
    HighestOffset,
}

pub mod commit_log;
pub mod in_memory;
