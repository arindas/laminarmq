use std::{borrow::Cow, collections::HashMap, time::Duration};

pub mod channel {
    use async_trait::async_trait;
    use std::error::Error;

    pub trait Sender<T> {
        type Error: Error;

        fn try_send(&self, item: T) -> Result<(), Self::Error>;
    }

    #[async_trait(?Send)]
    pub trait Receiver<T> {
        async fn recv(&self) -> Option<T>;
    }
}

pub type Record = crate::commit_log::Record<'static>;

pub enum Request {
    PartitionHierachy,

    RemoveExpired {
        partition: partition::PartitionId,
        expiry_duration: Duration,
    },

    CreatePartition(partition::PartitionId),
    RemovePartition(partition::PartitionId),

    Read {
        partition: partition::PartitionId,
        offset: u64,
    },
    Append {
        partition: partition::PartitionId,
        record_bytes: Cow<'static, [u8]>,
    },

    LowestOffset {
        partition: partition::PartitionId,
    },
    HighestOffset {
        partition: partition::PartitionId,
    },
}

pub enum Response {
    PartitionHierachy(HashMap<Cow<'static, str>, Vec<u64>>),

    Read { record: Record, next_offset: u64 },
    LowestOffset(u64),
    HighestOffset(u64),

    Append { write_offset: u64 },

    ExpiredRemoved,
    PartitionCreated,
    PartitionRemoved,
}

pub mod partition;
pub mod worker;

pub mod tokio_compat;

#[cfg(target_os = "linux")]
pub mod glommio_impl;
