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

pub mod single_node {
    use super::{partition::PartitionId, Record};
    use std::{borrow::Cow, collections::HashMap, time::Duration};

    pub enum Request {
        PartitionHierachy,

        RemoveExpired {
            partition: PartitionId,
            expiry_duration: Duration,
        },

        CreatePartition(PartitionId),
        RemovePartition(PartitionId),

        Read {
            partition: PartitionId,
            offset: u64,
        },
        Append {
            partition: PartitionId,
            record_bytes: Cow<'static, [u8]>,
        },

        LowestOffset {
            partition: PartitionId,
        },
        HighestOffset {
            partition: PartitionId,
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
}

pub mod partition;
pub mod router;
pub mod worker;

pub mod tokio_compat;

#[cfg(target_os = "linux")]
pub mod glommio_impl;
