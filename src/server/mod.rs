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

pub mod single_node {
    use crate::commit_log::{segmented_log::RecordMetadata, Record_};

    use super::partition::PartitionId;
    use std::{borrow::Cow, collections::HashMap, ops::Deref, time::Duration};

    pub enum Request<T: Deref<Target = [u8]>> {
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
            record_bytes: T,
        },

        LowestOffset {
            partition: PartitionId,
        },
        HighestOffset {
            partition: PartitionId,
        },
    }

    pub enum Response<T: Deref<Target = [u8]>> {
        PartitionHierachy(HashMap<Cow<'static, str>, Vec<u64>>),

        Read {
            record: Record_<RecordMetadata<()>, T>,
            next_offset: u64,
        },
        LowestOffset(u64),
        HighestOffset(u64),

        Append {
            write_offset: u64,
            bytes_written: usize,
        },

        ExpiredRemoved,
        PartitionCreated,
        PartitionRemoved,
    }

    #[derive(Clone, Copy)]
    pub enum RequestKind {
        Read,
        Append,

        LowestOffset,
        HighestOffset,
        RemoveExpired,

        PartitionHierachy,
        CreatePartition,
        RemovePartition,
    }
}

pub mod partition;
pub mod router;
pub mod worker;

pub mod tokio_compat;

#[cfg(target_os = "linux")]
pub mod glommio_impl;
