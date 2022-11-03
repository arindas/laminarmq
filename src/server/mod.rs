use std::time::Duration;

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

pub enum Request {
    RemoveExpired { expiry_duration: Duration },
    CreatePartition,
    RemovePartition,

    Read { offset: u64 },
    LowestOffset,
    HighestOffset,

    Append { record_bytes: Vec<u8> },
}

pub type Record = crate::commit_log::Record<'static>;

pub enum Response {
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
