use std::{error::Error, time::Duration};

use async_trait::async_trait;

#[async_trait(?Send)]
pub trait Partition {
    type Error: Error;

    async fn serve_idempotent(&self, request: Request) -> Result<Response, Self::Error>;

    async fn serve(&mut self, request: Request) -> Result<Response, Self::Error>;

    async fn remove(self) -> Result<(), Self::Error>;
}

#[async_trait(?Send)]
pub trait PartitionCreator<P: Partition> {
    async fn new_partition(&self, partition_id: &PartitionId) -> Result<P, P::Error>;
}

#[derive(Eq, Hash, PartialEq, Debug, Clone)]
pub struct PartitionId {
    pub topic: String,
    pub partition_number: u64,
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

pub mod in_memory;
