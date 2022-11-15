use async_trait::async_trait;

#[async_trait(?Send)]
pub trait Partition {
    type Error: std::error::Error;
    type Request;
    type Response;

    async fn serve_idempotent(&self, request: Self::Request)
        -> Result<Self::Response, Self::Error>;

    async fn serve(&mut self, request: Self::Request) -> Result<Self::Response, Self::Error>;

    async fn close(self) -> Result<(), Self::Error>;

    async fn remove(self) -> Result<(), Self::Error>;
}

#[async_trait(?Send)]
pub trait PartitionCreator<P: Partition> {
    async fn new_partition(&self, partition_id: &PartitionId) -> Result<P, P::Error>;
}

#[derive(Eq, Hash, PartialEq, Debug, Clone)]
pub struct PartitionId {
    pub topic: std::borrow::Cow<'static, str>,
    pub partition_number: u64,
}

pub mod single_node {
    use std::{borrow::Cow, time::Duration};

    pub enum PartitionRequest {
        RemoveExpired { expiry_duration: Duration },

        Read { offset: u64 },
        Append { record_bytes: Cow<'static, [u8]> },

        LowestOffset,
        HighestOffset,
    }
}

pub mod in_memory;
