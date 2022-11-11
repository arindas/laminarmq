use std::borrow::Cow;

use super::{Request, Response};
use async_trait::async_trait;

#[async_trait(?Send)]
pub trait Partition {
    type Error: std::error::Error;

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
    pub topic: Cow<'static, str>,
    pub partition_number: u64,
}

pub mod in_memory;
