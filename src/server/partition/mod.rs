//! Module providing abstractions necessary for immplementing "partition"s in our message queue.

use async_trait::async_trait;

/// Unit of data sharding and replication. A partition provides an abstract RPC service for serving
/// record read/write requests.
#[async_trait(?Send)]
pub trait Partition {
    type Error: std::error::Error;
    type Request;
    type Response;

    /// Serves idempotent requests from a shared partition ref that have no side effects.
    async fn serve_idempotent(&self, request: Self::Request)
        -> Result<Self::Response, Self::Error>;

    /// Exclusively serves requests from a mutable partition ref, that may or may not have side
    /// effects.
    async fn serve(&mut self, request: Self::Request) -> Result<Self::Response, Self::Error>;

    /// Closes all files associated with this partition and syncs all changes to disk. This
    /// method acquires the ownership of this partition to prevent this partition from being
    /// used any further.
    async fn close(self) -> Result<(), Self::Error>;

    /// Removes all storage files associated with this partition. This method acquires the
    /// ownership of this partition to prevent this partition from being used any further.
    async fn remove(self) -> Result<(), Self::Error>;
}

/// Trait to represent a mechanism for creating partitions from [`PartitionId`].
#[async_trait(?Send)]
pub trait PartitionCreator<P: Partition> {
    /// Creates a new partition from the given [`PartitionId`].
    async fn new_partition(&self, partition_id: &PartitionId) -> Result<P, P::Error>;
}

#[derive(Eq, Hash, PartialEq, Debug, Clone)]
pub struct PartitionId {
    /// topic id string    
    pub topic: std::borrow::Cow<'static, str>,

    /// partition number under topic   
    pub partition_number: u64,
}

pub mod single_node;

#[doc(hidden)]
pub mod cached {}
