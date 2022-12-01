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

pub mod single_node;

pub mod cached {
    use crate::common::{borrow::BytesCow, cache::Cache};

    pub enum PartitionError<C, P>
    where
        C: Cache<u64, (BytesCow<'static>, u64)>,
        P: super::Partition,
    {
        PartitionError(P::Error),
        CacheError(C::Error),
        WrongResponseFromPartition,
    }

    impl<C, P> std::fmt::Display for PartitionError<C, P>
    where
        C: Cache<u64, (BytesCow<'static>, u64)>,
        C::Error: std::error::Error,
        P: super::Partition,
        P::Error: std::error::Error,
    {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                PartitionError::PartitionError(error) => write!(f, "PartitionError: {:?}", error),
                PartitionError::CacheError(error) => write!(f, "CacheError: {:?}", error),
                PartitionError::WrongResponseFromPartition => {
                    write!(f, "Wrong response from partition")
                }
            }
        }
    }

    impl<C, P> std::fmt::Debug for PartitionError<C, P>
    where
        C: Cache<u64, (BytesCow<'static>, u64)>,
        C::Error: std::error::Error,
        P: super::Partition,
        P::Error: std::error::Error,
    {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::PartitionError(error) => {
                    f.debug_tuple("PartitionError").field(error).finish()
                }
                Self::CacheError(error) => f.debug_tuple("CacheError").field(error).finish(),
                PartitionError::WrongResponseFromPartition => {
                    write!(f, "WrongResponseFromPartition")
                }
            }
        }
    }

    impl<C, P> std::error::Error for PartitionError<C, P>
    where
        C: Cache<u64, (BytesCow<'static>, u64)>,
        C::Error: std::error::Error,
        P: super::Partition,
        P::Error: std::error::Error,
    {
    }

    pub struct Partition<C, P>
    where
        C: Cache<u64, (BytesCow<'static>, u64)>,
        P: super::Partition,
    {
        pub cache: C,
        pub partition: P,
    }
}
