use super::{
    super::super::common::{borrow::BytesCow, cache::Cache},
    base, Partition,
};

pub enum CachedPartitionError<P, C>
where
    P: Partition,
    C: Cache<u64, (BytesCow<'static>, u64)>,
{
    PartitionError(P::Error),
    CacheError(C::Error),
}

pub struct CachedPartition<P, C>
where
    C: Cache<u64, (BytesCow<'static>, u64)>,
    P: Partition,
    P::Request: Into<base::Request>,
    P::Response: Into<base::Response>,
{
    _cache: C,
    _partition: P,
}
