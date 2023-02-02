use async_trait::async_trait;
use futures_core::Stream;
use num::{CheckedSub, ToPrimitive, Unsigned};
use std::{cmp, ops::RangeBounds};

pub mod commit_log;

#[async_trait(?Send)]
pub trait AsyncIndexedRead {
    /// Error type to be used by this trait.
    type ReadError: std::error::Error;

    /// Value to be read.
    type Value;

    /// Type to index with.
    type Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy;

    fn highest_index(&self) -> &Self::Idx;

    fn lowest_index(&self) -> &Self::Idx;

    fn normalize_index(&self, idx: &Self::Idx) -> Option<Self::Idx> {
        if idx <= &self.highest_index() {
            idx.checked_sub(self.lowest_index())
        } else {
            None
        }
    }

    async fn read(&self, idx: &Self::Idx) -> Result<Self::Value, Self::ReadError>;
}

pub async fn indexed_read_stream<'a, R, RB>(
    indexed_read: &'a R,
    index_bounds: RB,
) -> impl Stream<Item = R::Value> + 'a
where
    RB: RangeBounds<R::Idx>,
    R: AsyncIndexedRead,
    R::Value: 'a,
{
    let (lo_min, hi_max) = (*indexed_read.lowest_index(), *indexed_read.highest_index());

    let lo = match index_bounds.start_bound() {
        std::ops::Bound::Included(x) => *x,
        std::ops::Bound::Excluded(x) => *x + <R::Idx as num::One>::one(),
        std::ops::Bound::Unbounded => lo_min,
    };

    let hi = match index_bounds.end_bound() {
        std::ops::Bound::Included(x) => *x,
        std::ops::Bound::Excluded(x) => *x - <R::Idx as num::One>::one(),
        std::ops::Bound::Unbounded => hi_max,
    };

    let (lo, hi) = (cmp::max(lo, lo_min), cmp::min(hi, hi_max));

    async_stream::stream! {
        for index in num::range_inclusive(lo, hi) {
            if let Ok(record) = indexed_read.read(&index).await {
                yield record;
            }
        }
    }
}

#[async_trait(?Send)]
pub trait AsyncTruncate {
    type TruncError: std::error::Error;

    type Mark: Unsigned;

    async fn truncate(&mut self, idx: &Self::Mark) -> Result<(), Self::TruncError>;
}

#[async_trait(?Send)]
pub trait AsyncConsume {
    type ConsumeError: std::error::Error;

    async fn remove(self) -> Result<(), Self::ConsumeError>;

    async fn close(self) -> Result<(), Self::ConsumeError>;
}

pub trait SizedStorage {
    fn size(&self) -> usize;
}

pub mod impls;
