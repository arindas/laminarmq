use async_trait::async_trait;
use bytes::Buf;
use futures_core::{Future, Stream};
use futures_lite::AsyncWrite;
use num::{cast::FromPrimitive, CheckedSub, ToPrimitive, Unsigned};
use std::{
    cmp,
    iter::Sum,
    ops::{Deref, RangeBounds},
};

pub mod commit_log;

#[async_trait(?Send)]
pub trait AsyncIndexedRead {
    /// Error type to be used by this trait.
    type ReadError: std::error::Error;

    /// Value to be read.
    type Value;

    /// Type to index with.
    type Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy;

    /// Index upper exclusive bound
    fn highest_index(&self) -> Self::Idx;

    /// Index lower inclusive bound
    fn lowest_index(&self) -> Self::Idx;

    fn has_index(&self, idx: &Self::Idx) -> bool {
        *idx >= self.lowest_index() && *idx < self.highest_index()
    }

    fn len(&self) -> Self::Idx {
        self.highest_index()
            .checked_sub(&self.lowest_index())
            .unwrap_or(num::zero())
    }

    fn normalize_index(&self, idx: &Self::Idx) -> Option<Self::Idx> {
        self.has_index(idx)
            .then_some(idx)
            .and_then(|idx| idx.checked_sub(&self.lowest_index()))
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
    let (lo_min, hi_max) = (indexed_read.lowest_index(), indexed_read.highest_index());

    let one = <R::Idx as num::One>::one();

    let hi_max = hi_max - one;

    let lo = match index_bounds.start_bound() {
        std::ops::Bound::Included(x) => *x,
        std::ops::Bound::Excluded(x) => *x + one,
        std::ops::Bound::Unbounded => lo_min,
    };

    let hi = match index_bounds.end_bound() {
        std::ops::Bound::Included(x) => *x,
        std::ops::Bound::Excluded(x) => x.checked_sub(&one).unwrap_or(lo),
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

pub trait Sizable {
    type Size: Unsigned + FromPrimitive + Sum + Ord;

    fn size(&self) -> Self::Size;
}

#[async_trait(?Send)]
pub trait Storage:
    AsyncTruncate<Mark = Self::Position, TruncError = Self::Error>
    + AsyncConsume<ConsumeError = Self::Error>
    + Sizable
{
    type Content: Deref<Target = [u8]>;

    type Write: AsyncWrite + Unpin;

    type Position;

    type Error: std::error::Error;

    async fn read(
        &mut self,
        position: &Self::Position,
        size: &Self::Size,
    ) -> Result<Self::Content, Self::Error>;

    async fn append<B, S, W, F, T>(
        &mut self,
        byte_stream: &mut S,
        write_fn: &mut W,
    ) -> Result<(Self::Position, T), Self::Error>
    where
        B: Buf,
        S: Stream<Item = B> + Unpin,
        W: FnMut(&mut S, &mut Self::Write) -> F,
        F: Future<Output = std::io::Result<T>>;
}

pub mod impls;
