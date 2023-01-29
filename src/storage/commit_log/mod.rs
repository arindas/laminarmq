use std::{cmp, ops::RangeBounds, time::Duration};

use async_trait::async_trait;
use futures_core::Stream;

pub struct Record<M, T> {
    pub metadata: M,
    pub value: T,
}

#[async_trait(?Send)]
pub trait CommitLog<M, X, T> {
    /// Error type to be used by this trait.
    type Error: std::error::Error;

    fn highest_index(&self) -> u32;

    fn lowest_index(&self) -> u32;

    #[inline]
    fn len(&self) -> usize {
        (self.highest_index() - self.lowest_index() + 1) as usize
    }

    async fn append(&mut self, record: &Record<M, X>) -> Result<(u32, usize), Self::Error>;

    async fn read(&self, idx: &u32) -> Result<Record<M, T>, Self::Error>;

    async fn remove_expired(&mut self, _expiry_duration: Duration) -> Result<usize, Self::Error> {
        async { Ok(0) }.await
    }

    /// Truncates this [`CommitLog`] instance by removing all records starting from the given
    /// offset. The offset is expected to be at the start or end of a record.
    ///
    /// ### Errors:
    /// Possible errors arising in implementations could include:
    /// - Invalid truncateion offset (offset out of bounds or in the middle of a record)
    /// - Errors from underlying storage media during remooval of backing files.
    async fn truncate(&mut self, _offset: u64) -> Result<(), Self::Error>;

    /// Removes all underlying storage files associated. Consumes this [`CommitLog`] instance to
    /// prevent further operations on this instance.
    ///
    /// ### Errors:
    /// Possible errors arising in implementations could include:
    /// - Errors from underlying storage media during removal of backing files.
    async fn remove(self) -> Result<(), Self::Error>;

    /// Closes the files associated with this [`CommitLog`] instances and syncs them to storage
    /// media. Consumes this [`CommitLog`] instance to  prevent further operations on this
    /// instance.
    ///
    /// ### Errors:
    /// Possible errors arising in implementations could include:
    /// - Errors from underlying storage media when syncing and closing files.
    async fn close(self) -> Result<(), Self::Error>;
}

pub fn commit_log_record_stream<'log, M, X, T, CL, RB>(
    commit_log: &'log CL,
    index_bounds: RB,
) -> impl Stream<Item = Record<M, T>> + 'log
where
    M: 'log,
    T: 'log,
    CL: CommitLog<M, X, T>,
    RB: RangeBounds<u32>,
{
    let (lo_min, hi_max) = (commit_log.lowest_index(), commit_log.highest_index());

    let lo = match index_bounds.start_bound() {
        std::ops::Bound::Included(x) => *x,
        std::ops::Bound::Excluded(x) => *x + 1,
        std::ops::Bound::Unbounded => lo_min,
    };

    let hi = match index_bounds.end_bound() {
        std::ops::Bound::Included(x) => *x,
        std::ops::Bound::Excluded(x) => *x - 1,
        std::ops::Bound::Unbounded => hi_max,
    };

    let (lo, hi) = (cmp::max(lo, lo_min), cmp::min(hi, hi_max));

    async_stream::stream! {
        for index in lo..=hi {
            if let Ok(record) = commit_log.read(&index).await {
                yield record;
            }
        }
    }
}

pub mod segmented_log;
