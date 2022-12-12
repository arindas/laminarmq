//! Module providing a "commit log" abstraction.
//!
//! This kind of data structure is primarily useful for storing a series of "events". This module
//! provides the abstractions necessary for manipulating this data structure asynchronously on
//! persistent storage media. The abstractions provided by this module are generic over different
//! async runtimes and storage media. Hence user's will need to choose their specializations and
//! implementations of these generic abstractions as necessary.
//!
//! Also see: [`segmented_log`], [`glommio_impl`].
//!
//! In the context of `laminarmq` this module is intended to provide the storage for individual
//! partitions in a topic.

use std::{ops::Deref, time::Duration};

use async_trait::async_trait;
use futures_core::Stream;

/// Represents a record in a [`CommitLog`].
#[derive(Debug)]
pub struct Record<M, T>
where
    M: serde::Serialize + serde::de::DeserializeOwned,
    T: Deref<Target = [u8]>,
{
    pub metadata: M,
    pub value: T,
}

impl<M, T> Record<M, T>
where
    M: serde::Serialize + serde::de::DeserializeOwned,
    T: Deref<Target = [u8]>,
{
    pub fn new(metadata: M, value: T) -> Self {
        Self { metadata, value }
    }
}

/// An ordered sequential collection of [`Record`] instances.
///
/// A [`Record`] in an [`CommitLog`] is addressed with an unique [`u64`] offset, which denotes it
/// position in the collection of records. The offsets in a [`CommitLog`] are monotonically
/// increasing. A higher offset denotes that the [`Record`] appears later in the [`CommitLog`].
///
/// This is useful for storing a sequence of events or operations, which may be used to restore a
/// system to it's previous or current state.
#[async_trait(?Send)]
pub trait CommitLog<M, T>
where
    M: serde::Serialize + serde::de::DeserializeOwned,
    T: Deref<Target = [u8]>,
{
    /// Error type used by the methods of this trait.
    type Error;

    /// Returns the offset where the next [`Record`] will be written in this [`CommitLog`].
    fn highest_offset(&self) -> u64;

    /// Returns the offset of the first [`Record`] in this [`CommitLog`].
    fn lowest_offset(&self) -> u64;

    /// Returns whether the given offset lies in `[lowest_offset, highest_offset)`
    #[inline]
    fn offset_within_bounds(&self, offset: u64) -> bool {
        offset >= self.lowest_offset() && offset < self.highest_offset()
    }

    /// Appends a new [`Record`] at the end of this [`CommitLog`].
    /// Returns the offset at which the record was written, along with the number of bytes written.
    async fn append(
        &mut self,
        record_bytes: &[u8],
        metadata: M,
    ) -> Result<(u64, usize), Self::Error>;

    /// Reads the [`Record`] at the given offset, along with the offset of the next record from
    /// this [`CommitLog`].
    async fn read(&self, offset: u64) -> Result<(Record<M, T>, u64), Self::Error>;

    /// Remove expired storage used, if any. Default implementation simply returns with [`Ok(())`]
    async fn remove_expired(&mut self, _expiry_duration: Duration) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Truncates this [`CommitLog`] instance by removing all records starting from the given
    /// offset.
    async fn truncate(&mut self, _offset: u64) -> Result<(), Self::Error>;

    /// Removes all underlying storage files associated. Consumes this [`CommitLog`] instance to
    /// prevent further operations on this instance.
    async fn remove(self) -> Result<(), Self::Error>;

    /// Closes the files associated with this [`CommitLog`] instances and syncs them to storage
    /// media. Consumes this [`CommitLog`] instance to  prevent further operations on this
    /// instance.
    async fn close(self) -> Result<(), Self::Error>;
}

pub fn commit_log_record_stream<'log, M, T, CL: CommitLog<M, T>>(
    commit_log: &'log CL,
    from_offset: u64,
    scan_seek_bytes: u64,
) -> impl Stream<Item = Record<M, T>> + 'log
where
    M: serde::Serialize + serde::de::DeserializeOwned + 'log,
    T: Deref<Target = [u8]> + 'log,
{
    async_stream::stream! {
        let mut offset = from_offset;

        while offset < commit_log.highest_offset() {
            if let Ok((record, next_record_offset)) = commit_log.read(offset).await {
                offset = next_record_offset;
                yield record;
            } else {
                offset += scan_seek_bytes;
            }
        }
    }
}

pub mod segmented_log;

#[cfg(target_os = "linux")]
pub mod glommio_impl;

pub mod prelude {
    //! Prelude module for [`commit_log`](super) with common exports for convenience.

    #[cfg(target_os = "linux")]
    pub use super::glommio_impl::prelude::*;

    pub use super::{
        segmented_log::{
            common::store_file_path,
            config::SegmentedLogConfig,
            segment::{config::SegmentConfig, Segment, SegmentError},
            store::Store,
            SegmentCreator, SegmentedLog, SegmentedLogError,
        },
        CommitLog, Record,
    };
}
