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

use async_trait::async_trait;

/// Represents a record in a [`CommitLog`].
#[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq, Clone)]
pub struct Record<'a> {
    /// Value stored in this record entry. The value itself might be serialized bytes of some other
    /// form of record.
    pub value: std::borrow::Cow<'a, [u8]>,

    /// Offset at which this record is stored in the log.
    pub offset: u64,
}

/// Abstraction for representing all types that can asynchronously linearly scanned for items.
#[async_trait(?Send)]
pub trait Scanner {
    /// The items scanned out by this scanner.
    type Item;

    /// Returns the next item in this scanner asynchronously. A `None` value indicates that no items
    /// are left to be scanned.
    async fn next(&mut self) -> Option<Self::Item>;
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
pub trait CommitLog {
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
    async fn append(&mut self, record_bytes: &[u8]) -> Result<u64, Self::Error>;

    /// Reads the [`Record`] at the given offset, along with the offset of the next record from
    /// this [`CommitLog`].
    async fn read<'record>(&self, offset: u64) -> Result<(Record<'record>, u64), Self::Error>;

    /// Removes all underlying storage files associated. Consumes this [`CommitLog`] instance to
    /// prevent further operations on this instance.
    async fn remove(self) -> Result<(), Self::Error>;

    /// Closes the files associated with this [`CommitLog`] instances and syncs them to storage
    /// media. Consumes this [`CommitLog`] instance to  prevent further operations on this
    /// instance.
    async fn close(self) -> Result<(), Self::Error>;
}

/// [`Scanner`] implementation for a [`CommitLog`] implementation.
pub struct LogScanner<'a, Log: CommitLog> {
    log: &'a Log,
    offset: u64,
    scan_seek_bytes: u64,
}

impl<'a, Log: CommitLog> LogScanner<'a, Log> {
    /// Creates a new [`LogScanner`] from the given [`CommitLog`] implementation immutable reference.
    ///
    /// ## Default parameters values used
    /// - `offset`: `log.lowest_offset()`
    /// - `scan_seek_bytes`: `8`
    pub fn new(log: &'a Log) -> Option<Self> {
        Self::with_offset_and_scan_seek_bytes(log, log.lowest_offset(), 8)
    }

    /// Creates a new [`LogScanner`] scanner instance with the given parameters.
    ///
    /// ## Parameters
    /// - `log`: an immutable reference to a [`CommitLog`] implementation instance.
    /// - `offset`: offset from which to start reading from.
    /// - `scan_seek_bytes`: number of bytes to seek at a time to search for next record, if a read
    /// operation fails in the middle.
    pub fn with_offset_and_scan_seek_bytes(
        log: &'a Log,
        offset: u64,
        scan_seek_bytes: u64,
    ) -> Option<Self> {
        if !log.offset_within_bounds(offset) {
            None
        } else {
            Some(LogScanner {
                log,
                offset,
                scan_seek_bytes,
            })
        }
    }
}

#[async_trait(?Send)]
impl<'a, Log: CommitLog> Scanner for LogScanner<'a, Log> {
    type Item = Record<'a>;

    /// Linearly scans and reads the next record in the [`CommitLog`] instance asynchronously. If
    /// the read operation fails, it searches for the next readable offset by seeking with the
    /// configured `scan_seek_bytes`.
    ///
    /// It stops when the internal offset of this scanner reaches the commit log's highest offset.
    ///
    /// ## Returns
    /// - [`Some(Record)`]: if a record is there to be read.
    /// - None: to indicate that are there are no more records to read.
    async fn next(&mut self) -> Option<Self::Item> {
        while self.offset < self.log.highest_offset() {
            if let Ok((record, next_record_offset)) = self.log.read(self.offset).await {
                self.offset = next_record_offset;
                return Some(record);
            } else {
                self.offset += self.scan_seek_bytes;
            }
        }

        None
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
        CommitLog, LogScanner, Record, Scanner,
    };
}
