//! Module providing abstractions to store records.

use super::common::stream::StreamUnexpectedLength;
use async_trait::async_trait;
use futures_lite::{Stream, StreamExt};
use num::{cast::FromPrimitive, CheckedSub, ToPrimitive, Unsigned};
use std::{iter::Sum, ops::Deref};

/// Collection providing asynchronous read access to an indexed set of records (or values).
#[async_trait(?Send)]
pub trait AsyncIndexedRead {
    /// Error that can occur during a read operation.
    type ReadError: std::error::Error;

    /// Value to be read.
    type Value;

    /// Type to index with.
    type Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy;

    /// Reads the value at the given index.
    async fn read(&self, idx: &Self::Idx) -> Result<Self::Value, Self::ReadError>;

    /// Index upper exclusive bound
    fn highest_index(&self) -> Self::Idx;

    /// Index lower inclusive bound
    fn lowest_index(&self) -> Self::Idx;

    /// Returns whether the given index is within the index bounds of this collection.
    ///
    /// This method checks the following condition:
    /// ```text
    /// lowest_index <= idx < highest_index
    /// ```
    fn has_index(&self, idx: &Self::Idx) -> bool {
        *idx >= self.lowest_index() && *idx < self.highest_index()
    }

    /// Returns the number of values in this collection.
    fn len(&self) -> Self::Idx {
        self.highest_index()
            .checked_sub(&self.lowest_index())
            .unwrap_or(num::zero())
    }

    /// Returns whether this collection is empty.
    fn is_empty(&self) -> bool {
        self.len() == num::zero()
    }

    /// Normalizes the given index between `[0, len)` by subtracting `lowest_index` from it.
    ///
    /// Returns `Some(normalized_index)` if the index is within bounds, `None` otherwise.
    fn normalize_index(&self, idx: &Self::Idx) -> Option<Self::Idx> {
        self.has_index(idx)
            .then_some(idx)
            .and_then(|idx| idx.checked_sub(&self.lowest_index()))
    }
}

/// Trait representing a truncable collection of records, which can be truncated after a "mark".
#[async_trait(?Send)]
pub trait AsyncTruncate {
    /// Error that can occur during a truncation operation.
    type TruncError: std::error::Error;

    /// Type to denote a truncation "mark", after which the collection will be truncated.
    type Mark: Unsigned;

    /// Truncates this collection after the given mark, such that this collection
    /// contains records only upto this "mark".
    async fn truncate(&mut self, mark: &Self::Mark) -> Result<(), Self::TruncError>;
}

/// Trait representing a collection that can be closed or removed entirely.
#[async_trait(?Send)]
pub trait AsyncConsume {
    /// Error that can occur during a consumption operation.
    type ConsumeError: std::error::Error;

    /// Removes all storage associated with this collection.
    ///
    /// The records in this collection are completely removed.
    async fn remove(self) -> Result<(), Self::ConsumeError>;

    /// Closes this collection.
    ///
    /// One would need to re-qcquire a handle to this collection from the storage
    /// in-order to access the records ot this collection again.
    async fn close(self) -> Result<(), Self::ConsumeError>;
}

/// Tratis representing collections which have a measurable size in number of bytes.
pub trait Sizable {
    /// Type to represent the size of this collection in number of bytes.
    type Size: Unsigned + FromPrimitive + Sum + Ord;

    /// Returns the size of this collection in butes.
    fn size(&self) -> Self::Size;
}

/// Trait representing a read-append-truncate storage media.
#[async_trait(?Send)]
pub trait Storage:
    AsyncTruncate<Mark = Self::Position, TruncError = Self::Error>
    + AsyncConsume<ConsumeError = Self::Error>
    + Sizable<Size = Self::Position>
{
    /// Type to represent the content bytes of this storage media.
    type Content: Deref<Target = [u8]> + Unpin;

    /// Type to represent data positions inside this storage media.
    type Position: Unsigned + FromPrimitive + ToPrimitive + Sum + Ord + Copy;

    /// Error that can occur during storage operations.
    type Error: std::error::Error + From<StreamUnexpectedLength>;

    /// Appends the given slice of bytes to the end of this storage.
    ///
    /// Implementations must update internal cursor or write pointers, if any,
    /// when implementing this method.
    async fn append_slice(
        &mut self,
        slice: &[u8],
    ) -> Result<(Self::Position, Self::Size), Self::Error>;

    /// Appends a stream of byte slices to the end of this storage.
    ///
    /// This method writes at max `append_threshold` number of bytes from the
    /// given stream of bytes slices. If the provided `append_threshold` is
    /// `None`, no such check is enforced; we attempt to write the entire
    /// stream until it's exhausted.
    ///
    /// The following error scenarios may occur during writing:
    /// - `append_threshold` is `Some(_)`, and the stream contains more bytes
    /// than the threshold
    /// - The stream unexpectedly yields an error when attempting to read the
    /// next byte slice from the stream
    /// - There is a storage error when attempting to write one of the byte
    /// slices from the stream.
    ///
    /// In all of the above error cases, we truncate this storage media with
    /// the size of the storage media before we started the append operation,
    /// effectively rolling back any writes.
    ///
    /// Returns the position where the bytes were written and the number of
    /// bytes written.
    async fn append<XBuf, XE, X>(
        &mut self,
        buf_stream: &mut X,
        append_threshold: Option<Self::Size>,
    ) -> Result<(Self::Position, Self::Size), Self::Error>
    where
        XBuf: Deref<Target = [u8]>,
        X: Stream<Item = Result<XBuf, XE>> + Unpin,
    {
        let (mut bytes_written, pos) = (num::zero(), self.size());

        while let Some(buf) = buf_stream.next().await {
            match match match (buf, append_threshold) {
                (Ok(buf), Some(write_capacity)) => {
                    match Self::Size::from_usize(buf.deref().len()) {
                        Some(buf_len) if buf_len + bytes_written > write_capacity => {
                            Err::<XBuf, Self::Error>(StreamUnexpectedLength.into())
                        }
                        Some(_) => Ok(buf),
                        None => Err(StreamUnexpectedLength.into()),
                    }
                }
                (Ok(buf), None) => Ok(buf),
                (Err(_), _) => Err(StreamUnexpectedLength.into()),
            } {
                Ok(buf) => self.append_slice(buf.deref()).await,
                Err(_) => Err(StreamUnexpectedLength.into()),
            } {
                Ok((_, buf_bytes_w)) => {
                    bytes_written = bytes_written + buf_bytes_w;
                }
                Err(error) => {
                    self.truncate(&pos).await?;
                    return Err(error);
                }
            };
        }

        Ok((pos, bytes_written))
    }

    /// Reads `size` number of bytes from the given `position`.
    ///
    /// Returns the bytes read.
    async fn read(
        &self,
        position: &Self::Position,
        size: &Self::Size,
    ) -> Result<Self::Content, Self::Error>;
}

pub mod commit_log;
pub mod common;
pub mod impls;
