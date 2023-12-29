//! Module providing abstractions for modelling an ordered, persistent sequence of records.
//!
//! This module forms the basis for storage in our message queue. It provides the different storage
//! components for representing, storing, indexing and retreiving our records (messages).

use super::{AsyncConsume, AsyncIndexedRead, AsyncTruncate, Sizable};

/// The unit of storage in our [`CommitLog`].
pub struct Record<M, T> {
    pub metadata: M,
    pub value: T,
}

/// An abstract, append-only, ordered sequence of [`Record`] instances.
///
/// This trait acts as a generalized storage mechanism for storing our records. All our
/// message-queue server APIs can be expressed using this trait.
#[async_trait::async_trait(?Send)]
pub trait CommitLog<M, T>:
    AsyncIndexedRead<Value = Record<M, T>, ReadError = Self::Error>
    + AsyncTruncate<Mark = Self::Idx, TruncError = Self::Error>
    + AsyncConsume<ConsumeError = Self::Error>
    + Sizable
{
    /// Error type associated with [`CommitLog`].
    type Error: std::error::Error;

    /// Appends the given [`Record`] at the end of this [`CommitLog`].
    ///
    /// The [`Record`] may contain a stream of byte slices. Implementations are to exhaustively
    /// read the stream and append the corresponding byte slices as a single record.
    ///
    /// Returns the index at which the [`Record`] was appended.
    async fn append<X, XBuf, XE>(&mut self, record: Record<M, X>) -> Result<Self::Idx, Self::Error>
    where
        X: futures_lite::Stream<Item = Result<XBuf, XE>>,
        X: Unpin + 'async_trait,
        XBuf: std::ops::Deref<Target = [u8]>;

    /// Removes all expired records from this [`CommitLog`].
    ///
    /// Returns the number of expired records.
    async fn remove_expired(
        &mut self,
        _expiry_duration: std::time::Duration,
    ) -> Result<Self::Idx, Self::Error> {
        async { Ok(<Self::Idx as num::Zero>::zero()) }.await
    }
}

pub mod segmented_log;

pub(crate) mod test {
    use super::{super::common::indexed_read_stream, *};
    use futures_lite::StreamExt;
    use std::ops::Deref;

    pub(crate) async fn _test_indexed_read_contains_expected_records<R, M, X, I, Y>(
        indexed_read: &R,
        expected_records: I,
        expected_record_count: usize,
    ) where
        R: AsyncIndexedRead<Value = Record<M, X>>,
        X: Deref<Target = [u8]>,
        I: Iterator<Item = Y>,
        Y: Deref<Target = [u8]>,
    {
        let count = futures_lite::stream::iter(expected_records)
            .zip(indexed_read_stream(indexed_read, ..))
            .map(|(y, record)| {
                assert_eq!(y.deref(), record.value.deref());
                Some(())
            })
            .count()
            .await;

        assert_eq!(count, expected_record_count);
    }
}
