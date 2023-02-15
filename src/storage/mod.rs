use async_trait::async_trait;
use bytes::Buf;
use futures_core::{Future, Stream};
use futures_lite::AsyncWrite;
use num::{cast::FromPrimitive, CheckedSub, ToPrimitive, Unsigned};
use std::{iter::Sum, ops::Deref};

#[async_trait(?Send)]
pub trait AsyncIndexedRead {
    /// Error type to be used by this trait.
    type ReadError: std::error::Error;

    /// Value to be read.
    type Value;

    /// Type to index with.
    type Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy;

    async fn read(&self, idx: &Self::Idx) -> Result<Self::Value, Self::ReadError>;

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
}

#[async_trait(?Send)]
pub trait AsyncTruncate {
    type TruncError: std::error::Error;

    type Mark: Unsigned;

    async fn truncate(&mut self, pos: &Self::Mark) -> Result<(), Self::TruncError>;
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

/// A storage media. This abstraction supports random reads and linear writes.
#[async_trait(?Send)]
pub trait Storage:
    AsyncTruncate<Mark = Self::Position, TruncError = Self::Error>
    + AsyncConsume<ConsumeError = Self::Error>
    + Sizable
{
    type Content: Deref<Target = [u8]>;

    type Write: AsyncWrite + Unpin;

    type Position: Unsigned;

    type Error: std::error::Error;

    /// Appends a stream of byte buffers at the end of the storage. Using streams enable us to
    /// append data when all of the data is not available all at once. (e.g. when parsing a
    /// HTTP request body).
    ///
    /// ## Params:
    /// - `byte_stream`: Stream of byte buffers to write.
    /// - `write_fn`: function to use for writing the provided stream to our underlying
    /// [`AsyncWrite`] implementation.
    async fn append<'storage, 'byte_stream, B, S, W, F, T>(
        &'storage mut self,
        byte_stream: &'byte_stream mut S,
        write_fn: &mut W,
    ) -> Result<(Self::Position, T), Self::Error>
    where
        B: Buf,
        S: Stream<Item = B> + Unpin,
        F: Future<Output = std::io::Result<T>>,
        W: FnMut(&'byte_stream mut S, &'storage mut Self::Write) -> F;

    /// Reads `size` number of bytes from the given `position`.
    async fn read(
        &self,
        position: &Self::Position,
        size: &Self::Size,
    ) -> Result<Self::Content, Self::Error>;
}

pub mod commit_log;
pub mod common;
pub mod impls;
