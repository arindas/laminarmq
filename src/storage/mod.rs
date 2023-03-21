use super::common::stream::StreamUnexpectedLength;
use async_trait::async_trait;
use futures_lite::{Stream, StreamExt};
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

    fn is_empty(&self) -> bool {
        self.len() == num::zero()
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

    async fn truncate(&mut self, mark: &Self::Mark) -> Result<(), Self::TruncError>;
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
    + Sizable<Size = Self::Position>
{
    type Content: Deref<Target = [u8]>;

    type Position: Unsigned + FromPrimitive + ToPrimitive + Sum + Ord + Copy;

    type Error: std::error::Error + From<StreamUnexpectedLength>;

    async fn append_slice(
        &mut self,
        slice: &[u8],
    ) -> Result<(Self::Position, Self::Size), Self::Error>;

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
            match match (buf, append_threshold) {
                (_, Some(write_capacity)) if bytes_written > write_capacity => {
                    Err(StreamUnexpectedLength.into())
                }
                (Ok(buf), _) => self.append_slice(buf.deref()).await,
                (Err(_), _) => Err(StreamUnexpectedLength.into()),
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

    async fn read(
        &self,
        position: &Self::Position,
        size: &Self::Size,
    ) -> Result<Self::Content, Self::Error>;

    fn is_persistent() -> bool;
}

pub mod commit_log;
pub mod common;
pub mod impls;
