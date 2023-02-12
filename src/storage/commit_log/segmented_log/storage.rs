use super::super::super::{AsyncConsume, AsyncTruncate, SizedStorage};
use async_trait::async_trait;
use bytes::Buf;
use futures_core::{Future, Stream};
use futures_lite::AsyncWrite;
use std::ops::Deref;

#[async_trait(?Send)]
pub trait Storage:
    AsyncTruncate<Mark = Self::Position, TruncError = Self::Error>
    + AsyncConsume<ConsumeError = Self::Error>
    + SizedStorage
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
