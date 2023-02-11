use super::super::super::{AsyncConsume, AsyncIndexedRead, AsyncTruncate, SizedStorage};
use async_trait::async_trait;
use bytes::Buf;
use futures_core::{Future, Stream};
use futures_lite::AsyncWrite;
use std::ops::Deref;

#[async_trait(?Send)]
pub trait Storage:
    AsyncIndexedRead<Value = Self::Content, ReadError = Self::Error>
    + AsyncTruncate<Mark = Self::Position, TruncError = Self::Error>
    + AsyncConsume<ConsumeError = Self::Error>
    + SizedStorage
{
    type Content: Deref<Target = [u8]>;

    type AsyncW: AsyncWrite + Unpin;

    type Position;

    type Error: std::error::Error;

    async fn append<B, S, W, F, T>(
        &mut self,
        byte_stream: &mut S,
        write_fn: W,
    ) -> Result<(Self::Position, T), Self::Error>
    where
        B: Buf,
        S: Stream<Item = B> + Unpin,
        W: FnMut(&mut S, &mut Self::AsyncW) -> F,
        F: Future<Output = std::io::Result<T>>;
}
