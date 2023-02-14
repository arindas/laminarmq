use super::super::super::{AsyncConsume, AsyncTruncate, Sizable, Storage};
use async_trait::async_trait;
use bytes::Buf;
use futures_core::{Future, Stream};
use std::{
    io::{Cursor, Read, Seek, SeekFrom},
    ops::Deref,
};

#[derive(Default)]
pub struct InMemStorage {
    storage: Vec<u8>,
}

impl InMemStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Sizable for InMemStorage {
    type Size = usize;

    fn size(&self) -> Self::Size {
        self.storage.len()
    }
}

#[derive(Debug)]
pub enum InMemStorageError {
    IoError(std::io::Error),
}

impl std::fmt::Display for InMemStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for InMemStorageError {}

#[async_trait(?Send)]
impl AsyncTruncate for InMemStorage {
    type Mark = usize;

    type TruncError = InMemStorageError;

    async fn truncate(&mut self, pos: &Self::Mark) -> Result<(), Self::TruncError> {
        self.storage.truncate(*pos);

        Ok(())
    }
}

#[async_trait(?Send)]
impl AsyncConsume for InMemStorage {
    type ConsumeError = InMemStorageError;

    async fn remove(self) -> Result<(), Self::ConsumeError> {
        Ok(())
    }

    async fn close(self) -> Result<(), Self::ConsumeError> {
        Ok(())
    }
}

#[async_trait(?Send)]
impl Storage for InMemStorage {
    type Content = Vec<u8>;

    type Write = Vec<u8>;

    type Position = usize;

    type Error = InMemStorageError;

    async fn append<'storage, 'byte_stream, B, S, W, F, T>(
        &'storage mut self,
        byte_stream: &'byte_stream mut S,
        write_fn: &mut W,
    ) -> Result<(Self::Position, T), Self::Error>
    where
        B: Buf,
        S: Stream<Item = B> + Unpin,
        F: Future<Output = std::io::Result<T>>,
        W: FnMut(&'byte_stream mut S, &'storage mut Self::Write) -> F,
    {
        let position = self.size();
        let write_fn_returned_value = write_fn(byte_stream, &mut self.storage)
            .await
            .map_err(InMemStorageError::IoError)?;

        Ok((position, write_fn_returned_value))
    }

    async fn read(
        &mut self,
        position: &Self::Position,
        size: &Self::Size,
    ) -> Result<Self::Content, Self::Error> {
        let mut cursor = Cursor::new(self.storage.deref());

        cursor
            .seek(SeekFrom::Start(*position as u64))
            .map_err(InMemStorageError::IoError)?;

        let mut vec = Vec::new();

        <Cursor<&[u8]> as Read>::take(cursor, *size as u64)
            .read_to_end(&mut vec)
            .map_err(InMemStorageError::IoError)?;

        Ok(vec)
    }
}
