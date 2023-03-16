use super::super::super::{AsyncConsume, AsyncTruncate, Sizable, Storage, StreamBroken};
use async_trait::async_trait;
use std::{
    io::{
        self, Cursor,
        ErrorKind::{self, UnexpectedEof},
        Read, Seek, SeekFrom, Write,
    },
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
    IoError(io::Error),
    StreamBroken,
}

impl std::fmt::Display for InMemStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for InMemStorageError {}

impl From<ErrorKind> for InMemStorageError {
    fn from(kind: ErrorKind) -> Self {
        Self::IoError(io::Error::from(kind))
    }
}

impl From<StreamBroken> for InMemStorageError {
    fn from(_: StreamBroken) -> Self {
        Self::StreamBroken
    }
}

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

    type Position = usize;

    type Error = InMemStorageError;

    async fn append_slice(
        &mut self,
        slice: &[u8],
    ) -> Result<(Self::Position, Self::Size), Self::Error> {
        let position = self.size();

        self.storage
            .write_all(slice)
            .map_err(InMemStorageError::IoError)?;

        Ok((position, slice.len()))
    }

    async fn read(
        &self,
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

        if vec.len() < *size {
            return Err(UnexpectedEof.into());
        }

        Ok(vec)
    }

    fn is_persistent() -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::{super::super::super::common, InMemStorage};

    #[test]
    fn test_in_mem_storage_read_append_truncate_consistency() {
        futures_lite::future::block_on(async {
            common::test::_test_storage_read_append_truncate_consistency(|| async {
                InMemStorage::default()
            })
            .await;
        });
    }
}
