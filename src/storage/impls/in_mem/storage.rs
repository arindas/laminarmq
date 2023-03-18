use super::super::super::{AsyncConsume, AsyncTruncate, Sizable, Storage, StreamBroken};
use async_trait::async_trait;
use std::{
    cell::RefCell,
    io::{
        self, Cursor,
        ErrorKind::{self, UnexpectedEof},
        Read, Seek, SeekFrom, Write,
    },
    ops::Deref,
    rc::Rc,
};

#[derive(Default)]
pub struct InMemStorage {
    storage: Rc<RefCell<Vec<u8>>>,
    size: usize,
}

impl InMemStorage {
    pub fn new(storage: Rc<RefCell<Vec<u8>>>) -> Result<Self, InMemStorageError> {
        let size = storage
            .try_borrow()
            .map_err(|_| InMemStorageError::BorrowError)?
            .len();

        Ok(Self { storage, size })
    }
}

impl Sizable for InMemStorage {
    type Size = usize;

    fn size(&self) -> Self::Size {
        self.size
    }
}

#[derive(Debug)]
pub enum InMemStorageError {
    IoError(io::Error),
    StreamBroken,
    BorrowError,
    StorageNotFound,
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

    async fn truncate(&mut self, position: &Self::Mark) -> Result<(), Self::TruncError> {
        let mut storage_vec = self
            .storage
            .try_borrow_mut()
            .map_err(|_| InMemStorageError::BorrowError)?;

        storage_vec.truncate(*position);

        self.size = storage_vec.len();

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
        let position = self.size;

        let mut storage_vec = self
            .storage
            .try_borrow_mut()
            .map_err(|_| InMemStorageError::BorrowError)?;

        storage_vec
            .write_all(slice)
            .map_err(InMemStorageError::IoError)?;

        self.size = storage_vec.len();

        Ok((position, slice.len()))
    }

    async fn read(
        &self,
        position: &Self::Position,
        size: &Self::Size,
    ) -> Result<Self::Content, Self::Error> {
        let storage_vec = self
            .storage
            .try_borrow()
            .map_err(|_| InMemStorageError::BorrowError)?;

        let mut cursor = Cursor::new(storage_vec.deref().deref());

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
