use super::super::super::{AsyncConsume, AsyncTruncate, Sizable, Storage};
use async_trait::async_trait;
use bytes::Buf;
use futures_core::{Future, Stream};
use std::{
    io::{
        self, Cursor,
        ErrorKind::{self, UnexpectedEof},
        Read, Seek, SeekFrom,
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
        F: Future<Output = io::Result<T>>,
        W: FnMut(&'byte_stream mut S, &'storage mut Self::Write) -> F,
    {
        let position = self.size();
        let write_fn_returned_value = write_fn(byte_stream, &mut self.storage)
            .await
            .map_err(InMemStorageError::IoError)?;

        Ok((position, write_fn_returned_value))
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

    use super::{
        super::super::super::common::write_stream, InMemStorage, InMemStorageError, Sizable,
        Storage,
    };
    use futures_lite::stream;

    #[test]
    fn test_in_mem_storage_read_write_consistency() {
        futures_lite::future::block_on(async {
            const REQ_BYTES: &[u8] = b"Hello World!";
            let mut req_body = stream::iter(std::iter::once(REQ_BYTES));

            let mut in_mem_storage = InMemStorage::default();

            assert!(matches!(
                in_mem_storage.read(&(0 as usize), &(1 as usize)).await,
                Err(InMemStorageError::IoError(_))
            ));

            let write_position = in_mem_storage.size();

            let (position, bytes_written) = in_mem_storage
                .append(&mut req_body, &mut write_stream)
                .await
                .unwrap();

            assert_eq!(position, write_position);
            assert_eq!(bytes_written, REQ_BYTES.len());

            let read_bytes = in_mem_storage
                .read(&position, &bytes_written)
                .await
                .unwrap();

            assert_eq!(read_bytes, REQ_BYTES);

            const REPEAT: usize = 5;
            let mut repeated_req_body = stream::iter([REQ_BYTES; REPEAT]);

            let write_position = in_mem_storage.size();

            let (position, bytes_written) = in_mem_storage
                .append(&mut repeated_req_body, &mut write_stream)
                .await
                .unwrap();

            assert_eq!(position, write_position);
            assert_eq!(bytes_written, REQ_BYTES.len() * REPEAT);

            let read_bytes = in_mem_storage
                .read(&position, &bytes_written)
                .await
                .unwrap();

            for i in 0..REPEAT {
                let (lo, hi) = (i * REQ_BYTES.len(), (i + 1) * REQ_BYTES.len());
                assert_eq!(REQ_BYTES, &read_bytes[lo..hi]);
            }
        });
    }
}
