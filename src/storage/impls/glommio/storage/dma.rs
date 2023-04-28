use crate::storage::{
    impls::common::PathAddressedStorageProvider, AsyncConsume, AsyncTruncate, Sizable, Storage,
    StreamUnexpectedLength,
};
use async_trait::async_trait;
use futures_lite::AsyncWriteExt;
use glommio::{
    io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions, ReadResult},
    GlommioError,
};
use std::{ops::Deref, path::Path};

#[derive(Debug)]
pub enum DmaStorageError {
    StorageError(GlommioError<()>),
    IoError(std::io::Error),
    StreamUnexpectedLength,
    ReadBeyondWrittenArea,
    NoBackingFileError,
}

impl From<std::io::Error> for DmaStorageError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(value)
    }
}

impl std::fmt::Display for DmaStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for DmaStorageError {}

impl From<StreamUnexpectedLength> for DmaStorageError {
    fn from(_: StreamUnexpectedLength) -> Self {
        Self::StreamUnexpectedLength
    }
}

#[derive(Debug)]
pub struct DmaStorage {
    reader: DmaFile,
    writer: DmaStreamWriter,

    buffer_size: usize,
    size: u64,
}

pub const DEFAULT_DMA_STORAGE_BUFFER_SIZE: usize = 128 << 10;

impl DmaStorage {
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, DmaStorageError> {
        Self::with_storage_path_and_buffer_size(path, DEFAULT_DMA_STORAGE_BUFFER_SIZE).await
    }

    pub async fn with_storage_path_and_buffer_size<P: AsRef<Path>>(
        path: P,
        buffer_size: usize,
    ) -> Result<Self, DmaStorageError> {
        let backing_dma_storage_writer_file =
            Self::obtain_backing_writer_file(path.as_ref()).await?;

        let initial_size = backing_dma_storage_writer_file
            .file_size()
            .await
            .map_err(DmaStorageError::StorageError)?;

        Ok(Self {
            reader: Self::obtain_backing_reader_file(path).await?,
            writer: Self::stream_writer_with_buffer_size(
                backing_dma_storage_writer_file,
                buffer_size,
            ),
            buffer_size,
            size: initial_size,
        })
    }

    async fn obtain_backing_writer_file<P: AsRef<Path>>(
        path: P,
    ) -> Result<DmaFile, DmaStorageError> {
        OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .dma_open(path.as_ref())
            .await
            .map_err(DmaStorageError::StorageError)
    }

    async fn obtain_backing_reader_file<P: AsRef<Path>>(
        path: P,
    ) -> Result<DmaFile, DmaStorageError> {
        OpenOptions::new()
            .read(true)
            .dma_open(path.as_ref())
            .await
            .map_err(DmaStorageError::StorageError)
    }

    fn stream_writer_with_buffer_size(
        backing_writer_file: DmaFile,
        buffer_size: usize,
    ) -> DmaStreamWriter {
        DmaStreamWriterBuilder::new(backing_writer_file)
            .with_buffer_size(buffer_size)
            .build()
    }
}

impl Sizable for DmaStorage {
    type Size = u64;

    fn size(&self) -> Self::Size {
        self.size
    }
}

#[async_trait(?Send)]
impl AsyncTruncate for DmaStorage {
    type Mark = u64;

    type TruncError = DmaStorageError;

    async fn truncate(&mut self, position: &Self::Mark) -> Result<(), Self::TruncError> {
        let (writer, reader) = {
            let backing_file_path = self
                .reader
                .path()
                .ok_or(DmaStorageError::NoBackingFileError)?;

            self.writer
                .close()
                .await
                .map_err(DmaStorageError::IoError)?;

            let backing_writer_file =
                Self::obtain_backing_writer_file(backing_file_path.deref()).await?;

            backing_writer_file
                .truncate(*position)
                .await
                .map_err(DmaStorageError::StorageError)?;

            (
                Self::stream_writer_with_buffer_size(backing_writer_file, self.buffer_size),
                Self::obtain_backing_reader_file(backing_file_path.deref()).await?,
            )
        };

        self.writer = writer;
        self.reader = reader;

        self.size = *position;

        Ok(())
    }
}

#[async_trait(?Send)]
impl AsyncConsume for DmaStorage {
    type ConsumeError = DmaStorageError;

    async fn remove(self) -> Result<(), Self::ConsumeError> {
        let backing_file_path = self
            .reader
            .path()
            .ok_or(DmaStorageError::NoBackingFileError)?
            .to_path_buf();

        self.close().await?;

        glommio::io::remove(backing_file_path)
            .await
            .map_err(DmaStorageError::StorageError)
    }

    async fn close(mut self) -> Result<(), Self::ConsumeError> {
        self.writer
            .close()
            .await
            .map_err(DmaStorageError::IoError)?;
        self.reader
            .close()
            .await
            .map_err(DmaStorageError::StorageError)
    }
}

#[async_trait(?Send)]
impl Storage for DmaStorage {
    type Content = ReadResult;

    type Position = u64;

    type Error = DmaStorageError;

    async fn append_slice(
        &mut self,
        slice: &[u8],
    ) -> Result<(Self::Position, Self::Size), Self::Error> {
        let current_position = self.size;

        self.writer
            .write_all(slice)
            .await
            .map_err(DmaStorageError::IoError)?;

        let bytes_written = slice.len() as u64;

        self.size += bytes_written;

        Ok((current_position, bytes_written))
    }

    async fn read(
        &self,
        position: &Self::Position,
        size: &Self::Size,
    ) -> Result<Self::Content, Self::Error> {
        if *position + *size > self.size {
            return Err(DmaStorageError::ReadBeyondWrittenArea);
        }

        self.reader
            .read_at(*position, *size as usize)
            .await
            .map_err(DmaStorageError::StorageError)
    }

    fn is_persistent() -> bool {
        true
    }
}

pub struct DmaStorageProvider;

#[async_trait(?Send)]
impl PathAddressedStorageProvider<DmaStorage> for DmaStorageProvider {
    async fn obtain_storage<P: AsRef<Path>>(&self, path: P) -> Result<DmaStorage, DmaStorageError> {
        DmaStorage::new(path).await
    }
}

#[cfg(test)]
mod tests {
    use super::{super::super::super::super::common, DmaStorage};
    use glommio::{LocalExecutorBuilder, Placement};
    use std::{fs, path::Path};

    #[test]
    fn test_dma_storage_read_append_truncate_consistency() {
        const TEST_DMA_STORAGE_PATH: &str =
            "/tmp/laminarmq_test_dma_storage_read_append_truncate_consistency.storage";

        if Path::new(TEST_DMA_STORAGE_PATH).exists() {
            fs::remove_file(TEST_DMA_STORAGE_PATH).unwrap();
        }

        let local_executor = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                common::test::_test_storage_read_append_truncate_consistency(|| async {
                    DmaStorage::new(TEST_DMA_STORAGE_PATH).await.unwrap()
                })
                .await;
            })
            .unwrap();

        local_executor.join().unwrap();
    }
}
