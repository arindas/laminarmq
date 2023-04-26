use std::{ops::Deref, path::Path};

use crate::storage::{AsyncTruncate, Sizable, StreamUnexpectedLength};
use async_trait::async_trait;
use futures_lite::AsyncWriteExt;
use glommio::{
    io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions},
    GlommioError,
};

#[derive(Debug)]
pub enum DmaStorageError {
    StorageError(GlommioError<()>),
    IoError(std::io::Error),
    StreamUnexpectedLength,
    NoBackingFileError,
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

impl DmaStorage {
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

        self.writer = Self::stream_writer_with_buffer_size(backing_writer_file, self.buffer_size);

        let reader = Self::obtain_backing_reader_file(backing_file_path.deref()).await?;

        drop(backing_file_path);

        self.reader = reader;

        self.size = *position;

        Ok(())
    }
}
