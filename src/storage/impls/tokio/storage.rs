use crate::{
    common::stream::StreamUnexpectedLength,
    storage::{AsyncConsume, AsyncTruncate, Sizable},
};
use async_trait::async_trait;
use std::{
    fmt::Display,
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs::{File as TokioFile, OpenOptions},
    task,
};

#[allow(unused)]
pub struct StdFileStorage {
    writer: TokioFile,
    reader: Arc<File>,

    backing_file_path: PathBuf,

    size: u64,
}

#[derive(Debug)]
pub enum StdFileStorageError {
    JoinError(task::JoinError),
    IoError(io::Error),
    StreamUnexpectedLength,
    ReadBeyondWrittenArea,
}

impl From<StreamUnexpectedLength> for StdFileStorageError {
    fn from(_: StreamUnexpectedLength) -> Self {
        Self::StreamUnexpectedLength
    }
}

impl Display for StdFileStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for StdFileStorageError {}

impl StdFileStorage {
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, StdFileStorageError> {
        let backing_file_path = path.as_ref().to_path_buf();

        let writer = Self::obtain_backing_writer_file(path.as_ref()).await?;

        let reader = Self::obtain_backing_reader_file(path.as_ref()).await?;

        let initial_size = writer
            .metadata()
            .await
            .map_err(StdFileStorageError::IoError)?
            .len();

        Ok(Self {
            writer,
            reader: Arc::new(reader),
            backing_file_path,
            size: initial_size,
        })
    }

    async fn obtain_backing_writer_file<P: AsRef<Path>>(
        path: P,
    ) -> Result<TokioFile, StdFileStorageError> {
        OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(path.as_ref())
            .await
            .map_err(StdFileStorageError::IoError)
    }

    async fn obtain_backing_reader_file<P: AsRef<Path>>(
        path: P,
    ) -> Result<File, StdFileStorageError> {
        Ok(OpenOptions::new()
            .read(true)
            .open(path.as_ref())
            .await
            .map_err(StdFileStorageError::IoError)?
            .into_std()
            .await)
    }
}

impl Sizable for StdFileStorage {
    type Size = u64;

    fn size(&self) -> Self::Size {
        self.size
    }
}

#[async_trait(?Send)]
impl AsyncTruncate for StdFileStorage {
    type Mark = u64;

    type TruncError = StdFileStorageError;

    async fn truncate(&mut self, position: &Self::Mark) -> Result<(), Self::TruncError> {
        self.writer
            .sync_all()
            .await
            .map_err(StdFileStorageError::IoError)?;

        self.writer
            .set_len(*position)
            .await
            .map_err(StdFileStorageError::IoError)?;

        let reader = Self::obtain_backing_reader_file(&self.backing_file_path).await?;
        self.reader = Arc::new(reader);

        self.size = *position;

        Ok(())
    }
}

#[async_trait(?Send)]
impl AsyncConsume for StdFileStorage {
    type ConsumeError = StdFileStorageError;

    async fn remove(mut self) -> Result<(), Self::ConsumeError> {
        tokio::fs::remove_file(&self.backing_file_path)
            .await
            .map_err(StdFileStorageError::IoError)
    }

    async fn close(mut self) -> Result<(), Self::ConsumeError> {
        self.writer
            .sync_all()
            .await
            .map_err(StdFileStorageError::IoError)?;

        drop(self.writer);

        let reader = Arc::<_>::into_inner(self.reader);

        tokio::task::spawn_blocking(|| drop(reader))
            .await
            .map_err(StdFileStorageError::JoinError)
    }
}

#[cfg(target_family = "unix")]
pub mod unix {

    use std::os::unix::prelude::FileExt;

    use tokio::io::AsyncWriteExt;

    use crate::storage::Storage;

    use super::*;

    #[async_trait(?Send)]
    impl Storage for StdFileStorage {
        type Content = Vec<u8>;

        type Position = u64;

        type Error = StdFileStorageError;

        async fn append_slice(
            &mut self,
            slice: &[u8],
        ) -> Result<(Self::Position, Self::Size), Self::Error> {
            let current_position = self.size;

            self.writer
                .write_all(slice)
                .await
                .map_err(StdFileStorageError::IoError)?;

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
                return Err(StdFileStorageError::ReadBeyondWrittenArea);
            }

            let reader = self.reader.clone();
            let read_offset = *position;
            let buf_size = *size;

            tokio::task::spawn_blocking(move || {
                let mut read_buf = vec![0_u8; buf_size as usize];
                reader
                    .read_exact_at(&mut read_buf, read_offset)
                    .map(|_| read_buf)
            })
            .await
            .map_err(StdFileStorageError::JoinError)?
            .map_err(StdFileStorageError::IoError)
        }
    }
}
