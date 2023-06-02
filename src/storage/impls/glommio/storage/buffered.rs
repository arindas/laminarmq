use crate::storage::{
    impls::common::PathAddressedStorageProvider, AsyncConsume, AsyncTruncate, Sizable, Storage,
    StreamUnexpectedLength,
};
use async_trait::async_trait;
use futures_lite::AsyncWriteExt;
use glommio::{
    io::{BufferedFile, OpenOptions, ReadResult, StreamWriter, StreamWriterBuilder},
    GlommioError,
};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum BufferedStorageError {
    StorageError(GlommioError<()>),
    IoError(std::io::Error),
    StreamUnexpectedLength,
    ReadBeyondWrittenArea,
}

impl From<std::io::Error> for BufferedStorageError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(value)
    }
}

impl std::fmt::Display for BufferedStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for BufferedStorageError {}

impl From<StreamUnexpectedLength> for BufferedStorageError {
    fn from(_: StreamUnexpectedLength) -> Self {
        Self::StreamUnexpectedLength
    }
}

pub struct BufferedStorage {
    reader: BufferedFile,
    writer: StreamWriter,

    backing_file_path: PathBuf,

    buffer_size: usize,
    size: u64,
}

impl BufferedStorage {
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, BufferedStorageError> {
        Self::with_storage_path_and_buffer_size(path, super::DEFAULT_STORAGE_BUFFER_SIZE).await
    }

    pub async fn with_storage_path_and_buffer_size<P: AsRef<Path>>(
        path: P,
        buffer_size: usize,
    ) -> Result<Self, BufferedStorageError> {
        let backing_file_path = path.as_ref().to_path_buf();

        let backing_buffered_file_storage_writer_file =
            Self::obtain_backing_writer_file(path.as_ref()).await?;

        let initial_size = backing_buffered_file_storage_writer_file
            .file_size()
            .await
            .map_err(BufferedStorageError::StorageError)?;

        Ok(Self {
            reader: Self::obtain_backing_reader_file(path).await?,
            writer: Self::stream_writer_with_buffer_size(
                backing_buffered_file_storage_writer_file,
                buffer_size,
            ),
            backing_file_path,
            buffer_size,
            size: initial_size,
        })
    }

    async fn obtain_backing_writer_file<P: AsRef<Path>>(
        path: P,
    ) -> Result<BufferedFile, BufferedStorageError> {
        OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .buffered_open(path.as_ref())
            .await
            .map_err(BufferedStorageError::StorageError)
    }

    async fn obtain_backing_reader_file<P: AsRef<Path>>(
        path: P,
    ) -> Result<BufferedFile, BufferedStorageError> {
        OpenOptions::new()
            .read(true)
            .buffered_open(path.as_ref())
            .await
            .map_err(BufferedStorageError::StorageError)
    }

    fn stream_writer_with_buffer_size(
        backing_writer_file: BufferedFile,
        buffer_size: usize,
    ) -> StreamWriter {
        StreamWriterBuilder::new(backing_writer_file)
            .with_buffer_size(buffer_size)
            .build()
    }
}

impl Sizable for BufferedStorage {
    type Size = u64;

    fn size(&self) -> Self::Size {
        self.size
    }
}

#[async_trait(?Send)]
impl AsyncTruncate for BufferedStorage {
    type Mark = u64;

    type TruncError = BufferedStorageError;

    async fn truncate(&mut self, position: &Self::Mark) -> Result<(), Self::TruncError> {
        let (writer, reader) = {
            self.writer
                .close()
                .await
                .map_err(BufferedStorageError::IoError)?;

            let backing_writer_file =
                Self::obtain_backing_writer_file(&self.backing_file_path).await?;

            backing_writer_file
                .truncate(*position)
                .await
                .map_err(BufferedStorageError::StorageError)?;

            (
                Self::stream_writer_with_buffer_size(backing_writer_file, self.buffer_size),
                Self::obtain_backing_reader_file(&self.backing_file_path).await?,
            )
        };

        self.writer = writer;
        self.reader = reader;

        self.size = *position;

        Ok(())
    }
}

#[async_trait(?Send)]
impl AsyncConsume for BufferedStorage {
    type ConsumeError = BufferedStorageError;

    async fn remove(mut self) -> Result<(), Self::ConsumeError> {
        glommio::io::remove(self.backing_file_path)
            .await
            .map_err(BufferedStorageError::StorageError)
    }

    async fn close(mut self) -> Result<(), Self::ConsumeError> {
        self.writer
            .close()
            .await
            .map_err(BufferedStorageError::IoError)?;
        self.reader
            .close()
            .await
            .map_err(BufferedStorageError::StorageError)
    }
}

#[async_trait(?Send)]
impl Storage for BufferedStorage {
    type Content = ReadResult;

    type Position = u64;

    type Error = BufferedStorageError;

    async fn append_slice(
        &mut self,
        slice: &[u8],
    ) -> Result<(Self::Position, Self::Size), Self::Error> {
        let current_position = self.size;

        self.writer
            .write_all(slice)
            .await
            .map_err(BufferedStorageError::IoError)?;

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
            return Err(BufferedStorageError::ReadBeyondWrittenArea);
        }

        self.reader
            .read_at(*position, *size as usize)
            .await
            .map_err(BufferedStorageError::StorageError)
    }
}

#[derive(Clone, Copy)]
pub struct BufferedStorageProvider;

#[async_trait(?Send)]
impl PathAddressedStorageProvider<BufferedStorage> for BufferedStorageProvider {
    async fn obtain_storage<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<BufferedStorage, BufferedStorageError> {
        BufferedStorage::new(path).await
    }
}

#[cfg(test)]
mod tests {
    use crate::common::serde_compat::bincode;
    use crate::storage::impls::{
        common::DiskBackedSegmentStorageProvider,
        glommio::storage::buffered::BufferedStorageProvider,
    };

    use super::{
        super::super::super::super::{
            commit_log::segmented_log::{self, index, segment, store},
            common::{self, _TestStorage},
        },
        BufferedStorage,
    };
    use glommio::{LocalExecutorBuilder, Placement};
    use std::{fs, marker::PhantomData, path::Path};

    #[test]
    fn test_buffered_storage_read_append_truncate_consistency() {
        const TEST_BUFFERED_STORAGE_PATH: &str =
            "/tmp/laminarmq_test_buffered_storage_read_append_truncate_consistency.storage";

        if Path::new(TEST_BUFFERED_STORAGE_PATH).exists() {
            fs::remove_file(TEST_BUFFERED_STORAGE_PATH).unwrap();
        }

        let local_executor = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                common::test::_test_storage_read_append_truncate_consistency(|| async {
                    _TestStorage {
                        storage: BufferedStorage::new(TEST_BUFFERED_STORAGE_PATH)
                            .await
                            .unwrap(),
                        persistent: true,
                    }
                })
                .await;
            })
            .unwrap();

        local_executor.join().unwrap();
    }

    #[test]
    fn test_buffered_store_read_append_truncate_consistency() {
        const TEST_BUFFERED_STORE_STORAGE_PATH: &str =
            "/tmp/laminarmq_test_buffered_store_read_append_truncate_consistency.store";

        if Path::new(TEST_BUFFERED_STORE_STORAGE_PATH).exists() {
            fs::remove_file(TEST_BUFFERED_STORE_STORAGE_PATH).unwrap();
        }

        let local_executor = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                store::test::_test_store_read_append_truncate_consistency(|| async {
                    (
                        _TestStorage {
                            storage: BufferedStorage::new(TEST_BUFFERED_STORE_STORAGE_PATH)
                                .await
                                .unwrap(),
                            persistent: true,
                        },
                        PhantomData::<crc32fast::Hasher>,
                    )
                })
                .await;
            })
            .unwrap();

        local_executor.join().unwrap();
    }

    #[test]
    fn test_buffered_index_read_append_truncate_consistency() {
        const TEST_BUFFERED_INDEX_STORAGE_PATH: &str =
            "/tmp/laminarmq_test_buffered_index_read_append_truncate_consistency.index";

        if Path::new(TEST_BUFFERED_INDEX_STORAGE_PATH).exists() {
            fs::remove_file(TEST_BUFFERED_INDEX_STORAGE_PATH).unwrap();
        }

        let local_executor = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                index::test::_test_index_read_append_truncate_consistency(|| async {
                    (
                        _TestStorage {
                            storage: BufferedStorage::new(TEST_BUFFERED_INDEX_STORAGE_PATH)
                                .await
                                .unwrap(),
                            persistent: true,
                        },
                        PhantomData::<(crc32fast::Hasher, u32)>,
                    )
                })
                .await;
            })
            .unwrap();

        local_executor.join().unwrap();
    }

    #[test]
    fn test_buffered_segment_read_append_truncate_consistency() {
        const TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY: &str =
            "/tmp/laminarmq_test_buffered_segment_read_append_truncate_consistency";

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            fs::remove_dir_all(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).unwrap();
        }

        let local_executor = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                let disk_backed_storage_provider = DiskBackedSegmentStorageProvider::<
                    BufferedStorage,
                    BufferedStorageProvider,
                    u32,
                >::with_storage_directory_path_and_provider(
                    TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY,
                    BufferedStorageProvider,
                )
                .unwrap();

                segment::test::_test_segment_read_append_truncate_consistency(
                    disk_backed_storage_provider,
                    PhantomData::<((), crc32fast::Hasher, bincode::BinCode)>,
                )
                .await;
            })
            .unwrap();

        local_executor.join().unwrap();

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            fs::remove_dir_all(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).unwrap();
        }
    }

    #[test]
    fn test_buffered_segmented_log_read_append_truncate_consistency() {
        const TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY: &str =
            "/tmp/laminarmq_test_buffered_segmented_log_read_append_truncate_consistency";

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            fs::remove_dir_all(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).unwrap();
        }

        let local_executor = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                let disk_backed_storage_provider = DiskBackedSegmentStorageProvider::<
                    BufferedStorage,
                    BufferedStorageProvider,
                    u32,
                >::with_storage_directory_path_and_provider(
                    TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY,
                    BufferedStorageProvider,
                )
                .unwrap();

                segmented_log::test::_test_segmented_log_read_append_truncate_consistency(
                    disk_backed_storage_provider,
                    PhantomData::<((), crc32fast::Hasher, bincode::BinCode)>,
                )
                .await;
            })
            .unwrap();

        local_executor.join().unwrap();

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            fs::remove_dir_all(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).unwrap();
        }
    }

    #[test]
    fn test_buffered_segmented_log_remove_expired_segments() {
        const TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY: &str =
            "/tmp/laminarmq_test_buffered_segmented_log_remove_expired_segments";

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            fs::remove_dir_all(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).unwrap();
        }

        let local_executor = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                let disk_backed_storage_provider = DiskBackedSegmentStorageProvider::<
                    BufferedStorage,
                    BufferedStorageProvider,
                    u32,
                >::with_storage_directory_path_and_provider(
                    TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY,
                    BufferedStorageProvider,
                )
                .unwrap();

                segmented_log::test::_test_segmented_log_remove_expired_segments(
                    disk_backed_storage_provider,
                    |duration| async move { glommio::timer::sleep(duration).await },
                    PhantomData::<((), crc32fast::Hasher, bincode::BinCode)>,
                )
                .await;
            })
            .unwrap();

        local_executor.join().unwrap();

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            fs::remove_dir_all(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).unwrap();
        }
    }
}
