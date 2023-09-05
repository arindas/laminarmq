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
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum DmaStorageError {
    StorageError(GlommioError<()>),
    IoError(std::io::Error),
    StreamUnexpectedLength,
    ReadBeyondWrittenArea,
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

    backing_file_path: PathBuf,

    buffer_size: usize,
    size: u64,
}

impl DmaStorage {
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, DmaStorageError> {
        Self::with_storage_path_and_buffer_size(path, super::DEFAULT_STORAGE_BUFFER_SIZE).await
    }

    pub async fn with_storage_path_and_buffer_size<P: AsRef<Path>>(
        path: P,
        buffer_size: usize,
    ) -> Result<Self, DmaStorageError> {
        let backing_file_path = path.as_ref().to_path_buf();

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
            backing_file_path,
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
            self.writer
                .close()
                .await
                .map_err(DmaStorageError::IoError)?;

            let backing_writer_file =
                Self::obtain_backing_writer_file(&self.backing_file_path).await?;

            backing_writer_file
                .truncate(*position)
                .await
                .map_err(DmaStorageError::StorageError)?;

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
impl AsyncConsume for DmaStorage {
    type ConsumeError = DmaStorageError;

    async fn remove(mut self) -> Result<(), Self::ConsumeError> {
        glommio::io::remove(self.backing_file_path)
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
}

#[derive(Clone, Copy)]
pub struct DmaStorageProvider;

#[async_trait(?Send)]
impl PathAddressedStorageProvider<DmaStorage> for DmaStorageProvider {
    async fn obtain_storage<P: AsRef<Path>>(&self, path: P) -> Result<DmaStorage, DmaStorageError> {
        DmaStorage::new(path).await
    }
}

#[cfg(test)]
mod tests {
    use crate::common::serde_compat::bincode;
    use crate::storage::impls::{
        common::DiskBackedSegmentStorageProvider, glommio::storage::dma::DmaStorageProvider,
    };

    use super::{
        super::super::super::super::{
            commit_log::segmented_log::{self, index, segment, store},
            common::{self, _TestStorage},
        },
        DmaStorage,
    };
    use glommio::{LocalExecutorBuilder, Placement};
    use std::{fs, marker::PhantomData, path::Path};

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
                    _TestStorage {
                        storage: DmaStorage::new(TEST_DMA_STORAGE_PATH).await.unwrap(),
                        persistent: true,
                    }
                })
                .await;
            })
            .unwrap();

        local_executor.join().unwrap();
    }

    #[test]
    fn test_dma_store_read_append_truncate_consistency() {
        const TEST_DMA_STORE_STORAGE_PATH: &str =
            "/tmp/laminarmq_test_dma_store_read_append_truncate_consistency.store";

        if Path::new(TEST_DMA_STORE_STORAGE_PATH).exists() {
            fs::remove_file(TEST_DMA_STORE_STORAGE_PATH).unwrap();
        }

        let local_executor = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                store::test::_test_store_read_append_truncate_consistency(|| async {
                    (
                        _TestStorage {
                            storage: DmaStorage::new(TEST_DMA_STORE_STORAGE_PATH).await.unwrap(),
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
    fn test_dma_index_read_append_truncate_consistency() {
        const TEST_DMA_INDEX_STORAGE_PATH: &str =
            "/tmp/laminarmq_test_dma_index_read_append_truncate_consistency.index";

        if Path::new(TEST_DMA_INDEX_STORAGE_PATH).exists() {
            fs::remove_file(TEST_DMA_INDEX_STORAGE_PATH).unwrap();
        }

        let local_executor = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                index::test::_test_index_read_append_truncate_consistency(|| async {
                    (
                        _TestStorage {
                            storage: DmaStorage::new(TEST_DMA_INDEX_STORAGE_PATH).await.unwrap(),
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
    fn test_dma_segment_read_append_truncate_consistency() {
        const TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY: &str =
            "/tmp/laminarmq_test_dma_segment_read_append_truncate_consistency";

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            fs::remove_dir_all(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).unwrap();
        }

        let local_executor = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                let disk_backed_storage_provider = DiskBackedSegmentStorageProvider::<
                    DmaStorage,
                    DmaStorageProvider,
                    u32,
                >::with_storage_directory_path_and_provider(
                    TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY,
                    DmaStorageProvider,
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
    fn test_dma_segmented_log_read_append_truncate_consistency() {
        const TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY: &str =
            "/tmp/laminarmq_test_dma_segmented_log_read_append_truncate_consistency";

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            fs::remove_dir_all(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).unwrap();
        }

        let local_executor = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                let disk_backed_storage_provider = DiskBackedSegmentStorageProvider::<
                    DmaStorage,
                    DmaStorageProvider,
                    u32,
                >::with_storage_directory_path_and_provider(
                    TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY,
                    DmaStorageProvider,
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
    fn test_dma_segmented_log_remove_expired_segments() {
        const TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY: &str =
            "/tmp/laminarmq_test_dma_segmented_log_remove_expired_segments";

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            fs::remove_dir_all(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).unwrap();
        }

        let local_executor = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                let disk_backed_storage_provider = DiskBackedSegmentStorageProvider::<
                    DmaStorage,
                    DmaStorageProvider,
                    u32,
                >::with_storage_directory_path_and_provider(
                    TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY,
                    DmaStorageProvider,
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

    #[test]
    fn test_dma_segmented_log_segment_index_caching() {
        const TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY: &str =
            "/tmp/laminarmq_test_dma_segmented_log_segment_index_caching";

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            fs::remove_dir_all(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).unwrap();
        }

        let local_executor = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                let disk_backed_storage_provider = DiskBackedSegmentStorageProvider::<
                    DmaStorage,
                    DmaStorageProvider,
                    u32,
                >::with_storage_directory_path_and_provider(
                    TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY,
                    DmaStorageProvider,
                )
                .unwrap();

                segmented_log::test::_test_segmented_log_segment_index_caching(
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
