use crate::{
    common::stream::StreamUnexpectedLength,
    storage::{
        impls::common::PathAddressedStorageProvider, AsyncConsume, AsyncTruncate, Sizable, Storage,
    },
};
use async_trait::async_trait;
use std::{
    fmt::Display,
    io,
    path::{Path, PathBuf},
};
use tokio::{
    fs::{File as TokioFile, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter},
    sync::RwLock,
    task,
};

#[allow(unused)]
pub struct StdSeekReadFileStorage {
    storage: RwLock<BufWriter<TokioFile>>,
    backing_file_path: PathBuf,

    size: u64,
}

#[derive(Debug)]
pub enum StdSeekReadFileStorageError {
    JoinError(task::JoinError),
    IoError(io::Error),
    StreamUnexpectedLength,
    ReadBeyondWrittenArea,
    StdFileInUse,
}

impl From<StreamUnexpectedLength> for StdSeekReadFileStorageError {
    fn from(_: StreamUnexpectedLength) -> Self {
        Self::StreamUnexpectedLength
    }
}

impl From<io::Error> for StdSeekReadFileStorageError {
    fn from(value: io::Error) -> Self {
        Self::IoError(value)
    }
}

impl Display for StdSeekReadFileStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for StdSeekReadFileStorageError {}

impl StdSeekReadFileStorage {
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, StdSeekReadFileStorageError> {
        let backing_file_path = path.as_ref().to_path_buf();

        let storage = Self::obtain_backing_storage(&backing_file_path).await?;

        let initial_size = storage
            .metadata()
            .await
            .map_err(StdSeekReadFileStorageError::IoError)?
            .len();

        Ok(Self {
            storage: RwLock::new(BufWriter::new(storage)),
            backing_file_path,
            size: initial_size,
        })
    }

    async fn obtain_backing_storage<P: AsRef<Path>>(
        path: P,
    ) -> Result<TokioFile, StdSeekReadFileStorageError> {
        OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .read(true)
            .open(path)
            .await
            .map_err(StdSeekReadFileStorageError::IoError)
    }
}

impl Sizable for StdSeekReadFileStorage {
    type Size = u64;

    fn size(&self) -> Self::Size {
        self.size
    }
}

#[async_trait(?Send)]
impl AsyncTruncate for StdSeekReadFileStorage {
    type Mark = u64;

    type TruncError = StdSeekReadFileStorageError;

    async fn truncate(&mut self, position: &Self::Mark) -> Result<(), Self::TruncError> {
        self.storage
            .write()
            .await
            .flush()
            .await
            .map_err(StdSeekReadFileStorageError::IoError)?;

        let writer = Self::obtain_backing_storage(&self.backing_file_path).await?;

        writer
            .set_len(*position)
            .await
            .map_err(StdSeekReadFileStorageError::IoError)?;

        self.storage = RwLock::new(BufWriter::new(writer));
        self.size = *position;

        Ok(())
    }
}

#[async_trait(?Send)]
impl AsyncConsume for StdSeekReadFileStorage {
    type ConsumeError = StdSeekReadFileStorageError;

    async fn remove(mut self) -> Result<(), Self::ConsumeError> {
        let backing_file_path = self.backing_file_path.clone();

        self.close().await?;

        tokio::fs::remove_file(&backing_file_path)
            .await
            .map_err(StdSeekReadFileStorageError::IoError)
    }

    async fn close(mut self) -> Result<(), Self::ConsumeError> {
        self.storage
            .write()
            .await
            .flush()
            .await
            .map_err(StdSeekReadFileStorageError::IoError)
    }
}

#[derive(Clone, Copy)]
pub struct StdSeekReadFileStorageProvider;

#[async_trait(?Send)]
impl Storage for StdSeekReadFileStorage {
    type Content = Vec<u8>;

    type Position = u64;

    type Error = StdSeekReadFileStorageError;

    async fn append_slice(
        &mut self,
        slice: &[u8],
    ) -> Result<(Self::Position, Self::Size), Self::Error> {
        let current_position = self.size;

        self.storage
            .write()
            .await
            .write_all(slice)
            .await
            .map_err(StdSeekReadFileStorageError::IoError)?;

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
            return Err(StdSeekReadFileStorageError::ReadBeyondWrittenArea);
        }

        let mut read_buf = vec![0_u8; *size as usize];

        let mut storage = self.storage.write().await;

        storage
            .seek(io::SeekFrom::Start(*position))
            .await
            .map_err(StdSeekReadFileStorageError::IoError)?;

        storage
            .read_exact(&mut read_buf)
            .await
            .map_err(StdSeekReadFileStorageError::IoError)?;

        storage
            .seek(io::SeekFrom::Start(self.size))
            .await
            .map_err(StdSeekReadFileStorageError::IoError)?;

        Ok(read_buf)
    }
}

#[async_trait(?Send)]
impl PathAddressedStorageProvider<StdSeekReadFileStorage> for StdSeekReadFileStorageProvider {
    async fn obtain_storage<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<StdSeekReadFileStorage, StdSeekReadFileStorageError> {
        StdSeekReadFileStorage::new(path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        common::serde_compat::bincode,
        storage::{
            commit_log::segmented_log::{self, index, segment, store},
            common::{self, _TestStorage},
            impls::common::DiskBackedSegmentStorageProvider,
        },
    };
    use std::marker::PhantomData;

    #[tokio::test]
    async fn test_tokio_std_seek_read_file_storage_read_append_truncate_consistency() {
        const TEST_TOKIO_STD_SEEK_READ_FILE_STORAGE_PATH: &str =
            "/tmp/laminarmq_test_tokio_std_seek_read_file_storage_read_append_truncate_consistency.storage";

        if Path::new(TEST_TOKIO_STD_SEEK_READ_FILE_STORAGE_PATH).exists() {
            let path = TEST_TOKIO_STD_SEEK_READ_FILE_STORAGE_PATH;
            tokio::fs::remove_file(path).await.unwrap();
        }

        common::test::_test_storage_read_append_truncate_consistency(|| async {
            _TestStorage {
                storage: StdSeekReadFileStorage::new(TEST_TOKIO_STD_SEEK_READ_FILE_STORAGE_PATH)
                    .await
                    .unwrap(),
                persistent: true,
            }
        })
        .await;
    }

    #[tokio::test]
    async fn test_tokio_std_seek_read_file_store_read_append_truncate_consistency() {
        const TEST_TOKIO_STD_SEEK_READ_FILE_STORE_STORAGE_PATH: &str =
            "/tmp/laminarmq_test_tokio_std_seek_read_file_store_read_append_truncate_consistency.store";

        if Path::new(TEST_TOKIO_STD_SEEK_READ_FILE_STORE_STORAGE_PATH).exists() {
            let path = TEST_TOKIO_STD_SEEK_READ_FILE_STORE_STORAGE_PATH;
            tokio::fs::remove_file(path).await.unwrap();
        }

        store::test::_test_store_read_append_truncate_consistency(|| async {
            (
                _TestStorage {
                    storage: StdSeekReadFileStorage::new(
                        TEST_TOKIO_STD_SEEK_READ_FILE_STORE_STORAGE_PATH,
                    )
                    .await
                    .unwrap(),
                    persistent: true,
                },
                PhantomData::<crc32fast::Hasher>,
            )
        })
        .await;
    }

    #[tokio::test]
    async fn test_tokio_std_seek_read_file_index_read_append_truncate_consistency() {
        const TEST_TOKIO_STD_SEEK_READ_FILE_INDEX_STORAGE_PATH: &str =
            "/tmp/laminarmq_test_tokio_std_seek_read_file_index_read_append_truncate_consistency.index";

        if Path::new(TEST_TOKIO_STD_SEEK_READ_FILE_INDEX_STORAGE_PATH).exists() {
            let path = TEST_TOKIO_STD_SEEK_READ_FILE_INDEX_STORAGE_PATH;
            tokio::fs::remove_file(path).await.unwrap();
        }

        index::test::_test_index_read_append_truncate_consistency(|| async {
            (
                _TestStorage {
                    storage: StdSeekReadFileStorage::new(
                        TEST_TOKIO_STD_SEEK_READ_FILE_INDEX_STORAGE_PATH,
                    )
                    .await
                    .unwrap(),
                    persistent: true,
                },
                PhantomData::<(crc32fast::Hasher, u32)>,
            )
        })
        .await;
    }

    #[tokio::test]
    async fn test_tokio_std_seek_read_file_segment_read_append_truncate_consistency() {
        const TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY: &str =
            "/tmp/laminarmq_test_tokio_std_seek_read_file_segment_read_append_truncate_consistency";

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            let directory_path = TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY;
            tokio::fs::remove_dir_all(directory_path).await.unwrap();
        }

        let disk_backed_storage_provider =
                DiskBackedSegmentStorageProvider::<_, _, u32>::with_storage_directory_path_and_provider(
                    TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY,
                    StdSeekReadFileStorageProvider,
                )
                .unwrap();

        segment::test::_test_segment_read_append_truncate_consistency(
            disk_backed_storage_provider,
            PhantomData::<((), crc32fast::Hasher, bincode::BinCode)>,
        )
        .await;

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            let directory_path = TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY;
            tokio::fs::remove_dir_all(directory_path).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_tokio_std_seek_read_file_segmented_log_read_append_truncate_consistency() {
        const TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY: &str =
            "/tmp/laminarmq_test_tokio_std_seek_read_file_segmented_log_read_append_truncate_consistency";

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            let directory_path = TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY;
            tokio::fs::remove_dir_all(directory_path).await.unwrap();
        }

        let disk_backed_storage_provider =
                DiskBackedSegmentStorageProvider::<_, _, u32>::with_storage_directory_path_and_provider(
                    TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY,
                    StdSeekReadFileStorageProvider,
                )
                .unwrap();

        segmented_log::test::_test_segmented_log_read_append_truncate_consistency(
            disk_backed_storage_provider,
            PhantomData::<((), crc32fast::Hasher, bincode::BinCode)>,
        )
        .await;

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            let directory_path = TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY;
            tokio::fs::remove_dir_all(directory_path).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_tokio_std_seek_read_file_segmented_log_remove_expired_segments() {
        const TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY: &str =
            "/tmp/laminarmq_test_tokio_std_seek_read_file_segmented_log_remove_expired_segments";

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            let directory_path = TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY;
            tokio::fs::remove_dir_all(directory_path).await.unwrap();
        }

        let disk_backed_storage_provider =
                DiskBackedSegmentStorageProvider::<_, _, u32>::with_storage_directory_path_and_provider(
                    TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY,
                    StdSeekReadFileStorageProvider,
                )
                .unwrap();

        segmented_log::test::_test_segmented_log_remove_expired_segments(
            disk_backed_storage_provider,
            |duration| async move { tokio::time::sleep(duration).await },
            PhantomData::<((), crc32fast::Hasher, bincode::BinCode)>,
        )
        .await;

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            let directory_path = TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY;
            tokio::fs::remove_dir_all(directory_path).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_tokio_std_seek_read_file_segmented_log_segment_index_caching() {
        const TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY: &str =
            "/tmp/laminarmq_test_tokio_std_seek_read_file_segmented_log_segment_index_caching";

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            let directory_path = TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY;
            tokio::fs::remove_dir_all(directory_path).await.unwrap();
        }

        let disk_backed_storage_provider =
                DiskBackedSegmentStorageProvider::<_, _, u32>::with_storage_directory_path_and_provider(
                    TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY,
                    StdSeekReadFileStorageProvider,
                )
                .unwrap();

        segmented_log::test::_test_segmented_log_segment_index_caching(
            disk_backed_storage_provider,
            |duration| async move { tokio::time::sleep(duration).await },
            true,
            PhantomData::<((), crc32fast::Hasher, bincode::BinCode)>,
        )
        .await;

        if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
            let directory_path = TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY;
            tokio::fs::remove_dir_all(directory_path).await.unwrap();
        }
    }
}
