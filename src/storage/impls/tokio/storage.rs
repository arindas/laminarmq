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
    io::{AsyncWriteExt, BufWriter},
    task,
};

#[allow(unused)]
pub struct StdFileStorage {
    writer: BufWriter<TokioFile>,
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
    StdFileInUse,
}

impl From<StreamUnexpectedLength> for StdFileStorageError {
    fn from(_: StreamUnexpectedLength) -> Self {
        Self::StreamUnexpectedLength
    }
}

impl From<io::Error> for StdFileStorageError {
    fn from(value: io::Error) -> Self {
        Self::IoError(value)
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
            writer: BufWriter::new(writer),
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
            .flush()
            .await
            .map_err(StdFileStorageError::IoError)?;

        let writer = Self::obtain_backing_writer_file(&self.backing_file_path).await?;

        writer
            .set_len(*position)
            .await
            .map_err(StdFileStorageError::IoError)?;

        self.writer = BufWriter::new(writer);

        let reader = Self::obtain_backing_reader_file(&self.backing_file_path).await?;

        let old_reader = std::mem::replace(&mut self.reader, Arc::new(reader));

        tokio::task::spawn_blocking(|| drop(old_reader))
            .await
            .map_err(StdFileStorageError::JoinError)?;

        self.size = *position;

        Ok(())
    }
}

#[async_trait(?Send)]
impl AsyncConsume for StdFileStorage {
    type ConsumeError = StdFileStorageError;

    async fn remove(mut self) -> Result<(), Self::ConsumeError> {
        let backing_file_path = self.backing_file_path.clone();

        self.close().await?;

        tokio::fs::remove_file(&backing_file_path)
            .await
            .map_err(StdFileStorageError::IoError)
    }

    async fn close(mut self) -> Result<(), Self::ConsumeError> {
        self.writer
            .flush()
            .await
            .map_err(StdFileStorageError::IoError)?;

        drop(self.writer);

        let reader = Arc::<_>::into_inner(self.reader).ok_or(StdFileStorageError::StdFileInUse)?;

        tokio::task::spawn_blocking(|| drop(reader))
            .await
            .map_err(StdFileStorageError::JoinError)
    }
}

#[derive(Clone, Copy)]
pub struct StdFileStorageProvider;

#[cfg(target_family = "unix")]
pub mod unix {

    use super::*;
    use crate::storage::{impls::common::PathAddressedStorageProvider, Storage};
    use std::os::unix::prelude::FileExt;
    use tokio::io::AsyncWriteExt;

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

    #[async_trait(?Send)]
    impl PathAddressedStorageProvider<StdFileStorage> for StdFileStorageProvider {
        async fn obtain_storage<P: AsRef<Path>>(
            &self,
            path: P,
        ) -> Result<StdFileStorage, StdFileStorageError> {
            StdFileStorage::new(path).await
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
        async fn test_tokio_std_file_storage_read_append_truncate_consistency() {
            const TEST_TOKIO_STD_FILE_STORAGE_PATH: &str = "/tmp/laminarmq_test_tokio_std_file_storage_read_append_truncate_consistency.storage";

            if Path::new(TEST_TOKIO_STD_FILE_STORAGE_PATH).exists() {
                let path = TEST_TOKIO_STD_FILE_STORAGE_PATH;
                tokio::fs::remove_file(path).await.unwrap();
            }

            common::test::_test_storage_read_append_truncate_consistency(|| async {
                _TestStorage {
                    storage: StdFileStorage::new(TEST_TOKIO_STD_FILE_STORAGE_PATH)
                        .await
                        .unwrap(),
                    persistent: true,
                }
            })
            .await;
        }

        #[tokio::test]
        async fn test_tokio_std_file_store_read_append_truncate_consistency() {
            const TEST_TOKIO_STD_FILE_STORE_STORAGE_PATH: &str =
                "/tmp/laminarmq_test_tokio_std_file_store_read_append_truncate_consistency.store";

            if Path::new(TEST_TOKIO_STD_FILE_STORE_STORAGE_PATH).exists() {
                let path = TEST_TOKIO_STD_FILE_STORE_STORAGE_PATH;
                tokio::fs::remove_file(path).await.unwrap();
            }

            store::test::_test_store_read_append_truncate_consistency(|| async {
                (
                    _TestStorage {
                        storage: StdFileStorage::new(TEST_TOKIO_STD_FILE_STORE_STORAGE_PATH)
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
        async fn test_tokio_std_file_index_read_append_truncate_consistency() {
            const TEST_TOKIO_STD_FILE_INDEX_STORAGE_PATH: &str =
                "/tmp/laminarmq_test_tokio_std_file_index_read_append_truncate_consistency.index";

            if Path::new(TEST_TOKIO_STD_FILE_INDEX_STORAGE_PATH).exists() {
                let path = TEST_TOKIO_STD_FILE_INDEX_STORAGE_PATH;
                tokio::fs::remove_file(path).await.unwrap();
            }

            index::test::_test_index_read_append_truncate_consistency(|| async {
                (
                    _TestStorage {
                        storage: StdFileStorage::new(TEST_TOKIO_STD_FILE_INDEX_STORAGE_PATH)
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
        async fn test_tokio_std_file_segment_read_append_truncate_consistency() {
            const TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY: &str =
                "/tmp/laminarmq_test_tokio_std_file_segment_read_append_truncate_consistency";

            if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
                let directory_path = TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY;
                tokio::fs::remove_dir_all(directory_path).await.unwrap();
            }

            let disk_backed_storage_provider =
                DiskBackedSegmentStorageProvider::<_, _, u32>::with_storage_directory_path_and_provider(
                    TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY,
                    StdFileStorageProvider,
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
        async fn test_tokio_std_file_segmented_log_read_append_truncate_consistency() {
            const TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY: &str =
                "/tmp/laminarmq_test_tokio_std_file_segmented_log_read_append_truncate_consistency";

            if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
                let directory_path = TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY;
                tokio::fs::remove_dir_all(directory_path).await.unwrap();
            }

            let disk_backed_storage_provider =
                DiskBackedSegmentStorageProvider::<_, _, u32>::with_storage_directory_path_and_provider(
                    TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY,
                    StdFileStorageProvider,
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
        async fn test_tokio_std_file_segmented_log_remove_expired_segments() {
            const TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY: &str =
                "/tmp/laminarmq_test_tokio_std_file_segmented_log_remove_expired_segments";

            if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
                let directory_path = TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY;
                tokio::fs::remove_dir_all(directory_path).await.unwrap();
            }

            let disk_backed_storage_provider =
                DiskBackedSegmentStorageProvider::<_, _, u32>::with_storage_directory_path_and_provider(
                    TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY,
                    StdFileStorageProvider,
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
        async fn test_tokio_std_file_segmented_log_segment_index_caching() {
            const TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY: &str =
                "/tmp/laminarmq_test_tokio_std_file_segmented_log_segment_index_caching";

            if Path::new(TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY).exists() {
                let directory_path = TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY;
                tokio::fs::remove_dir_all(directory_path).await.unwrap();
            }

            let disk_backed_storage_provider =
                DiskBackedSegmentStorageProvider::<_, _, u32>::with_storage_directory_path_and_provider(
                    TEST_DISK_BACKED_STORAGE_PROVIDER_STORAGE_DIRECTORY,
                    StdFileStorageProvider,
                )
                .unwrap();

            segmented_log::test::_test_segmented_log_segment_index_caching(
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
    }
}
