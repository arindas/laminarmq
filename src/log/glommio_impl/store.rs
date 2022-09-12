use std::{cell::Ref, error::Error, fmt::Display, path::Path, rc::Rc, result::Result};

use async_trait::async_trait;
use futures_lite::AsyncWriteExt;

use glommio::{
    io::{
        CloseResult, DmaFile, DmaStreamReader, DmaStreamReaderBuilder, DmaStreamWriter,
        DmaStreamWriterBuilder, OpenOptions, ReadResult,
    },
    GlommioError,
};

use crate::log::store::common::{is_checksum_valid, RecordHeader, RECORD_HEADER_LENGTH};

#[derive(Debug)]
pub enum StoreError {
    SerializationError(std::io::Error),
    StorageError(GlommioError<()>),
    ChecksumError,
    NoBackingFileError,
}

impl Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::SerializationError(err) => {
                write!(f, "Error during record ser/deser-ialization: {}", err)
            }
            StoreError::StorageError(err) => {
                write!(f, "Error during Storage IO: {}", err)
            }
            StoreError::ChecksumError => {
                write!(f, "Checksum validation failed.")
            }
            StoreError::NoBackingFileError => {
                write!(f, "Store not backed by a file.")
            }
        }
    }
}

impl Error for StoreError {}

pub struct Store {
    reader: Rc<DmaFile>,
    writer: DmaStreamWriter,
    size: u64,
}

pub const DEFAULT_STORE_WRITER_BUFFER_SIZE: usize = 128 << 10;

impl Store {
    /// Creates a new [`Store`] from a [`glommio::io::DmaFile`]
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, StoreError> {
        Self::with_path_and_buffer_size(path, DEFAULT_STORE_WRITER_BUFFER_SIZE).await
    }

    pub async fn with_path_and_buffer_size<P: AsRef<Path>>(
        path: P,
        buffer_size: usize,
    ) -> Result<Self, StoreError> {
        let writer_dma_file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .dma_open(path.as_ref())
            .await
            .map_err(StoreError::StorageError)?;

        let initial_size = writer_dma_file
            .file_size()
            .await
            .map_err(StoreError::StorageError)?;

        Ok(Self {
            reader: Rc::new(
                DmaFile::open(path.as_ref())
                    .await
                    .map_err(StoreError::StorageError)?,
            ),
            writer: DmaStreamWriterBuilder::new(writer_dma_file)
                .with_buffer_size(buffer_size)
                .build(),
            size: initial_size,
        })
    }

    /// Returns the stream reader of this [`Store`].
    pub fn stream_reader(&self) -> DmaStreamReader {
        DmaStreamReaderBuilder::from_rc(self.reader.clone()).build()
    }
}

#[async_trait(?Send)]
impl crate::log::store::Store<ReadResult> for Store {
    type CloseResult = CloseResult;
    type Error = StoreError;

    async fn append(&mut self, record_bytes: &[u8]) -> Result<(u64, usize), Self::Error> {
        let record_header = RecordHeader::from_record_bytes(record_bytes);
        let record_header_bytes = record_header
            .as_bytes()
            .map_err(StoreError::SerializationError)?;

        let current_position = self.size;

        self.writer
            .write_all(&record_header_bytes)
            .await
            .map_err(|x| StoreError::StorageError(GlommioError::from(x)))?;
        self.writer
            .write_all(&record_bytes)
            .await
            .map_err(|x| StoreError::StorageError(GlommioError::from(x)))?;

        let bytes_written = record_header_bytes.len() + record_bytes.len();
        self.size += bytes_written as u64;

        Ok((current_position, bytes_written))
    }

    async fn read(&self, position: u64) -> Result<ReadResult, Self::Error> {
        let record_header_bytes = self
            .reader
            .read_at(position, RECORD_HEADER_LENGTH)
            .await
            .map_err(StoreError::StorageError)?;
        let record_header = RecordHeader::from_bytes(&record_header_bytes)
            .map_err(StoreError::SerializationError)?;

        let record_bytes = self
            .reader
            .read_at(
                position + RECORD_HEADER_LENGTH as u64,
                record_header.length as usize,
            )
            .await
            .map_err(StoreError::StorageError)?;

        if !is_checksum_valid(&record_bytes, record_header.checksum) {
            return Err(StoreError::ChecksumError);
        }

        Ok(record_bytes)
    }

    async fn close(mut self) -> Result<Self::CloseResult, Self::Error> {
        self.writer
            .close()
            .await
            .map_err(|x| StoreError::StorageError(GlommioError::from(x)))?;
        self.reader
            .close_rc()
            .await
            .map_err(StoreError::StorageError)
    }

    #[inline]
    fn size(&self) -> u64 {
        self.size
    }

    /// Returns the path to the underlying file.
    /// If no path could be found, we return [`StoreError::NoBackingFileError`].
    fn path(&self) -> Result<Ref<Path>, Self::Error> {
        self.reader.path().ok_or(StoreError::NoBackingFileError)
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, path::PathBuf};

    use glommio::{
        io::{remove, CloseResult},
        LocalExecutorBuilder, Placement,
    };

    use super::{Store, StoreError};
    use crate::log::{
        store::{
            common::{RECORD_HEADER_LENGTH, STORE_FILE_EXTENSION},
            Store as BaseStore, StoreScanner,
        },
        Scanner,
    };

    #[inline]
    fn test_file_path_string(test_name: &str) -> String {
        format!(
            "/tmp/laminarmq_test_log_store_{}{}",
            test_name, STORE_FILE_EXTENSION
        )
    }

    #[test]
    fn test_store_new_and_close() {
        let test_file_path = PathBuf::from(test_file_path_string("test_store_new_and_close"));

        let local_ex = LocalExecutorBuilder::new(Placement::Fixed(1))
            .spawn(move || async move {
                let store = Store::new(test_file_path.clone()).await.unwrap();

                assert_eq!(store.path().unwrap().deref(), test_file_path.clone());

                matches!(store.close().await.unwrap(), CloseResult::Closed);
                remove(test_file_path).await.unwrap();
            })
            .unwrap();
        local_ex.join().unwrap();
    }

    #[test]
    fn test_store_reads_reflect_writes() {
        const FIRST_RECORD: &[u8] = b"Hello World!";

        const RECORDS: [&[u8; 129]; 20] = [
                    b"T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77t",
                    b"9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9YxuipjdD",
                    b"zjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMsW",
                    b"9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqcw",
                    b"ZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7Xco",
                    b"9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nOi",
                    b"KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3A",
                    b"cJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykL",
                    b"6BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL7O",
                    b"h5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKe",
                    b"DNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0Npc8",
                    b"6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOhu",
                    b"0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO6",
                    b"BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1V",
                    b"VWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcX",
                    b"RaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVc",
                    b"ujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7PE",
                    b"6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs9",
                    b"28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcvW",
                    b"j9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSD",
                ];

        const WRITE_BUFFER_SIZE: usize = 512;

        const EXPECTED_STORAGE_SIZE: usize = (RECORD_HEADER_LENGTH + FIRST_RECORD.len())
            + (RECORDS.len()) * (RECORD_HEADER_LENGTH + RECORDS[0].len());

        let test_file_path =
            PathBuf::from(test_file_path_string("test_store_reads_reflect_writes"));

        if test_file_path.exists() {
            std::fs::remove_file(test_file_path.clone()).unwrap();
        }

        let local_ex = LocalExecutorBuilder::new(Placement::Fixed(1))
            .spawn(move || async move {
                let mut store =
                    Store::with_path_and_buffer_size(test_file_path.clone(), WRITE_BUFFER_SIZE)
                        .await
                        .unwrap();

                // read on empty store should result in storage error
                matches!(store.read(0).await, Err(StoreError::StorageError(_)));

                // read on arbitrary high position should end in storage error
                matches!(store.read(68419).await, Err(StoreError::StorageError(_)));

                // append a record
                let first_record_bytes = FIRST_RECORD;
                let expected_num_bytes_written =
                    super::RECORD_HEADER_LENGTH + first_record_bytes.len();

                let (position, num_bytes_written) = store.append(first_record_bytes).await.unwrap();
                assert_eq!(position, 0);
                assert_eq!(num_bytes_written, expected_num_bytes_written);

                // not enough bytes written to trigger flush
                matches!(store.read(0).await, Err(StoreError::StorageError(_)));

                let mut record_positions_and_sizes = Vec::with_capacity(RECORDS.len());

                for &record in RECORDS {
                    record_positions_and_sizes.push(store.append(&record).await.unwrap());
                }

                let mut read_records = Vec::with_capacity(RECORDS.len());

                for (position, _written_bytes) in &record_positions_and_sizes[0..RECORDS.len() / 2]
                {
                    read_records.push(store.read(position.clone()).await.unwrap());
                }

                for i in 0..RECORDS.len() / 2 {
                    assert_eq!(read_records[i].deref(), RECORDS[i]);
                }

                let mut i = 0;

                let mut scanner = StoreScanner::new(&store);
                while let Some(record) = scanner.next().await {
                    if i == 0 {
                        assert_eq!(record.deref(), first_record_bytes)
                    } else {
                        assert_eq!(record.deref(), RECORDS[i - 1]);
                    }
                    i += 1;
                }

                assert_eq!(store.size() as usize, EXPECTED_STORAGE_SIZE);

                // close store to sync data
                matches!(store.close().await.unwrap(), CloseResult::Closed);

                // reopen store and check stored records to test durability
                let store =
                    Store::with_path_and_buffer_size(test_file_path.clone(), WRITE_BUFFER_SIZE)
                        .await
                        .unwrap();

                assert_eq!(store.size() as usize, EXPECTED_STORAGE_SIZE);

                let mut i = 0;

                let mut scanner = StoreScanner::new(&store);
                while let Some(record) = scanner.next().await {
                    if i == 0 {
                        assert_eq!(record.deref(), first_record_bytes)
                    } else {
                        assert_eq!(record.deref(), RECORDS[i - 1]);
                    }
                    i += 1;
                }
                assert_eq!(i, RECORDS.len() + 1);
                matches!(store.close().await.unwrap(), CloseResult::Closed);

                remove(test_file_path).await.unwrap();
            })
            .unwrap();
        local_ex.join().unwrap();
    }
}
