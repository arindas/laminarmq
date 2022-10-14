//! Module providing [`Store`](crate::commit_log::store::Store) implementation for the [`glommio`]
//! runtime.

use std::{cell::Ref, error::Error, fmt::Display, path::Path, result::Result};

use async_trait::async_trait;
use futures_lite::AsyncWriteExt;

use glommio::{
    io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions, ReadResult},
    GlommioError,
};

use crate::commit_log::store::common::{RecordHeader, RECORD_HEADER_LENGTH};

/// Error type used by [`Store`].
#[derive(Debug)]
pub enum StoreError {
    SerializationError(std::io::Error),
    StorageError(GlommioError<()>),
    InvalidRecordHeader,
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
            StoreError::InvalidRecordHeader => {
                write!(f, "Checksum or record length mismatch.")
            }
            StoreError::NoBackingFileError => {
                write!(f, "Store not backed by a file.")
            }
        }
    }
}

impl Error for StoreError {}

/// [`crate::commit_log::store::Store`] implementation for the [`glommio`] runtime.
///
/// This implementation uses directly mapped files to leverage `io_uring` powered operations. A
/// [`Store`] consists of a [`DmaFile`] reader and [`DmaStreamWriter`], both of which point to the
/// same underlying file on the disk.
///
/// We have chosen a simple [`DmaFile`] for the reader since the read amplification caused due to
/// random unbuffered reads is manageable. However, write amplifications are generally more costly.
/// Since we always append to the end of the file, [`DmaStreamWriter`] is a better solution which
/// enables use to have the best tradeoff of responsiveness v/s buffering.
#[derive(Debug)]
pub struct Store {
    reader: DmaFile,
    writer: DmaStreamWriter,
    size: u64,
}

/// Default buffer size used by the [`DmaStreamWriter`] in [`Store`] when created with
/// [`Store::new`].
pub const DEFAULT_STORE_WRITER_BUFFER_SIZE: usize = 128 << 10;

impl Store {
    /// Creates a new [`Store`] from a [`glommio::io::DmaFile`]
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, StoreError> {
        Self::with_path_and_buffer_size(path, DEFAULT_STORE_WRITER_BUFFER_SIZE).await
    }

    /// Creates a new [`Store`] instance.
    ///
    /// The writer is opened with "write", "append" and "create" flags, while the reader is opened
    /// with only the "read" flag. The given `buffer_size` is used to configure the buffer size of
    /// the [`DmaStreamWriter`].
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
            reader: DmaFile::open(path.as_ref())
                .await
                .map_err(StoreError::StorageError)?,
            writer: DmaStreamWriterBuilder::new(writer_dma_file)
                .with_buffer_size(buffer_size)
                .build(),
            size: initial_size,
        })
    }
}

#[async_trait(?Send)]
impl crate::commit_log::store::Store<ReadResult> for Store {
    type Error = StoreError;

    /// Appends the given record bytes at the end of this store.
    ///
    /// ## Returns
    /// A tuple with `(position_where_the_record_was_written, number_of_bytes_written)`
    ///
    /// ## Errors
    /// - [`StoreError::SerializationError`]: if there was an error during serializing the record
    /// header for the given record bytes to bytes
    /// - [`StoreError::StorageError`]: if there was an error during writing to the underlying
    /// [`DmaStreamWriter`] instance.
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
            .write_all(record_bytes)
            .await
            .map_err(|x| StoreError::StorageError(GlommioError::from(x)))?;

        let bytes_written = record_header_bytes.len() + record_bytes.len();
        self.size += bytes_written as u64;

        Ok((current_position, bytes_written))
    }

    /// Reads the record at the given position.
    ///
    /// ## Returns
    /// A tuple containing;
    /// - [`ReadResult`] containing the bytes of the desired record.
    /// - An [`u64`] containing the position of the next record.
    ///
    /// ## Errors
    /// - [`StoreError::SerializationError`]: if there was an error during deserializing the
    /// recordy header from the first [`RECORD_HEADER_LENGTH`] bytes at the given `position`.
    /// - [`StoreError::StorageError`]: if there was an error in reading from the underlying
    /// [`DmaFile`].
    /// - [`StoreError::InvalidRecordHeader`]: if the record header is invalid for the bytes in the
    /// [`ReadResult`] instance read from the underlying [`DmaFile`]. (Checksum mismatch or invalid
    /// record length).
    async fn read(&self, position: u64) -> Result<(ReadResult, u64), Self::Error> {
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

        if !record_header.valid_for_record_bytes(&record_bytes) {
            return Err(StoreError::InvalidRecordHeader);
        }

        let bytes_read = (record_header_bytes.len() + record_bytes.len()) as u64;

        Ok((record_bytes, position + bytes_read))
    }

    /// Closes this [`Store`] instance.
    ///
    /// ## Errors
    /// - [`StoreError::StorageError`] if there is an error in closing the reader or writer.
    async fn close(mut self) -> Result<(), Self::Error> {
        self.writer
            .close()
            .await
            .map_err(|x| StoreError::StorageError(GlommioError::from(x)))?;
        self.reader.close().await.map_err(StoreError::StorageError)
    }

    #[inline]
    fn size(&self) -> u64 {
        self.size
    }

    /// Returns the path to the underlying file.
    ///
    /// ## Errors
    /// - [`StoreError::NoBackingFileError`]: if the reader's backing file's path couldn't be found
    fn path(&self) -> Result<Ref<Path>, Self::Error> {
        self.reader.path().ok_or(StoreError::NoBackingFileError)
    }

    /// Removes the files associated with this [`Store`] instance.
    ///
    /// First this method closes this instance. Then this method invokes [`glommio::io::remove`] on
    /// the underlying file's path.
    ///
    /// ## Errors
    /// - [StoreError::StorageError]: if there is an error during closing this instance or removing
    /// the files associated with this instance.
    async fn remove(self) -> Result<(), Self::Error> {
        let store_file_path = self.path()?.to_path_buf();

        self.close().await?;

        glommio::io::remove(store_file_path)
            .await
            .map_err(StoreError::StorageError)
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, path::PathBuf};

    use glommio::{LocalExecutorBuilder, Placement};

    use super::{Store, StoreError};
    use crate::commit_log::{
        store::{
            common::{RECORD_HEADER_LENGTH, STORE_FILE_EXTENSION},
            Store as BaseStore, StoreScanner,
        },
        Scanner,
    };

    #[inline]
    fn test_file_path_string(test_name: &str) -> String {
        format!(
            "/tmp/laminarmq_test_log_store_{}.{}",
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

                store.remove().await.unwrap();
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
                assert!(matches!(
                    store.read(0).await,
                    Err(StoreError::SerializationError(_))
                ));

                // read on arbitrary high position should end in storage error
                assert!(matches!(
                    store.read(68419).await,
                    Err(StoreError::SerializationError(_))
                ));

                // append a record
                let first_record_bytes = FIRST_RECORD;
                let expected_num_bytes_written =
                    super::RECORD_HEADER_LENGTH + first_record_bytes.len();

                let (position, num_bytes_written) = store.append(first_record_bytes).await.unwrap();
                assert_eq!(position, 0);
                assert_eq!(num_bytes_written, expected_num_bytes_written);

                // not enough bytes written to trigger flush
                assert!(matches!(
                    store.read(0).await,
                    Err(StoreError::SerializationError(_))
                ));

                let mut record_positions_and_sizes = Vec::with_capacity(RECORDS.len());

                for &record in RECORDS {
                    record_positions_and_sizes.push(store.append(&record).await.unwrap());
                }

                let mut records_read_before_sync = Vec::with_capacity(RECORDS.len());

                for (position, _written_bytes) in &record_positions_and_sizes {
                    if let Ok((record, _next_record_offset)) = store.read(position.clone()).await {
                        records_read_before_sync.push(record);
                    } else {
                        break;
                    }
                }

                for i in 0..records_read_before_sync.len() {
                    assert_eq!(records_read_before_sync[i].deref(), RECORDS[i]);
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
                store.close().await.unwrap();

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
                store.remove().await.unwrap();
            })
            .unwrap();
        local_ex.join().unwrap();
    }
}
