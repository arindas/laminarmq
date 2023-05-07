//! Module providing [`Store`](crate::commit_log::segmented_log::store::Store)
//! implementation for the [`glommio`] runtime.

use std::{
    cell::Ref, error::Error, fmt::Display, iter::Extend, marker::Unpin, ops::Deref, path::Path,
};

use async_trait::async_trait;
use futures_core::Stream;
use futures_lite::{AsyncWriteExt, StreamExt};

use glommio::{
    io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions, ReadResult},
    GlommioError,
};

use crate::commit_log::segmented_log::store::common::{RecordHeader, RECORD_HEADER_LENGTH};

/// Error type used by [`Store`].
#[derive(Debug)]
pub enum StoreError {
    SerializationError(std::io::Error),
    StorageError(GlommioError<()>),
    InvalidReadPosition(u64),
    InvalidRecordHeader,
    NoBackingFileError,
}

impl Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::SerializationError(err) => {
                write!(f, "Error during record ser/deser-ialization: {err}")
            }
            StoreError::StorageError(err) => {
                write!(f, "Error during Storage IO: {err}")
            }
            StoreError::InvalidRecordHeader => {
                write!(f, "Checksum or record length mismatch.")
            }
            StoreError::NoBackingFileError => {
                write!(f, "Store not backed by a file.")
            }
            StoreError::InvalidReadPosition(pos) => {
                write!(f, "Not a valid read position: {pos:?}")
            }
        }
    }
}

impl Error for StoreError {}

fn record_positions_in<P: AsRef<Path>>(store: P) -> impl Stream<Item = Result<u64, StoreError>> {
    async_stream::stream! {
        let store = DmaFile::open(store.as_ref())
            .await
            .map_err(StoreError::StorageError)?;

        let store_size = store
            .file_size()
            .await
            .map_err(StoreError::StorageError)?;

        let mut position = 0;

        while position < store_size {
            let record_header_bytes = store
                .read_at(position, RECORD_HEADER_LENGTH)
                .await
                .map_err(StoreError::StorageError)?;

            let record_header = RecordHeader::from_bytes(&record_header_bytes)
                .map_err(StoreError::SerializationError)?;

            yield Ok(position);

            position += record_header_bytes.len() as u64 + record_header.length as u64;
        }

        store.close().await.map_err(StoreError::StorageError)?;
    }
}

async fn record_positions<P, T>(store: P) -> Result<T, StoreError>
where
    P: AsRef<Path>,
    T: Default + Extend<u64>,
{
    let record_positions = record_positions_in(store);
    futures_util::pin_mut!(record_positions);
    record_positions.try_collect().await
}

async fn write_byte_slices<'a, I, W>(writer: &mut W, byte_slices: I) -> std::io::Result<()>
where
    W: AsyncWriteExt + Unpin,
    I: Iterator<Item = &'a &'a [u8]>,
{
    for byte_slice in byte_slices {
        writer.write_all(byte_slice).await?;
    }

    Ok(())
}

fn stream_writer_with_buffer_size(writer: DmaFile, buffer_size: usize) -> DmaStreamWriter {
    DmaStreamWriterBuilder::new(writer)
        .with_buffer_size(buffer_size)
        .build()
}

/// [`crate::commit_log::segmented_log::store::Store`] implementation for the [`glommio`] runtime.
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
    record_positions: Vec<u64>,
    buffer_size: usize,
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

    async fn backing_writer<P: AsRef<Path>>(path: P) -> Result<DmaFile, StoreError> {
        OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .dma_open(path.as_ref())
            .await
            .map_err(StoreError::StorageError)
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
        let writer = Self::backing_writer(path.as_ref()).await?;

        let initial_size = writer.file_size().await.map_err(StoreError::StorageError)?;

        let reader = DmaFile::open(path.as_ref())
            .await
            .map_err(StoreError::StorageError)?;

        Ok(Self {
            reader,
            writer: stream_writer_with_buffer_size(writer, buffer_size),
            record_positions: record_positions(path).await?,
            size: initial_size,
            buffer_size,
        })
    }

    fn record_index(&self, position: &u64) -> Result<usize, StoreError> {
        self.record_positions
            .binary_search(position)
            .map_err(|_| StoreError::InvalidReadPosition(*position))
    }

    #[tracing::instrument]
    async fn truncate_to(&mut self, position: u64) -> Result<(), StoreError> {
        let backing_file_path = self.reader.path().ok_or(StoreError::NoBackingFileError)?;

        let record_index = self.record_index(&position)?;
        self.record_positions.truncate(record_index);

        // avoid propagating error, to avoid leaving this [`Store`] instance
        // in an inconsitent state
        if let Err(error) = self.writer.close().await {
            tracing::error!("error closing old writer: {:?}", error);
        }

        let writer = Self::backing_writer(backing_file_path.deref()).await?;

        writer
            .truncate(position)
            .await
            .map_err(StoreError::StorageError)?;

        self.writer = stream_writer_with_buffer_size(writer, self.buffer_size);

        let reader = DmaFile::open(backing_file_path.deref())
            .await
            .map_err(StoreError::StorageError)?;

        drop(backing_file_path);

        self.reader = reader;

        self.size = position;
        Ok(())
    }
}

#[async_trait(?Send)]
impl crate::commit_log::segmented_log::store::Store<ReadResult> for Store {
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
    async fn append_multipart(
        &mut self,
        record_parts: &[&[u8]],
    ) -> Result<(u64, usize), StoreError> {
        let record_header = RecordHeader::from_record_parts(record_parts);
        let record_length = record_header.length as usize + RECORD_HEADER_LENGTH;
        let record_header_bytes = record_header
            .as_bytes()
            .map_err(StoreError::SerializationError)?;
        let record_header_bytes_ref: &[u8] = &record_header_bytes;

        let record_parts = std::iter::once(&record_header_bytes_ref).chain(record_parts.iter());

        let current_position = self.size;

        if let Err(error) = write_byte_slices(&mut self.writer, record_parts).await {
            self.truncate_to(current_position).await?;
            Err(StoreError::StorageError(GlommioError::from(error)))
        } else {
            self.record_positions.push(current_position);
            self.size += record_length as u64;
            Ok((current_position, record_length))
        }
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
        let _index = self.record_index(&position)?;

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

    async fn truncate(&mut self, position: u64) -> Result<(), Self::Error> {
        self.truncate_to(position).await
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

    use futures_util::{pin_mut, StreamExt};
    use glommio::{LocalExecutorBuilder, Placement};

    use super::{Store, StoreError};
    use crate::commit_log::segmented_log::store::{
        common::{RECORD_HEADER_LENGTH, STORE_FILE_EXTENSION},
        store_record_stream, Store as BaseStore,
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
                let store = Store::new(&test_file_path).await.unwrap();

                assert_eq!(store.path().unwrap().deref(), test_file_path);

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
            std::fs::remove_file(&test_file_path).unwrap();
        }

        let local_ex = LocalExecutorBuilder::new(Placement::Fixed(1))
            .spawn(move || async move {
                let mut store =
                    Store::with_path_and_buffer_size(&test_file_path, WRITE_BUFFER_SIZE)
                        .await
                        .unwrap();

                // read() on empty store should result in an invalid read position
                assert!(matches!(
                    store.read(0).await,
                    Err(StoreError::InvalidReadPosition(_))
                ));
                assert!(matches!(
                    store.read(68419).await, // We were this close to greatness!
                    Err(StoreError::InvalidReadPosition(_))
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
                    let (position, bytes_written) = store.append(&record).await.unwrap();
                    assert!(store.record_index(&position).is_ok());
                    record_positions_and_sizes.push((position, bytes_written));
                }

                let mut records_read_before_sync = Vec::with_capacity(RECORDS.len());

                for (position, _written_bytes) in &record_positions_and_sizes {
                    if let Ok((record, _next_record_position)) = store.read(*position).await {
                        records_read_before_sync.push(record);
                    } else {
                        break;
                    }
                }

                for i in 0..records_read_before_sync.len() {
                    assert_eq!(records_read_before_sync[i].deref(), RECORDS[i]);
                }

                {
                    let mut i = 0;

                    let record_stream = store_record_stream(&store, 0);
                    pin_mut!(record_stream);

                    while let Some(record) = record_stream.next().await {
                        if i == 0 {
                            assert_eq!(record.deref(), first_record_bytes)
                        } else {
                            assert_eq!(record.deref(), RECORDS[i - 1]);
                        }
                        i += 1;
                    }
                }

                assert_eq!(store.size() as usize, EXPECTED_STORAGE_SIZE);

                // close store to sync data
                store.close().await.unwrap();

                // reopen store and check stored records to test durability
                let mut store =
                    Store::with_path_and_buffer_size(&test_file_path, WRITE_BUFFER_SIZE)
                        .await
                        .unwrap();

                assert_eq!(store.size() as usize, EXPECTED_STORAGE_SIZE);

                assert!(matches!(
                    store.read(store.size()).await,
                    Err(StoreError::InvalidReadPosition(_))
                ));

                {
                    let mut i = 0;

                    let record_stream = store_record_stream(&store, 0);
                    pin_mut!(record_stream);

                    while let Some(record) = record_stream.next().await {
                        if i == 0 {
                            assert_eq!(record.deref(), first_record_bytes)
                        } else {
                            assert_eq!(record.deref(), RECORDS[i - 1]);
                        }
                        i += 1;
                    }
                    assert_eq!(i, RECORDS.len() + 1);
                }

                let trunate_index = record_positions_and_sizes.len() / 2;

                let (truncate_position, _) = record_positions_and_sizes[trunate_index];

                assert!(matches!(
                    store.truncate(truncate_position + 1).await,
                    Err(StoreError::InvalidReadPosition(_))
                ));

                store.truncate(truncate_position).await.unwrap();

                assert_eq!(store.size(), truncate_position);

                assert!(matches!(
                    store.read(store.size()).await,
                    Err(StoreError::InvalidReadPosition(_))
                ));

                {
                    let mut i = 0;

                    let record_stream = store_record_stream(&store, 0);
                    pin_mut!(record_stream);

                    while let Some(record) = record_stream.next().await {
                        if i == 0 {
                            assert_eq!(record.deref(), first_record_bytes)
                        } else {
                            assert_eq!(record.deref(), RECORDS[i - 1]);
                        }
                        i += 1;
                    }
                    assert_eq!(i, trunate_index + 1); // account for the first "Hello World!" record.
                }

                store.remove().await.unwrap();
            })
            .unwrap();
        local_ex.join().unwrap();
    }
}
