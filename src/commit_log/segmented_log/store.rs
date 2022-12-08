//! Module providing the [`Store`] abstraction.
//!
//! A store acts as the backing storage for segments in a segmented log. It is the fundamental
//! storage component providing access to persistence. This module only provides generic traits
//! representing stores. Users will require to implement the traits provided for their specific
//! async runtime and storage media.

use async_trait::async_trait;
use std::{cell::Ref, marker::PhantomData, ops::Deref, path::Path, result::Result};

use super::super::Scanner;

/// Trait representing a collection of record backed by some form of persistent storage.
///
/// This trait is generic over the kind of records that can be stored, provided that the can be
/// dereferenced as bytes. In the context of a segmented log, they act as the backing storage for
/// a "segment".
///
/// This trait's contract assumes the following with regard to how records are laid out in
/// storage:
/// ```text
/// ┌─────────────────────────┬────────────────────────┬───────────────────────┐
/// │ crc32_checksum: [u8; 4] │ record_length: [u8; 4] │ record_bytes: [u8; _] │
/// └─────────────────────────┴────────────────────────┴───────────────────────┘
/// ```
/// As such if the record header is invalidated due to checksum mismatch or record length
/// mismatch an appropriate error is expected on the associated operation.
#[async_trait(?Send)]
pub trait Store<T>
where
    T: Deref<Target = [u8]>,
{
    /// The error type used by the methods of this trait.
    type Error: std::error::Error;

    /// Appends a record containing the given record bytes at the end of this store.
    async fn append(&mut self, record_bytes: &[u8]) -> Result<(u64, usize), Self::Error> {
        let record_parts: [&[u8]; 1] = [record_bytes];
        let record_parts: &[&[u8]] = &record_parts;

        self.append_multipart(record_parts).await
    }

    /// Appends a record split in multiple parts as a single record at the end of this store.
    async fn append_multipart(
        &mut self,
        record_bytes: &[&[u8]],
    ) -> Result<(u64, usize), Self::Error>;

    /// Reads the record stored at the given position in the [`Store`] along with the position
    /// of the next record.
    async fn read(&self, position: u64) -> Result<(T, u64), Self::Error> {
        let (_, record_bytes, next_position) = self.read_with_metadata(position, 0).await?;

        Ok((record_bytes, next_position))
    }

    async fn read_with_metadata(
        &self,
        position: u64,
        metadata_size: usize,
    ) -> Result<(T, T, u64), Self::Error>;

    /// Truncates this [`Store`] instance by removing all records after the given position
    /// and makes the given position the size of this store.
    /// Returns a suitable error if this position is not a valid record position.
    async fn truncate(&mut self, position: u64) -> Result<(), Self::Error>;

    /// Closes this record. Consumes this record instance to stop further operations on this
    /// closed store instance.
    async fn close(self) -> Result<(), Self::Error>;

    /// Removes the file associated with this store from persistent media. Consumes this
    /// record to stop further operations on this "removed" store instance.
    async fn remove(self) -> Result<(), Self::Error>;

    /// Number of bytes stored in/with this store instance.
    fn size(&self) -> u64;

    /// Returns the path to the underlying file used for storage.
    fn path(&self) -> Result<Ref<Path>, Self::Error>;
}

/// [`Scanner`](super::Scanner) implementation for a [`Store`].
pub struct StoreScanner<'a, Record, S>
where
    Record: Deref<Target = [u8]>,
    S: Store<Record>,
{
    store: &'a S,
    position: u64,

    _phantom_data: PhantomData<Record>,
}

impl<'a, Record, S> StoreScanner<'a, Record, S>
where
    Record: Deref<Target = [u8]>,
    S: Store<Record>,
{
    /// Creates a store scanner instance which reads records starting from the given position.
    pub fn with_position(store: &'a S, position: u64) -> Self {
        Self {
            store,
            position,
            _phantom_data: PhantomData,
        }
    }

    /// Creates a store scanner instance which reads records from the start.
    pub fn new(store: &'a S) -> Self {
        Self::with_position(store, 0)
    }
}

#[async_trait(?Send)]
impl<'a, Record, S> Scanner for StoreScanner<'a, Record, S>
where
    Record: Deref<Target = [u8]> + Unpin,
    S: Store<Record>,
{
    type Item = Record;

    async fn next(&mut self) -> Option<Self::Item> {
        self.store
            .read(self.position)
            .await
            .ok()
            .map(|(record, next_position)| {
                self.position = next_position;
                record
            })
    }
}

pub mod common {
    //! Module providing common utilities to be used by all implementations of
    //! [`Store`](super::Store).

    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

    /// Extension used by backing files for [`Store`](super::Store)s.
    pub const STORE_FILE_EXTENSION: &str = "store";

    /// Number of bytes required for storing the record header.
    pub const RECORD_HEADER_LENGTH: usize = 8;

    /// Header for a record stored in [`Store`](super::Store).
    /// ```text
    /// ┌─────────────────────────┬────────────────────────┬───────────────────────┐
    /// │ crc32_checksum: [u8; 4] │ record_length: [u8; 4] │ record_bytes: [u8; _] │
    /// └─────────────────────────┴────────────────────────┴───────────────────────┘
    /// │─────────────── RecordHeader ─────────────────────│
    /// ```
    #[derive(Debug)]
    pub struct RecordHeader {
        /// checksum computed from the bytes in the record.
        pub checksum: u32,

        /// Length of the record.
        pub length: u32,
    }

    /// Returns the number of bytes needed to store the given record with binary encoding in a
    /// [`Store`](super::Store) implementation.
    /// This method returns a `Some(record_size)` if the size could be estimated correctly. If
    /// there is any error if ascertaining the serialized size, None is returned.
    pub fn bincoded_serialized_record_size<Record: serde::Serialize>(
        record: &Record,
    ) -> Option<u64> {
        bincode::serialized_size(record)
            .ok()
            .map(|x| x + RECORD_HEADER_LENGTH as u64)
    }

    impl RecordHeader {
        pub fn from_record_parts(record_parts: &[&[u8]]) -> Self {
            let mut hasher = crc32fast::Hasher::new();
            let mut length = 0;

            for part in record_parts {
                hasher.update(part);
                length += part.len() as u32;
            }

            Self {
                checksum: hasher.finalize(),
                length,
            }
        }

        /// Creates a new [`RecordHeader`] instance from the given record bytes. This function
        /// computes the checksum for the given byte slice and returns a [`RecordHeader`]
        /// instance with the checksum and the length of the given byte slice.
        pub fn from_record_bytes(record_bytes: &[u8]) -> Self {
            Self {
                checksum: crc32fast::hash(record_bytes),
                length: record_bytes.len() as u32,
            }
        }

        /// Creates a [`RecordHeader`] instance from serialized record header bytes.
        /// This method internally users a [`std::io::Cursor`] to read the checksum and
        /// length as integers from the given bytes with little endian encoding.
        pub fn from_bytes(record_header_bytes: &[u8]) -> std::io::Result<Self> {
            let mut cursor = std::io::Cursor::new(record_header_bytes);

            let checksum = cursor.read_u32::<LittleEndian>()?;
            let length = cursor.read_u32::<LittleEndian>()?;

            if checksum == 0 && length == 0 {
                Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))
            } else {
                Ok(Self { checksum, length })
            }
        }

        /// Serializes this given record header to an owned byte array.
        /// This method internally use a [`std::io::Cursor`] to write the checksum and length
        /// fields as little endian integers into the byte array.
        pub fn as_bytes(self) -> std::io::Result<[u8; RECORD_HEADER_LENGTH]> {
            let mut bytes = [0; RECORD_HEADER_LENGTH];

            let buffer: &mut [u8] = &mut bytes;
            let mut cursor = std::io::Cursor::new(buffer);

            cursor.write_u32::<LittleEndian>(self.checksum)?;
            cursor.write_u32::<LittleEndian>(self.length)?;

            Ok(bytes)
        }

        /// States whether this [`RecordHeader`] is valid for the given record bytes. This
        /// method internally checks if the checksum and length is valid for the given slice.
        #[inline]
        pub fn valid_for_record_bytes(&self, record_bytes: &[u8]) -> bool {
            self.length as usize == record_bytes.len()
                && self.checksum == crc32fast::hash(record_bytes)
        }
    }
}
