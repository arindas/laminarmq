//! Module providing a "commit log" abstraction.
//!
//! This kind of data structure is primarily useful for storing a series of "events". This module
//! provides the abstractions necessary for manipulating this data structure asynchronously on
//! persistent storage media. The abstractions provided by this module are generic over different
//! async runtimes and storage media. Hence user's will need to choose their specializations and
//! implementations of these generic abstractions as necessary.
//!
//! Also see: [`glommio_impl`].
//!
//! In the context of `laminarmq` this module is intended to provide the storage for individual
//! partitions in a topic.

use async_trait::async_trait;

/// Represents a record in a [`CommitLog`].
#[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq, Clone)]
pub struct Record {
    /// Value stored in this record entry. The value itself might be serialized bytes of some other
    /// form of record.
    pub value: Vec<u8>,

    /// Offset at which this record is stored in the log.
    pub offset: u64,
}

/// Abstraction for representing all types that can asynchronously linearly scanned for items.
#[async_trait(?Send)]
pub trait Scanner {
    /// The items scanned out by this scanner.
    type Item;

    /// Returns the next item in this scanner asynchronously. A `None` value indicates that no items
    /// are left to be scanned.
    async fn next(&mut self) -> Option<Self::Item>;
}

pub mod store {
    //! Module providing the [`Store`] abstraction.
    //!
    //! A store acts as the backing storage for segments in a segmented log. It is the fundamental
    //! storage component providing access to persistence. This module only provides generic traits
    //! representing stores. Users will require to implement the traits provided for their specific
    //! async runtime and storage media.

    use async_trait::async_trait;
    use std::{cell::Ref, marker::PhantomData, ops::Deref, path::Path, result::Result};

    use super::Scanner;

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
    pub trait Store<Record>
    where
        Record: Deref<Target = [u8]>,
    {
        /// The error type used by the methods of this trait.
        type Error: std::error::Error;

        /// Appends a record containing the given record bytes at the end of this store.
        async fn append(&mut self, record_bytes: &[u8]) -> Result<(u64, usize), Self::Error>;

        /// Reads the record stored at the given position in the [`Store`] along with the position
        /// of the next record.
        async fn read(&self, position: u64) -> Result<(Record, u64), Self::Error>;

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
}

pub mod segment {
    //! Module providing cross platform [`Segment`](Segment) abstractions for reading and writing
    //! [`Record`](super::Record) instances from [`Store`](super::store::Store) instances.

    use std::{fmt::Display, marker::PhantomData, ops::Deref, time::Instant};

    use async_trait::async_trait;

    use super::{
        store::{Store, StoreScanner},
        Record, Scanner,
    };

    /// Error type used for operations on a [`Segment`].
    #[derive(Debug)]
    pub enum SegmentError<T, S: Store<T>>
    where
        T: Deref<Target = [u8]>,
    {
        /// Error caused during an operation on the [`Segment`]'s underlying
        /// [`Store`](super::store::Store).
        StoreError(S::Error),

        /// The current segment has no more space for writing records.
        SegmentMaxed,

        /// Error when ser/deser-ializing a record.
        SerializationError,

        /// Offset used for reading or writing to a segment is beyond that store's allowed capacity
        /// by the [`config::SegmentConfig::max_store_bytes] limit.
        OffsetBeyondCapacity,

        /// Offset is out of bounds of the written region in this segment.
        OffsetOutOfBounds,
    }

    impl<T: Deref<Target = [u8]>, S: Store<T>> Display for SegmentError<T, S> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                SegmentError::StoreError(err) => {
                    write!(f, "Error during Store operation: {:?}", err)
                }
                SegmentError::SerializationError => {
                    write!(f, "Error occured during ser/deser-ializing a record.")
                }
                SegmentError::OffsetBeyondCapacity => {
                    write!(f, "Given offset is beyond the capacity for this segment.")
                }
                SegmentError::SegmentMaxed => {
                    write!(f, "Segment maxed out segment store size")
                }
                SegmentError::OffsetOutOfBounds => {
                    write!(f, "Given offset is out of bounds of the written region.")
                }
            }
        }
    }

    impl<T, S> std::error::Error for SegmentError<T, S>
    where
        T: Deref<Target = [u8]> + std::fmt::Debug,
        S: Store<T> + std::fmt::Debug,
    {
    }

    /// Cross platform storage abstraction used for reading and writing [`Record`](super::Record) instances
    /// from [`Store`](super::store::Store) implementations. It also acts the unit of storage in a
    /// [`SegmentedLog`](super::segmented_log::SegmentedLog).
    ///
    /// [`Segment`] uses binary encoding with [`bincode`] for serializing [`Record`](super::Record)s.
    ///
    /// Each segment can store a fixed size of bytes. A segment has a `base_offset` which is the
    /// offset of the first message in the segment. Note that the `base_offset` is not necessarily
    /// zero and can be arbitrarily high number. The `next_offset` for a segment is offset at which
    /// the next record will be written. (offset of last record + size of last record.)
    ///
    /// ```text
    /// [segment]
    /// ┌────────────────────────────────────────────────────────────────┐
    /// │                                                                │
    /// │  record offset = position in store + segment base_offset       │
    /// │                                                                │
    /// │  [store]                                                       │
    /// │  ┌──────────┐                                                  │
    /// │  │ record#x │ position: 0                                      │
    /// │  ├──────────┤                                                  │
    /// │  │ record#y │ position: size(record#x)                         │
    /// │  ├──────────┤                                                  │
    /// │  │ record#z │ position: size(record#x) + size(record#y)        │
    /// │  ├──────────┤                                                  │
    /// │      ....                                                      │
    /// │                                                                │
    /// └────────────────────────────────────────────────────────────────┘
    /// ```
    ///
    /// Each segment is backed by a store which is a handle to a file on disk. Since it directly
    /// deals with file, position starts from 0, like file position. As a consequence, we translate
    /// store position to segment offset by simply adding the segment offset to the store position.
    /// Writes in a store are buffered. The write buffer is synced when it fills up, to persist
    /// data to the disk. Reads read values that have been successfully synced and persisted to the
    /// disk. Hence, reads on a store lag behind the writes.
    ///
    /// All writes go to the write segment. When we max out the capacity of the write segment, we
    /// close the write segment and reopen it as a read segment. The re-opened segment is added to
    /// the list of read segments. A new write segment is then created with `base_offset` as the
    /// `next_offset` of the previous write segment.
    pub struct Segment<T, S: Store<T>>
    where
        T: Deref<Target = [u8]>,
    {
        store: S,
        base_offset: u64,
        next_offset: u64,
        config: config::SegmentConfig,

        creation_time: Instant,

        _phantom_data: PhantomData<T>,
    }

    #[doc(hidden)]
    macro_rules! consume_store_from_segment {
        ($segment:ident, $async_store_method:ident) => {
            $segment
                .store
                .$async_store_method()
                .await
                .map_err(SegmentError::StoreError)
        };
    }

    impl<T, S: Store<T>> Segment<T, S>
    where
        T: Deref<Target = [u8]>,
    {
        /// Creates a segment instance with the given config, base offset and store instance.
        pub fn with_config_base_offset_and_store(
            config: config::SegmentConfig,
            base_offset: u64,
            store: S,
        ) -> Self {
            let initial_store_size = store.size();
            let next_offset = if initial_store_size > 0 {
                base_offset + initial_store_size
            } else {
                base_offset
            };

            Self {
                store,
                base_offset,
                next_offset,
                config,
                creation_time: Instant::now(),
                _phantom_data: PhantomData,
            }
        }

        /// Offset of the first record in this segment instance.
        pub fn base_offset(&self) -> u64 {
            self.base_offset
        }

        /// Offset at which the next record will be written in this segment.
        pub fn next_offset(&self) -> u64 {
            self.next_offset
        }

        /// Returns a reference to the backing [`Store`](super::store::Store).
        pub fn store(&self) -> &S {
            &self.store
        }

        /// Returns the position on the underlying store to which the given offset would map to.
        /// This method returns None if the given offset is beyond the capacity.
        pub fn store_position(&self, offset: u64) -> Option<u64> {
            match offset.checked_sub(self.base_offset()) {
                Some(pos) if pos >= self.config.max_store_bytes => None,
                Some(pos) => Some(pos),
                None => None,
            }
        }

        /// Returns whether the size of the underlying store in number of bytes is greater than or
        /// equal to the allowed limit in [`config::SegmentConfig::max_store_bytes`].
        pub fn is_maxed(&self) -> bool {
            self.store().size() >= self.config.max_store_bytes
        }

        /// Returns the size of the underlying store.
        pub fn size(&self) -> u64 {
            self.store.size()
        }

        /// Appends the given record to the end of this store.
        ///
        /// ## Returns
        /// The offset at which the record was written.
        ///
        /// ## Errors
        /// - [`SegmentError::SegmentMaxed`] if [`Self::is_maxed`] returns true.
        /// - [`SegmentError::SerializationError`] if there was an error during binary encoding the
        /// given record instance.
        /// - [`SegmentError::StoreError`] if there was an error during writing to the underlying
        /// [`Store`](super::store::Store) instance.
        pub async fn append(&mut self, record: &mut Record) -> Result<u64, SegmentError<T, S>> {
            if self.is_maxed() {
                return Err(SegmentError::SegmentMaxed);
            }

            let old_record_offset = record.offset;

            let current_offset = self.next_offset;
            record.offset = current_offset;

            let bincoded_record =
                bincode::serialize(record).map_err(|_x| SegmentError::SerializationError)?;

            let (_, bytes_written) = self
                .store
                .append(&bincoded_record)
                .await
                .map_err(SegmentError::StoreError)?;

            self.next_offset += bytes_written as u64;

            record.offset = old_record_offset;

            Ok(current_offset)
        }

        pub fn offset_within_bounds(&self, offset: u64) -> bool {
            offset < self.next_offset()
        }

        /// Reads the record at the given offset.
        ///
        /// ## Returns
        /// A tuple containing:
        /// - [`Record`](super::Record) instance containing the desired record.
        /// - [`u64`] value containing the offset of the next record.
        ///
        /// ## Errors
        /// - [`SegmentError::OffsetOutOfBounds`] if the  `offset >= next_offset` value.
        /// - [`SegmentError::OffsetBeyondCapacity`] if the given offset is beyond this stores
        /// capacity, and doesn't map to a valid position on the underlying
        /// [`Store`](super::store::Store) instance.
        /// - [`SegmentError::StoreError`] if there was an error during reading the record at the
        /// given offset from the underlying [`Store`](super::store::Store) instance.
        /// - [`SegmentError::SerializationError`] if there is an error during deserializing the
        /// record from the bytes read from storage.
        pub async fn read(&self, offset: u64) -> Result<(Record, u64), SegmentError<T, S>> {
            if !self.offset_within_bounds(offset) {
                return Err(SegmentError::OffsetOutOfBounds);
            }

            let position = self
                .store_position(offset)
                .ok_or(SegmentError::OffsetBeyondCapacity)?;

            let (record_bytes, next_record_position) = self
                .store
                .read(position)
                .await
                .map_err(SegmentError::StoreError)?;

            let record: Record = bincode::deserialize(&record_bytes)
                .map_err(|_x| SegmentError::SerializationError)?;

            Ok((record, self.base_offset() + next_record_position))
        }

        /// Advances this [`Segment`] instance's `next_offset` value to the given value.
        /// This method simply returns [`Ok`] if `new_next_offset <= next_offset`.
        ///
        /// ## Errors
        /// - [`SegmentError::OffsetBeyondCapacity`] if the given offset doesn't map to a valid
        /// position on the underlying store.
        pub fn advance_to_offset(
            &mut self,
            new_next_offset: u64,
        ) -> Result<(), SegmentError<T, S>> {
            if new_next_offset <= self.next_offset() {
                return Ok(());
            }

            if self.store_position(new_next_offset).is_none() {
                return Err(SegmentError::OffsetBeyondCapacity);
            }

            self.next_offset = new_next_offset;

            Ok(())
        }

        /// Removes the underlying [`Store`](super::segment::Store) instances associated.
        /// This method consumes this [`Segment`] instance to prevent further operations on this
        /// segment.
        ///
        /// ## Errors
        /// [`SegmentError::StoreError`] if there was an error in removing the underlying store.
        #[inline]
        pub async fn remove(self) -> Result<(), SegmentError<T, S>> {
            consume_store_from_segment!(self, remove)
        }

        /// Closes the underlying [`Store`](super::segment::Store) instances associated.
        /// This method consumes this [`Segment`] instance to prevent further operations on this
        /// segment.
        ///
        /// ## Errors
        /// [`SegmentError::StoreError`] if there was an error in closing the underlying store.
        #[inline]
        pub async fn close(self) -> Result<(), SegmentError<T, S>> {
            consume_store_from_segment!(self, close)
        }

        /// Returns an [`std::time::Instant`] value denoting the time at which this segment was
        /// created.
        pub fn creation_time(&self) -> Instant {
            self.creation_time
        }
    }

    /// Implements [`Scanner`](super::Scanner) for [`Segment`] references.
    pub struct SegmentScanner<'a, T, S>
    where
        T: Deref<Target = [u8]>,
        S: Store<T>,
    {
        store_scanner: StoreScanner<'a, T, S>,
    }

    impl<'a, T, S> SegmentScanner<'a, T, S>
    where
        T: Deref<Target = [u8]>,
        S: Store<T>,
    {
        /// Creates a new [`SegmentScanner`] that starts reading from the given segments `base_offset`.
        pub fn new(segment: &'a Segment<T, S>) -> Result<Self, SegmentError<T, S>> {
            Self::with_offset(segment, segment.base_offset())
        }

        /// Creates a new [`SegmentScanner`] that starts reading from the given offset.
        ///
        /// ## Errors
        /// - [`SegmentError::OffsetOutOfBounds`] if the given `offset >= segment.next_offset()`
        /// - [`SegmentError::OffsetBeyondCapacity`] if the given offset doesn't map to a valid
        /// location on the [`Segment`] instances underlying store.
        pub fn with_offset(
            segment: &'a Segment<T, S>,
            offset: u64,
        ) -> Result<Self, SegmentError<T, S>> {
            if !segment.offset_within_bounds(offset) {
                return Err(SegmentError::OffsetOutOfBounds);
            }

            Ok(Self {
                store_scanner: StoreScanner::with_position(
                    segment.store(),
                    segment
                        .store_position(offset)
                        .ok_or(SegmentError::OffsetBeyondCapacity)?,
                ),
            })
        }
    }

    #[async_trait(?Send)]
    impl<'a, T, S> Scanner for SegmentScanner<'a, T, S>
    where
        T: Deref<Target = [u8]> + Unpin,
        S: Store<T>,
    {
        type Item = super::Record;

        async fn next(&mut self) -> Option<Self::Item> {
            self.store_scanner
                .next()
                .await
                .and_then(|record_bytes| bincode::deserialize(&record_bytes).ok())
        }
    }

    pub mod config {
        //! Module providing types for configuring [`Segment`](super::Segment) instances.
        use serde::{Deserialize, Serialize};

        /// Configuration pertaining to segment storage and buffer sizes.
        #[derive(Serialize, Deserialize, Debug, Clone, Copy)]
        pub struct SegmentConfig {
            /// Segment store's write buffer size. The write buffer is flushed to the disk when it is
            /// filled. Reads of records in a write buffer are only possible when the write buffer is
            /// flushed.
            pub store_buffer_size: usize,

            /// Maximum segment storage size, after which this segment is demoted from the current
            /// write segment to a read segment.
            pub max_store_bytes: u64,
        }
    }
}

/// An ordered sequential collection of [`Record`] instances.
///
/// A [`Record`] in an [`CommitLog`] is addressed with an unique [`u64`] offset, which denotes it
/// position in the collection of records. The offsets in a [`CommitLog`] are monotonically
/// increasing. A higher offset denotes that the [`Record`] appears later in the [`CommitLog`].
///
/// This is useful for storing a sequence of events or operations, which may be used to restore a
/// system to it's previous or current state.
#[async_trait(?Send)]
pub trait CommitLog {
    /// Error type used by the methods of this trait.
    type Error;

    /// Returns the offset where the next [`Record`] will be written in this [`CommitLog`].
    fn highest_offset(&self) -> u64;

    /// Returns the offset of the first [`Record`] in this [`CommitLog`].
    fn lowest_offset(&self) -> u64;

    /// Returns whether the given offset lies in `[lowest_offset, highest_offset)`
    #[inline]
    fn offset_within_bounds(&self, offset: u64) -> bool {
        offset >= self.lowest_offset() && offset < self.highest_offset()
    }

    /// Appends a new [`Record`] at the end of this [`CommitLog`].
    async fn append(&mut self, record: &mut Record) -> Result<u64, Self::Error>;

    /// Reads the [`Record`] at the given offset, along with the offset of the next record from
    /// this [`CommitLog`].
    async fn read(&self, offset: u64) -> Result<(Record, u64), Self::Error>;

    /// Removes all underlying storage files associated. Consumes this [`CommitLog`] instance to
    /// prevent further operations on this instance.
    async fn remove(self) -> Result<(), Self::Error>;

    /// Closes the files associated with this [`CommitLog`] instances and syncs them to storage
    /// media. Consumes this [`CommitLog`] instance to  prevent further operations on this
    /// instance.
    async fn close(self) -> Result<(), Self::Error>;
}

/// [`Scanner`] implementation for a [`CommitLog`] implementation.
pub struct LogScanner<'a, Log: CommitLog> {
    log: &'a Log,
    offset: u64,
    scan_seek_bytes: u64,
}

impl<'a, Log: CommitLog> LogScanner<'a, Log> {
    /// Creates a new [`LogScanner`] from the given [`CommitLog`] implementation immutable reference.
    ///
    /// ## Default parameters values used
    /// - `offset`: `log.lowest_offset()`
    /// - `scan_seek_bytes`: `8`
    pub fn new(log: &'a Log) -> Option<Self> {
        Self::with_offset_and_scan_seek_bytes(log, log.lowest_offset(), 8)
    }

    /// Creates a new [`LogScanner`] scanner instance with the given parameters.
    ///
    /// ## Parameters
    /// - `log`: an immutable reference to a [`CommitLog`] implementation instance.
    /// - `offset`: offset from which to start reading from.
    /// - `scan_seek_bytes`: number of bytes to seek at a time to search for next record, if a read
    /// operation fails in the middle.
    pub fn with_offset_and_scan_seek_bytes(
        log: &'a Log,
        offset: u64,
        scan_seek_bytes: u64,
    ) -> Option<Self> {
        if !log.offset_within_bounds(offset) {
            None
        } else {
            Some(LogScanner {
                log,
                offset,
                scan_seek_bytes,
            })
        }
    }
}

#[async_trait(?Send)]
impl<'a, Log: CommitLog> Scanner for LogScanner<'a, Log> {
    type Item = Record;

    /// Linearly scans and reads the next record in the [`CommitLog`] instance asynchronously. If
    /// the read operation fails, it searches for the next readable offset by seeking with the
    /// configured `scan_seek_bytes`.
    ///
    /// It stops when the internal offset of this scanner reaches the commit log's highest offset.
    ///
    /// ## Returns
    /// - [`Some(Record)`]: if a record is there to be read.
    /// - None: to indicate that are there are no more records to read.
    async fn next(&mut self) -> Option<Self::Item> {
        while self.offset < self.log.highest_offset() {
            if let Ok((record, next_record_offset)) = self.log.read(self.offset).await {
                self.offset = next_record_offset;
                return Some(record);
            } else {
                self.offset += self.scan_seek_bytes;
            }
        }

        None
    }
}

pub mod segmented_log {
    //! Module providing abstractions for supporting a immutable, segmented and persistent
    //! [`CommitLog`](super::CommitLog) implementation.
    //!
    //! This module is not meant to be used directly by application developers. Application
    //! developers would prefer one of the specializations specific to their async runtime.
    //! See [`glommio_impl`](super::glommio_impl).

    use std::{
        error::Error,
        fmt::Display,
        fs,
        ops::Deref,
        path::{Path, PathBuf},
        time::Duration,
    };

    use async_trait::async_trait;

    use super::{
        segment::{self, Segment},
        store, Record,
    };

    /// Error type for our segmented log implementation. Used as the `Error` associated type for
    /// our [`SegmentedLog`]'s [`CommitLog`](super::CommitLog) trait implementation.
    #[derive(Debug)]
    pub enum SegmentedLogError<T, S>
    where
        T: Deref<Target = [u8]>,
        S: store::Store<T>,
    {
        /// An error caused by an operation on an underlying segment.
        SegmentError(segment::SegmentError<T, S>),

        /// IO error caused during the creating and deletion of storage directories for our
        /// [`SegmentedLog`]'s storage.
        IoError(std::io::Error),

        /// The [`Option`] containing our write segment evaluates to [`None`].
        WriteSegmentLost,

        /// The given offset is greater than or equal to the highest offset of the segmented log.
        OffsetOutOfBounds,

        /// The given offset has not been synced to disk and is not a valid offset to advance to.
        OffsetNotValidToAdvanceTo,
    }

    impl<T, S> Display for SegmentedLogError<T, S>
    where
        T: Deref<Target = [u8]>,
        S: store::Store<T>,
    {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::SegmentError(err) => write!(f, "Segment error occurred: {}", err),
                Self::IoError(err) => write!(f, "IO error occurred: {}", err),
                Self::WriteSegmentLost => write!(f, "Write segment is None."),
                Self::OffsetOutOfBounds => write!(f, "Given offset is out of bounds."),
                Self::OffsetNotValidToAdvanceTo => {
                    write!(f, "Given offset is not a valid offset to advance to.")
                }
            }
        }
    }

    impl<T, S> Error for SegmentedLogError<T, S>
    where
        T: Deref<Target = [u8]> + std::fmt::Debug,
        S: store::Store<T> + std::fmt::Debug,
    {
    }

    /// Strategy pattern trait for constructing [`Segment`] instances for [`SegmentedLog`].
    #[async_trait(?Send)]
    pub trait SegmentCreator<T, S>
    where
        T: Deref<Target = [u8]>,
        S: store::Store<T>,
    {
        /// Constructs a new segment with it's store at the given store path, base_offset and
        /// segment config.
        async fn new_segment_with_store_file_path_offset_and_config<P: AsRef<Path>>(
            &self,
            store_file_path: P,
            base_offset: u64,
            config: segment::config::SegmentConfig,
        ) -> Result<Segment<T, S>, segment::SegmentError<T, S>>;

        /// Constructs a new segment in the given storage directory with the given base offset and
        /// config.
        ///
        /// The default provided implementation simply invokes
        /// [`Self::new_segment_with_store_file_path_offset_and_config`] with `store_file_path`
        /// obtained by invoking [`common::store_file_path`] on `storage_dir` and `base_offset`.
        async fn new_segment_with_storage_dir_offset_and_config<P: AsRef<Path>>(
            &self,
            storage_dir: P,
            base_offset: u64,
            config: segment::config::SegmentConfig,
        ) -> Result<Segment<T, S>, segment::SegmentError<T, S>> {
            self.new_segment_with_store_file_path_offset_and_config(
                common::store_file_path(storage_dir, base_offset),
                base_offset,
                config,
            )
            .await
        }
    }

    /// A cross platform immutable, persisted and segmented log implementation using [`Segment`]
    /// instances.
    ///
    /// [`SegmentedLog`] is a [`CommitLog`](super::CommitLog) implementation.
    ///
    /// A segmented log is a collection of read segments and a single write segment. It consists of
    /// a [`Vec<Segment>`] for storing read segments and a single [`Option<Segment>`] for storing
    /// the write segment. The log is immutable since, only append and read operations are
    /// available. There are no record update and delete operations. The log is segmented, since it
    /// is composed of segments, where each segment services records from a particular range of
    /// offsets.
    ///
    /// ```text
    /// [segmented_log]
    /// ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    /// │                                                                                                                                                     │
    /// │                               [log:"read"]           ├[offset#x)    ├[offset#x + size(xm_1)]  ├[offset(xm_n-1)]   ├[offset(xm_n-1) + size(xm_n-1)]  │
    /// │                               ┌──────────┐           ┌──────────────┬──────────────┬─         ┬───────────────────┬──────────────┐                  │
    /// │  initial_offset: offset#x     │ offset#x │->[segment]│ message#xm_1 │ message#xm_2 │ …      … │ message#xm_n-1    │ message#xm_n │                  │
    /// │                               │          │           └──────────────┴──────────────┴─         ┴───────────────────┴──────────────┘                  │
    /// │                               │          │                                                                                                          │
    /// │                               ├──────────┤           ├[offset#y)    ├[offset#y + size(ym_1)]  ├[offset(ym_n-1)]   ├[offset(ym_n-1) + size(ym_n-1)]  │
    /// │                               ├──────────┤           ┌──────────────┬──────────────┬─         ┬───────────────────┬──────────────┐                  │
    /// │  {offset#y = (offset(xm_n) +  │ offset#y │->[segment]│ message#ym_1 │ message#ym_2 │ …      … │ message#ym_n-1    │ message#ym_n │                  │
    /// │               size(xm_n))}    │          │           └──────────────┴──────────────┴─         ┴───────────────────┴──────────────┘                  │
    /// │                               │          │                                                                                                          │
    /// │                               ├──────────┤           ├[offset#x)    ├[offset#x + size(xm_1)]  ├[offset(xm_n-1)]   ├[offset(xm_n-1) + size(xm_n-1)]  │
    /// │                               ├──────────┤           ┌──────────────┬──────────────┬─         ┬───────────────────┬──────────────┐                  │
    /// │  {offset#z = (offset(ym_n) +  │ offset#z │->[segment]│ message#zm_1 │ message#zm_2 │ …      … │ message#zm_n-1    │ message#zm_n │                  │
    /// │               size(ym_n))}    │          │           └──────────────┴──────────────┴─         ┴───────────────────┴──────────────┘                  │
    /// │                               │          │                                                                                                          │
    /// │                               ├──────────┤                                                                                                          │
    /// │                                   ....                                                                                                              │
    /// │                                                                                                                                                     │
    /// │                                                                                                                                                     │
    /// │                               [log:"write"] -> [segment](with base_offset = next_offset of last read segment)                                       │
    /// │                                                                                                                                                     │
    /// │  where:                                                                                                                                             │
    /// │  =====                                                                                                                                              │
    /// │  offset(m: Message) := offset of message m in log.                                                                                                  │
    /// │  size(m: Message) := size of message m                                                                                                              │
    /// │                                                                                                                                                     │
    /// │  offset#x := some offset x                                                                                                                          │
    /// │  message#m := some message m                                                                                                                        │
    /// │                                                                                                                                                     │
    /// └─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    /// ```
    ///
    /// All writes go to the write segment. When we max out the capacity of the write segment, we
    /// close the write segment and reopen it as a read segment. The re-opened segment is added to
    /// the list of read segments. A new write segment is then created with `base_offset` as the
    /// `next_offset` of the previous write segment.
    ///
    /// When reading from a particular offset, we linearly check which segment contains the given
    /// read segment. If a segment capable of servicing a read from the given offset is found, we
    /// read from that segment. If no such segment is found among the read segments, we default to
    /// the write segment. The following scenarios may occur when reading from the write segment in
    /// this case:
    /// - The write segment has synced the messages including the message at the given offset. In
    /// this case the record is read successfully and returned.
    /// - The write segment hasn't synced the data at the given offset. In this case the read fails
    /// with a segment I/O error.
    /// - If the offset is out of bounds of even the write segment, we return an "out of bounds"
    /// error.

    pub struct SegmentedLog<T, S, SegC>
    where
        T: Deref<Target = [u8]>,
        S: store::Store<T>,
        SegC: SegmentCreator<T, S>,
    {
        write_segment: Option<Segment<T, S>>,
        read_segments: Vec<Segment<T, S>>,

        storage_directory: PathBuf,
        config: config::SegmentedLogConfig,

        segment_creator: SegC,
    }

    #[doc(hidden)]
    macro_rules! write_segment_ref {
        ($segmented_log:ident, $ref_method:ident) => {
            $segmented_log
                .write_segment
                .$ref_method()
                .ok_or(SegmentedLogError::WriteSegmentLost)
        };
    }

    impl<T, S, SegC> SegmentedLog<T, S, SegC>
    where
        T: Deref<Target = [u8]>,
        S: store::Store<T>,
        SegC: SegmentCreator<T, S>,
    {
        /// "Moves" the current write segment to the vector of read segments and in its place, a new
        /// write segment is created.
        ///
        /// This moving process involves closing the current write segments, creating a new read
        /// segment with this write segments base offset and adding it to the end of the read
        /// segments' vector. Once this process is completed, a new write segments is created with
        /// the old write segments base offset.
        ///
        /// ## Errors
        /// - [`SegmentedLogError::WriteSegmentLost`]: if the write segment [`Option`] evaluates to
        /// [`None`] before this operation.
        /// - [`SegmentedLogError::SegmentError`]: If there is an error during operning a new
        /// segment.
        async fn rotate_new_write_segment(&mut self) -> Result<(), SegmentedLogError<T, S>> {
            let old_write_segment = self
                .write_segment
                .take()
                .ok_or(SegmentedLogError::WriteSegmentLost)?;

            let old_write_segment_base_offset = old_write_segment.base_offset();
            let new_write_segment_base_offset = old_write_segment.next_offset();

            old_write_segment
                .close()
                .await
                .map_err(SegmentedLogError::SegmentError)?;

            self.read_segments.push(
                self.segment_creator
                    .new_segment_with_storage_dir_offset_and_config(
                        Path::new(&self.storage_directory),
                        old_write_segment_base_offset,
                        self.config.segment_config,
                    )
                    .await
                    .map_err(SegmentedLogError::SegmentError)?,
            );

            self.write_segment = Some(
                self.segment_creator
                    .new_segment_with_storage_dir_offset_and_config(
                        Path::new(&self.storage_directory),
                        new_write_segment_base_offset,
                        self.config.segment_config,
                    )
                    .await
                    .map_err(SegmentedLogError::SegmentError)?,
            );

            Ok(())
        }

        /// Returns whether the current write segment is maxed out or not. We invoke
        /// [`Segment::is_maxed`] on the current write segment and return it.
        ///
        /// ## Errors
        /// - [`SegmentedLogError::WriteSegmentLost`]: if the current write segment
        /// [`Option`] evaluates to [`None`].
        #[inline]
        pub fn is_write_segment_maxed(&self) -> Result<bool, SegmentedLogError<T, S>> {
            self.write_segment
                .as_ref()
                .map(|x| x.is_maxed())
                .ok_or(SegmentedLogError::WriteSegmentLost)
        }

        /// Creates a new [`SegmentedLog`] instance with files stored in the given storage
        /// directory.
        ///
        /// If there are no segments present in the given directory, a new write segment is created
        /// with [`config::SegmentedLogConfig::initial_offset`] as the base offset. The given
        /// segment creator is used for creating segments during the various operation of the
        /// segmented log, such as rotating in new write segments, reopening the write segment etc.
        ///
        /// ## Returns
        /// - A newly created [`SegmentedLog`] instance.
        ///
        /// ## Errors
        /// - As discussed in [`common::read_and_write_segments`] and
        /// [`common::segments_in_directory`]
        pub async fn new<P: AsRef<Path>>(
            storage_directory_path: P,
            config: config::SegmentedLogConfig,
            segment_creator: SegC,
        ) -> Result<Self, SegmentedLogError<T, S>> {
            let (read_segments, write_segment) = common::read_and_write_segments(
                &storage_directory_path,
                &segment_creator,
                config,
                common::segments_in_directory(
                    &storage_directory_path,
                    &segment_creator,
                    config.segment_config,
                )
                .await?,
            )
            .await?;

            Ok(Self {
                write_segment: Some(write_segment),
                read_segments,
                storage_directory: storage_directory_path.as_ref().to_path_buf(),
                config,
                segment_creator,
            })
        }

        /// Reopens i.e. closes and opens the current write segment. This might be useful in
        /// syncing changes to persistent media.
        ///
        /// ## Errors
        /// - [`SegmentedLogError::WriteSegmentLost`]: if the write segment [`Option`] evaluates
        /// to [`None`].
        /// - [`SegmentedLogError::SegmentError`]: if there is and error during opening or closing
        /// the write segment.
        pub async fn reopen_write_segment(&mut self) -> Result<(), SegmentedLogError<T, S>> {
            let write_segment = self
                .write_segment
                .take()
                .ok_or(SegmentedLogError::WriteSegmentLost)?;

            let write_segment_base_offset = write_segment.base_offset();

            write_segment
                .close()
                .await
                .map_err(SegmentedLogError::SegmentError)?;

            self.write_segment = Some(
                self.segment_creator
                    .new_segment_with_storage_dir_offset_and_config(
                        &self.storage_directory,
                        write_segment_base_offset,
                        self.config.segment_config,
                    )
                    .await
                    .map_err(SegmentedLogError::SegmentError)?,
            );

            Ok(())
        }

        /// Removes segments older than the given `expiry_duration`.
        ///
        /// ## Mechanism for removal
        /// If the write segment is expired, we move it to the end of the vector of read segments,
        /// and in it's place create a new write segment with `base_offset` as the old write
        /// segment's `next_offset`. Note that this is done without reopening the write segment so
        /// as to avoid updating the creation time of the write segment. Also we don't care about
        /// sync-ing its contents since it's due for removal anyway.
        ///
        /// All the read segments are appended at the end of the vector of read segments. Hence
        /// by definition the read segment vector is sorted in descending order of age. First we
        /// look for the first non expired segment in the read segments vector. Once we find it, it
        /// means that, all the segments before it are expired. Hence we split off the vector at
        /// that point. This enables us to separate the expired read segments. Next we attempt to
        /// remove them on by one.
        ///
        /// If there is any error in removing a segment, we stop and move back the remaining
        /// expired read segments to vector of read segments, preserving chronology, so that we can
        /// try again later.
        ///
        /// ## Errors
        /// - [`SegmentedLogError::SegmentError`]: if there is an error during closing a segment.
        /// - [`SegmentedLogError::WriteSegmentLost`]: if the current write segment [`Option`]
        /// evaluates to [`None`].
        pub async fn remove_expired_segments(
            &mut self,
            expiry_duration: Duration,
        ) -> Result<(), SegmentedLogError<T, S>> {
            if write_segment_ref!(self, as_ref)?.creation_time().elapsed() >= expiry_duration {
                let new_segment_base_offset = write_segment_ref!(self, as_ref)?.next_offset();
                let old_write_segment = self.write_segment.replace(
                    self.segment_creator
                        .new_segment_with_storage_dir_offset_and_config(
                            &self.storage_directory,
                            new_segment_base_offset,
                            self.config.segment_config,
                        )
                        .await
                        .map_err(SegmentedLogError::SegmentError)?,
                );

                self.read_segments
                    .push(old_write_segment.ok_or(SegmentedLogError::WriteSegmentLost)?);
            }

            let first_non_expired_segment_position = self
                .read_segments
                .iter()
                .position(|x| x.creation_time().elapsed() < expiry_duration);

            let non_expired_read_segments = if let Some(first_non_expired_segment_position) =
                first_non_expired_segment_position
            {
                self.read_segments
                    .split_off(first_non_expired_segment_position)
            } else {
                Vec::new()
            };

            let mut expired_read_segments =
                std::mem::replace(&mut self.read_segments, non_expired_read_segments);

            let mut remove_result = Ok(());

            while !expired_read_segments.is_empty() {
                let expired_segment = expired_read_segments.remove(0);

                remove_result = expired_segment.remove().await;
                if remove_result.is_err() {
                    break;
                }
            }

            if remove_result.is_err() && !expired_read_segments.is_empty() {
                expired_read_segments.append(&mut self.read_segments);
                self.read_segments = expired_read_segments;
            }

            remove_result.map_err(SegmentedLogError::SegmentError)
        }

        /// Advances this segmented log to the given new highest offset.
        ///
        /// This is useful in the scenario where multiple [`SegmentedLog`] instances are opened for
        /// the same backing files. This enables the [`SegmentedLog`] instances lagging behind to
        /// refresh their state to ensure that all persisted records upto the given offset can be
        /// read from them. This also helps [`SegmentedLog`] to ensure that no records are
        /// overwritten in situations like these.
        ///
        /// ## Mechanism for advancement
        /// Before doing anything we reopen the write segment of this [`SegmentedLog`] to refresh
        /// stale `highest_offset` if necessary.
        ///
        /// If the given `new_highest_offset <= self.highest_offset()` we do nothing and simply
        /// return Ok(()). Otherwise we have the following situation:
        /// - while The given `new_highest_offset` is beyond the capacity of the current write
        /// segment.
        ///     - if the size of the current write segment is zero
        ///         - this implies that the records upto the given offset have not been persisted on
        ///         the disk and the given new_highest_offset cannot be trusted. So we error out
        ///         with [`SegmentedLogError::OffsetNotValidToAdvanceTo`]
        ///     - we try to accommodate the offset by rotating the write segment
        /// - once the offset is accommodated, we attempt to advance the `next_offset` of the write
        /// segment to the given `new_highest_offset`. If there is an error, we error out with
        /// [`SegmentedLogError::SegmentError`].
        ///
        /// ## Returns
        /// - [`SegmentedLogError::OffsetNotValidToAdvanceTo`]: if the given offset is not a valid
        /// offset to advance to.
        /// - [`SegmentedLogError::SegmentError`]: if there is an error during advancing the offset
        /// ofthe write segment.
        pub async fn advance_to_offset(
            &mut self,
            new_highest_offset: u64,
        ) -> Result<(), SegmentedLogError<T, S>> {
            self.reopen_write_segment().await?;

            if new_highest_offset <= write_segment_ref!(self, as_ref)?.next_offset() {
                return Ok(());
            }

            while write_segment_ref!(self, as_ref)?
                .store_position(new_highest_offset)
                .is_none()
            {
                if write_segment_ref!(self, as_ref)?.size() == 0 {
                    return Err(SegmentedLogError::OffsetNotValidToAdvanceTo);
                }

                self.rotate_new_write_segment().await?;
            }

            write_segment_ref!(self, as_mut)?
                .advance_to_offset(new_highest_offset)
                .map_err(SegmentedLogError::SegmentError)?;

            Ok(())
        }
    }

    #[doc(hidden)]
    macro_rules! consume_segments_from_segmented_log_with_method {
        ($segmented_log:ident, $method:ident) => {
            let mut segments = $segmented_log.read_segments;

            segments.push(
                $segmented_log
                    .write_segment
                    .take()
                    .ok_or(SegmentedLogError::WriteSegmentLost)?,
            );

            for segment in segments {
                segment
                    .$method()
                    .await
                    .map_err(SegmentedLogError::SegmentError)?;
            }
        };
    }

    #[async_trait(?Send)]
    impl<T, S, SegC> super::CommitLog for SegmentedLog<T, S, SegC>
    where
        T: Deref<Target = [u8]>,
        S: store::Store<T>,
        SegC: SegmentCreator<T, S>,
    {
        type Error = SegmentedLogError<T, S>;

        async fn append(&mut self, record: &mut Record) -> Result<u64, Self::Error> {
            while self.is_write_segment_maxed()? {
                self.rotate_new_write_segment().await?;
            }

            self.write_segment
                .as_mut()
                .ok_or(SegmentedLogError::WriteSegmentLost)?
                .append(record)
                .await
                .map_err(SegmentedLogError::SegmentError)
        }

        async fn read(&self, offset: u64) -> Result<(Record, u64), Self::Error> {
            if !self.offset_within_bounds(offset) {
                return Err(SegmentedLogError::OffsetOutOfBounds);
            }

            let read_segment = self
                .read_segments
                .iter()
                .find(|x| x.store_position(offset).is_some());

            if let Some(read_segment) = read_segment {
                read_segment
                    .read(offset)
                    .await
                    .map_err(SegmentedLogError::SegmentError)
            } else {
                self.write_segment
                    .as_ref()
                    .ok_or(SegmentedLogError::WriteSegmentLost)?
                    .read(offset)
                    .await
                    .map_err(SegmentedLogError::SegmentError)
            }
        }

        fn highest_offset(&self) -> u64 {
            self.write_segment
                .as_ref()
                .map(|x| x.next_offset())
                .unwrap_or(self.config.initial_offset)
        }

        fn lowest_offset(&self) -> u64 {
            self.read_segments
                .first()
                .or(self.write_segment.as_ref())
                .map(|x| x.base_offset())
                .unwrap_or(self.config.initial_offset)
        }

        async fn remove(mut self) -> Result<(), Self::Error> {
            consume_segments_from_segmented_log_with_method!(self, remove);
            fs::remove_dir_all(self.storage_directory).map_err(SegmentedLogError::IoError)?;
            Ok(())
        }

        async fn close(mut self) -> Result<(), Self::Error> {
            consume_segments_from_segmented_log_with_method!(self, close);
            Ok(())
        }
    }

    pub mod common {
        //! Module providing utilities used by [`SegmentedLog`](super::SegmentedLog).

        use super::{
            config::SegmentedLogConfig, segment::config::SegmentConfig,
            store::common::STORE_FILE_EXTENSION, Segment, SegmentCreator, SegmentedLogError,
        };
        use std::{
            fs, io,
            ops::Deref,
            path::{Path, PathBuf},
        };

        type Error<T, S> = SegmentedLogError<T, S>;

        /// Returns the backing [`Store`](crate::commit_log::store::Store) file path, with the
        /// given Log storage directory and the given segment base offset.
        #[inline]
        pub fn store_file_path<P: AsRef<Path>>(storage_dir_path: P, offset: u64) -> PathBuf {
            storage_dir_path.as_ref().join(format!(
                "{}.{}",
                offset,
                super::store::common::STORE_FILE_EXTENSION
            ))
        }

        /// Returns an iterator of the paths of all the [`crate::commit_log::store::Store`] backing
        /// files in the given directory.
        ///
        /// ## Errors
        /// - This functions returns an error if the given storage directory doesn't exist and
        /// couldn't be created.
        pub fn obtain_store_files_in_directory<P: AsRef<Path>>(
            storage_dir_path: P,
        ) -> io::Result<impl Iterator<Item = PathBuf>> {
            fs::create_dir_all(&storage_dir_path).and(fs::read_dir(&storage_dir_path).map(
                |dir_entries| {
                    dir_entries
                        .filter_map(|dir_entry| dir_entry.ok().map(|x| x.path()))
                        .filter_map(|path| {
                            (path.extension()?.to_str()? == STORE_FILE_EXTENSION).then(|| path)
                        })
                },
            ))
        }

        /// Returns the given store paths sorted by the base offset of the respective segments that
        /// they belong to.
        ///
        /// ## Returns
        /// A vector of (path, offset) tuples, sorted by the offsets.
        pub fn store_paths_sorted_by_offset(
            store_paths: impl Iterator<Item = PathBuf>,
        ) -> Vec<(PathBuf, u64)> {
            let mut store_paths = store_paths
                .filter_map(|segment_file_path| {
                    let base_offset: u64 = segment_file_path.file_stem()?.to_str()?.parse().ok()?;

                    Some((segment_file_path, base_offset))
                })
                .collect::<Vec<(PathBuf, u64)>>();

            store_paths.sort_by(|a, b| a.1.cmp(&b.1));
            store_paths
        }

        /// Returns a [`Vec<Segment>`] of all the segments stored in this directory.
        ///
        /// ## Errors
        /// - [`SegmentedLogError::IoError`]: if there was an error in locating segment files in
        /// the given directory.
        /// - [`SegmentedLogError::SegmentError`]: if there was an error in opening a segment.
        pub async fn segments_in_directory<T, S, SegC, P: AsRef<Path>>(
            storage_dir_path: P,
            segment_creator: &SegC,
            segment_config: SegmentConfig,
        ) -> Result<Vec<Segment<T, S>>, Error<T, S>>
        where
            T: Deref<Target = [u8]>,
            S: super::store::Store<T>,
            SegC: SegmentCreator<T, S>,
        {
            let segment_args = store_paths_sorted_by_offset(
                obtain_store_files_in_directory(storage_dir_path)
                    .map_err(SegmentedLogError::IoError)?,
            );

            let mut segments = Vec::with_capacity(segment_args.len());

            for segment_arg in segment_args {
                segments.push(
                    segment_creator
                        .new_segment_with_store_file_path_offset_and_config(
                            &segment_arg.0,
                            segment_arg.1,
                            segment_config,
                        )
                        .await
                        .map_err(SegmentedLogError::SegmentError)?,
                );
            }

            Ok(segments)
        }

        /// Seperates the read segments and write segments in the given [`Vec<Segment>`].
        ///
        /// There are the following cases that can happen here:
        /// - If the given vector of segments is empty, we return an empty vector of read segments
        /// and a new write segment.
        /// - If the last segment is the given list of segment is maxed out, we return the given
        /// vector of segments as is as the vector of read segments, along with a new write
        /// segment.
        /// - Otherwise we pop out the last segment from the given vector of segments. The shorter
        /// vector of segments is returned as the vector of read segments along with the popped out
        /// segment as the write segment.
        ///
        /// ## Errors
        /// - [`SegmentedLogError::SegmentError`]: if there was an error in creating a new segment.
        /// (When creating the write segment)
        /// - [SegmentedLogError::WriteSegmentLost]: if the popped last segments from the given
        /// list of segments is `None` when `segments.last()` returned a [`Some(_)`].
        pub async fn read_and_write_segments<T, S, SegC, P: AsRef<Path>>(
            storage_dir_path: P,
            segment_creator: &SegC,
            log_config: SegmentedLogConfig,
            mut segments: Vec<Segment<T, S>>,
        ) -> Result<(Vec<Segment<T, S>>, Segment<T, S>), Error<T, S>>
        where
            T: Deref<Target = [u8]>,
            S: super::store::Store<T>,
            SegC: SegmentCreator<T, S>,
        {
            Ok(match segments.last() {
                Some(last_segment) if last_segment.is_maxed() => {
                    let segment_base_offset = last_segment.next_offset();
                    (
                        segments,
                        segment_creator
                            .new_segment_with_storage_dir_offset_and_config(
                                storage_dir_path,
                                segment_base_offset,
                                log_config.segment_config,
                            )
                            .await
                            .map_err(SegmentedLogError::SegmentError)?,
                    )
                }
                Some(_) => {
                    let last_segment = segments.pop();
                    (
                        segments,
                        last_segment.ok_or(SegmentedLogError::WriteSegmentLost)?,
                    )
                }
                None => (
                    Vec::new(),
                    segment_creator
                        .new_segment_with_storage_dir_offset_and_config(
                            storage_dir_path,
                            log_config.initial_offset,
                            log_config.segment_config,
                        )
                        .await
                        .map_err(SegmentedLogError::SegmentError)?,
                ),
            })
        }
    }

    pub mod config {
        //! Module providing types for configuring a [`SegmentedLog`](super::SegmentedLog).
        use serde::{Deserialize, Serialize};

        use super::segment::config::SegmentConfig;

        /// Log specific configuration.
        #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
        pub struct SegmentedLogConfig {
            /// Offset from which the first segment of the log starts.
            pub initial_offset: u64,
            /// Config for every segment in this log.
            pub segment_config: SegmentConfig,
        }
    }
}

#[cfg(target_os = "linux")]
pub mod glommio_impl;

pub mod prelude {
    //! Prelude module for [`commit_log`](super) with common exports for convenience.

    pub use super::glommio_impl::prelude::*;

    pub use super::segmented_log::{
        config::SegmentedLogConfig, SegmentCreator, SegmentedLog, SegmentedLogError,
    };
    pub use super::{
        segment::{config::SegmentConfig, Segment, SegmentError},
        store::Store,
        CommitLog, Record,
    };
}
