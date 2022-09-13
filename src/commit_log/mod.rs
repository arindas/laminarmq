use async_trait::async_trait;

/// Reperesents a record in a log.
#[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq, Clone)]
pub struct Record {
    /// Value stored in this record entry.
    pub value: Vec<u8>,
    /// Offset at which this record is stored in the log.
    pub offset: u64,
}

impl Record {
    /// Returns the number of bytes required for the bincoded representation of this record.
    /// This method serializes this record for calculating the size which might return an error.
    /// Hence we use an Option<_> to represent the error case with a None.
    pub fn bincoded_repr_size(&self) -> Option<u64> {
        Some(
            bincode::serialized_size(self).ok()? as u64
                + self::store::common::RECORD_HEADER_LENGTH as u64,
        )
    }

    /// Returns the possible next offset for this record, based on storage schema.
    /// This method serializes this record for calculating the size which might return an error.
    /// Hence we use an Option<_> to represent the error case with a None.
    pub fn next_offset(&self) -> Option<u64> {
        self.bincoded_repr_size().map(|x| x + self.offset)
    }
}

#[async_trait(?Send)]
pub trait Scanner {
    type Item;

    async fn next(&mut self) -> Option<Self::Item>;
}

pub mod store {
    use async_trait::async_trait;
    use std::{cell::Ref, marker::PhantomData, ops::Deref, path::Path, result::Result};

    use super::Scanner;

    #[async_trait(?Send)]
    pub trait Store<Record>
    where
        Record: Deref<Target = [u8]>,
    {
        type Error: std::error::Error;

        async fn append(&mut self, record_bytes: &[u8]) -> Result<(u64, usize), Self::Error>;

        async fn read(&self, position: u64) -> Result<Record, Self::Error>;

        async fn close(self) -> Result<(), Self::Error>;

        async fn remove(self) -> Result<(), Self::Error>;

        fn size(&self) -> u64;

        fn path(&self) -> Result<Ref<Path>, Self::Error>;
    }

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
        pub fn with_position(store: &'a S, position: u64) -> Self {
            Self {
                store,
                position,
                _phantom_data: PhantomData,
            }
        }

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
            if let Ok(record) = self.store.read(self.position).await {
                self.position += (record.len() + common::RECORD_HEADER_LENGTH) as u64;
                return Some(record);
            }

            None
        }
    }

    pub mod common {
        use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

        pub const STORE_FILE_EXTENSION: &str = "store";
        pub const RECORD_HEADER_LENGTH: usize = 8;

        pub struct RecordHeader {
            pub checksum: u32,
            pub length: u32,
        }

        impl RecordHeader {
            pub fn from_record_bytes(record_bytes: &[u8]) -> Self {
                Self {
                    checksum: crc32fast::hash(record_bytes),
                    length: record_bytes.len() as u32,
                }
            }

            pub fn from_bytes(record_header_bytes: &[u8]) -> std::io::Result<Self> {
                let mut cursor = std::io::Cursor::new(record_header_bytes);

                let checksum = cursor.read_u32::<LittleEndian>()?;
                let length = cursor.read_u32::<LittleEndian>()?;

                Ok(Self { checksum, length })
            }

            pub fn as_bytes(self) -> std::io::Result<[u8; RECORD_HEADER_LENGTH]> {
                let mut bytes = [0; RECORD_HEADER_LENGTH];

                let buffer: &mut [u8] = &mut bytes;
                let mut cursor = std::io::Cursor::new(buffer);

                cursor.write_u32::<LittleEndian>(self.checksum)?;
                cursor.write_u32::<LittleEndian>(self.length)?;

                Ok(bytes)
            }

            #[inline]
            pub fn valid_for_record_bytes(&self, record_bytes: &[u8]) -> bool {
                self.length as usize == record_bytes.len()
                    && self.checksum == crc32fast::hash(record_bytes)
            }
        }
    }
}

pub mod segment {
    use std::{fmt::Display, marker::PhantomData, ops::Deref};

    use async_trait::async_trait;

    use super::{
        store::{Store, StoreScanner},
        Record, Scanner,
    };

    #[derive(Debug)]
    pub enum SegmentError<T, S: Store<T>>
    where
        T: Deref<Target = [u8]>,
    {
        StoreError(S::Error),
        SegmentMaxed,
        SerializationError,
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
                SegmentError::OffsetOutOfBounds => {
                    write!(f, "Given offset is out of bounds.")
                }
                SegmentError::SegmentMaxed => {
                    write!(f, "Segment maxed out segment store size")
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

    pub struct Segment<T, S: Store<T>>
    where
        T: Deref<Target = [u8]>,
    {
        store: S,
        base_offset: u64,
        next_offset: u64,
        config: config::SegmentConfig,

        _phantom_data: PhantomData<T>,
    }

    impl<T, S: Store<T>> Segment<T, S>
    where
        T: Deref<Target = [u8]>,
    {
        pub fn with_config_base_offset_and_store(
            config: config::SegmentConfig,
            base_offset: u64,
            store: S,
        ) -> Self {
            let intial_store_size = store.size();
            let next_offset = if intial_store_size > 0 {
                base_offset + intial_store_size
            } else {
                base_offset
            };

            Self {
                store,
                base_offset,
                next_offset,
                config,
                _phantom_data: PhantomData,
            }
        }

        pub fn base_offset(&self) -> u64 {
            self.base_offset
        }

        pub fn next_offset(&self) -> u64 {
            self.next_offset
        }

        pub fn store(&self) -> &S {
            &self.store
        }

        fn store_position(&self, offset: u64) -> Option<u64> {
            match offset.checked_sub(self.base_offset()) {
                Some(pos) if pos >= self.config.max_store_bytes => None,
                Some(pos) => Some(pos),
                None => None,
            }
        }

        pub fn is_maxed(&self) -> bool {
            self.store().size() >= self.config.max_store_bytes
        }

        pub fn size(&self) -> u64 {
            self.store.size()
        }

        pub async fn append(&mut self, record_value: Vec<u8>) -> Result<u64, SegmentError<T, S>> {
            if self.is_maxed() {
                return Err(SegmentError::SegmentMaxed);
            }

            let current_offset = self.next_offset;
            let record = Record {
                value: record_value,
                offset: current_offset,
            };

            let bincoded_record =
                bincode::serialize(&record).map_err(|_x| SegmentError::SerializationError)?;

            let (_, bytes_written) = self
                .store
                .append(&bincoded_record)
                .await
                .map_err(SegmentError::StoreError)?;

            self.next_offset += bytes_written as u64;

            Ok(current_offset)
        }

        pub async fn read(&self, offset: u64) -> Result<Record, SegmentError<T, S>> {
            let position = self
                .store_position(offset)
                .ok_or(SegmentError::OffsetOutOfBounds)?;

            let record_bytes = self
                .store
                .read(position)
                .await
                .map_err(SegmentError::StoreError)?;

            let record: Record = bincode::deserialize(&record_bytes)
                .map_err(|_x| SegmentError::SerializationError)?;

            Ok(record)
        }

        pub async fn close(self) -> Result<(), SegmentError<T, S>> {
            self.store.close().await.map_err(SegmentError::StoreError)
        }

        pub async fn remove(self) -> Result<(), SegmentError<T, S>> {
            self.store.remove().await.map_err(SegmentError::StoreError)
        }
    }

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
        pub fn with_offset(
            segment: &'a Segment<T, S>,
            offset: u64,
        ) -> Result<Self, SegmentError<T, S>> {
            Ok(Self {
                store_scanner: StoreScanner::with_position(
                    segment.store(),
                    segment
                        .store_position(offset)
                        .ok_or(SegmentError::OffsetOutOfBounds)?,
                ),
            })
        }

        pub fn new(segment: &'a Segment<T, S>) -> Result<Self, SegmentError<T, S>> {
            Self::with_offset(segment, segment.base_offset())
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
            if let Some(record_bytes) = self.store_scanner.next().await {
                return bincode::deserialize(&record_bytes).ok();
            }

            None
        }
    }

    pub mod config {
        use serde::{Deserialize, Serialize};

        // Configuration pertaining to segment storage and buffer sizes.
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


#[cfg(target_os = "linux")]
pub mod glommio_impl;
