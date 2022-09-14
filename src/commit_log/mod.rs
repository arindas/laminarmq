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
            self.store.read(self.position).await.ok().map(|record| {
                self.position += common::header_padded_record_length(&record);
                record
            })
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

        pub fn header_padded_record_length(record: &[u8]) -> u64 {
            (record.len() + RECORD_HEADER_LENGTH) as u64
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
    use std::{fmt::Display, marker::PhantomData, ops::Deref, time::Instant};

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

        creation_time: Instant,

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

        pub fn base_offset(&self) -> u64 {
            self.base_offset
        }

        pub fn next_offset(&self) -> u64 {
            self.next_offset
        }

        pub fn store(&self) -> &S {
            &self.store
        }

        pub fn store_position(&self, offset: u64) -> Option<u64> {
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

        #[inline]
        pub async fn remove(self) -> Result<(), SegmentError<T, S>> {
            self.store.remove().await.map_err(SegmentError::StoreError)
        }

        #[inline]
        pub async fn close(self) -> Result<(), SegmentError<T, S>> {
            self.store.close().await.map_err(SegmentError::StoreError)
        }

        pub fn creation_time(&self) -> Instant {
            self.creation_time
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
        pub fn new(segment: &'a Segment<T, S>) -> Result<Self, SegmentError<T, S>> {
            Self::with_offset(segment, segment.base_offset())
        }

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

#[async_trait(?Send)]
pub trait CommitLog {
    type Error;

    fn highest_offset(&self) -> u64;

    fn lowest_offset(&self) -> u64;

    #[inline]
    fn has_offset(&self, offset: u64) -> bool {
        offset >= self.lowest_offset() && offset < self.highest_offset()
    }

    async fn append(&mut self, record: &mut Record) -> Result<u64, Self::Error>;

    async fn read(&self, offset: u64) -> Result<Record, Self::Error>;

    async fn remove(self) -> Result<(), Self::Error>;

    async fn close(self) -> Result<(), Self::Error>;
}

pub struct LogScanner<'a, Log: CommitLog> {
    log: &'a Log,
    offset: u64,
}

impl<'a, Log: CommitLog> LogScanner<'a, Log> {
    pub fn with_offset(log: &'a Log, offset: u64) -> Option<Self> {
        if !log.has_offset(offset) {
            None
        } else {
            Some(LogScanner { log, offset })
        }
    }

    pub fn new(log: &'a Log) -> Option<Self> {
        Self::with_offset(log, log.lowest_offset())
    }
}

#[async_trait(?Send)]
impl<'a, Log: CommitLog> Scanner for LogScanner<'a, Log> {
    type Item = Record;

    async fn next(&mut self) -> Option<Self::Item> {
        self.log.read(self.offset).await.ok().and_then(|record| {
            self.offset = record.next_offset()?;
            Some(record)
        })
    }
}

pub mod segmented_log {
    use std::{
        error::Error,
        fmt::Display,
        fs,
        ops::Deref,
        path::{Path, PathBuf},
        time::Duration,
    };

    use async_trait::async_trait;

    use self::config::SegmentedLogConfig;

    use super::{
        segment::{self, Segment, SegmentError},
        store, Record,
    };

    #[derive(Debug)]
    pub enum SegmentedLogError<T, S>
    where
        T: Deref<Target = [u8]>,
        S: store::Store<T>,
    {
        SegmentError(segment::SegmentError<T, S>),
        IoError(std::io::Error),
        WriteSegmentLost,
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
            }
        }
    }

    impl<T, S> Error for SegmentedLogError<T, S>
    where
        T: Deref<Target = [u8]> + std::fmt::Debug,
        S: store::Store<T> + std::fmt::Debug,
    {
    }

    #[async_trait(?Send)]
    pub trait SegmentCreator<T, S>
    where
        T: Deref<Target = [u8]>,
        S: store::Store<T>,
    {
        async fn new_segment_with_store_file_path_offset_and_config<P: AsRef<Path>>(
            &self,
            store_file_path: P,
            base_offset: u64,
            config: segment::config::SegmentConfig,
        ) -> Result<Segment<T, S>, segment::SegmentError<T, S>>;

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

    pub struct SegmentedLog<T, S, SegC>
    where
        T: Deref<Target = [u8]>,
        S: store::Store<T>,
        SegC: SegmentCreator<T, S>,
    {
        write_segment: Option<Segment<T, S>>,
        read_segments: Vec<Segment<T, S>>,

        storage_directory: PathBuf,
        config: SegmentedLogConfig,

        segment_creator: SegC,
    }

    impl<T, S, SegC> SegmentedLog<T, S, SegC>
    where
        T: Deref<Target = [u8]>,
        S: store::Store<T>,
        SegC: SegmentCreator<T, S>,
    {
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

        #[inline]
        pub fn is_write_segment_maxed(&self) -> Result<bool, SegmentedLogError<T, S>> {
            self.write_segment
                .as_ref()
                .map(|x| x.is_maxed())
                .ok_or(SegmentedLogError::WriteSegmentLost)
        }

        pub async fn new<P: AsRef<Path>>(
            storage_directory_path: P,
            config: SegmentedLogConfig,
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

        pub async fn remove_expired_segments(
            &mut self,
            expiry_duration: Duration,
        ) -> Result<(), SegmentedLogError<T, S>> {
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

            let expired_read_segments =
                std::mem::replace(&mut self.read_segments, non_expired_read_segments);

            for segment in expired_read_segments {
                segment
                    .remove()
                    .await
                    .map_err(SegmentedLogError::SegmentError)?;
            }

            let write_segment_ref = self
                .write_segment
                .as_ref()
                .ok_or(SegmentedLogError::WriteSegmentLost)?;

            if write_segment_ref.creation_time().elapsed() > expiry_duration {
                let new_segment_base_offset = write_segment_ref.next_offset();
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

                old_write_segment
                    .ok_or(SegmentedLogError::WriteSegmentLost)?
                    .remove()
                    .await
                    .map_err(SegmentedLogError::SegmentError)?;
            }

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

        async fn read(&self, offset: u64) -> Result<Record, Self::Error> {
            if !self.has_offset(offset) {
                return Err(SegmentedLogError::SegmentError(
                    SegmentError::OffsetOutOfBounds,
                ));
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
                .map(|x| x.base_offset())
                .or(self.write_segment.as_ref().map(|x| x.base_offset()))
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

        /// Returns the backing [`crate::log::store::Store`] file path, with the given Log storage
        /// directory and the given segment base offset.
        #[inline]
        pub fn store_file_path<P: AsRef<Path>>(storage_dir_path: P, offset: u64) -> PathBuf {
            storage_dir_path.as_ref().join(format!(
                "{}.{}",
                offset,
                super::store::common::STORE_FILE_EXTENSION
            ))
        }

        /// Returns an iterator of the paths of all the [`crate::log::store::Store`] backing files in the
        /// given directory.
        ///
        /// ## Errors
        /// - This functions returns an error if the given storage directory doesn't exist and couldn't
        /// be created.
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

        /// Returns the given store paths sorted by the base offset of the respective segments that they
        /// belong to.
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
