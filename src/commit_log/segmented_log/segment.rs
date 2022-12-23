//! Module providing cross platform [`Segment`](Segment) abstractions for reading and writing
//! [`Record`](super::Record) instances from [`Store`](super::store::Store) instances.

use std::{fmt::Display, marker::PhantomData, ops::Deref, time::Instant};

use futures_core::Stream;

use crate::{commit_log::Record, common::split::SplitAt};

use super::{store::Store, RecordMetadata};

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
    /// by the [`config::SegmentConfig::max_store_bytes`] limit.
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
/// [`SegmentedLog`](super::SegmentedLog).
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
pub struct Segment<T, M, S: Store<T>>
where
    T: Deref<Target = [u8]> + SplitAt<u8>,
    M: Default + serde::Serialize + serde::de::DeserializeOwned,
{
    store: S,
    base_offset: u64,
    next_offset: u64,
    config: config::SegmentConfig,

    creation_time: Instant,

    _phantom_data: PhantomData<(T, M)>,
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

impl<T, M, S: Store<T>> Segment<T, M, S>
where
    T: Deref<Target = [u8]> + SplitAt<u8>,
    M: Default + serde::Serialize + serde::de::DeserializeOwned,
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
    /// The offset at which the record was written, along with the number of bytes written.
    ///
    /// ## Errors
    /// - [`SegmentError::SegmentMaxed`] if [`Self::is_maxed`] returns true.
    /// - [`SegmentError::SerializationError`] if there was an error during binary encoding the
    /// given record instance.
    /// - [`SegmentError::StoreError`] if there was an error during writing to the underlying
    /// [`Store`](super::store::Store) instance.
    pub async fn append(
        &mut self,
        record_bytes: &[u8],
        metadata: RecordMetadata<M>,
    ) -> Result<(u64, usize), SegmentError<T, S>> {
        if self.is_maxed() {
            return Err(SegmentError::SegmentMaxed);
        }

        let metadata_bytes =
            bincode::serialize(&metadata).map_err(|_| SegmentError::SerializationError)?;

        let record_parts: [&[u8]; 2] = [&metadata_bytes, record_bytes];

        let (write_position, bytes_written) = self
            .store
            .append_multipart(&record_parts)
            .await
            .map_err(SegmentError::StoreError)?;

        self.next_offset += bytes_written as u64;

        Ok((self.base_offset() + write_position, bytes_written))
    }

    pub fn offset_within_bounds(&self, offset: u64) -> bool {
        offset >= self.base_offset() && offset < self.next_offset()
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
    pub async fn read(
        &self,
        offset: u64,
    ) -> Result<(Record<RecordMetadata<M>, T>, u64), SegmentError<T, S>> {
        if !self.offset_within_bounds(offset) {
            return Err(SegmentError::OffsetOutOfBounds);
        }

        let position = self
            .store_position(offset)
            .ok_or(SegmentError::OffsetBeyondCapacity)?;

        let metadata_size = bincode::serialized_size(&RecordMetadata::<M>::default())
            .map_err(|_| SegmentError::SerializationError)? as usize;

        let (record_bytes, next_record_position) = self
            .store
            .read(position)
            .await
            .map_err(SegmentError::StoreError)?;

        let (metadata, record_bytes) = record_bytes
            .split_at(metadata_size)
            .ok_or(SegmentError::SerializationError)?;

        let metadata =
            bincode::deserialize(&metadata).map_err(|_| SegmentError::SerializationError)?;

        Ok((
            Record {
                metadata,
                value: record_bytes,
            },
            self.base_offset() + next_record_position,
        ))
    }

    /// Advances this [`Segment`] instance's `next_offset` value to the given value.
    /// This method simply returns [`Ok`] if `new_next_offset <= next_offset`.
    ///
    /// ## Errors
    /// - [`SegmentError::OffsetBeyondCapacity`] if the given offset doesn't map to a valid
    /// position on the underlying store.
    pub(crate) fn _advance_to_offset(
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

    pub async fn truncate(&mut self, offset: u64) -> Result<(), SegmentError<T, S>> {
        if !self.offset_within_bounds(offset) {
            return Err(SegmentError::OffsetOutOfBounds);
        }

        let truncate_position = self
            .store_position(offset)
            .ok_or(SegmentError::OffsetBeyondCapacity)?;

        self.store
            .truncate(truncate_position)
            .await
            .map_err(SegmentError::StoreError)?;

        self.next_offset = self.base_offset + self.store().size();

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

/// Returns a [`Stream`] of [`Record`]`s` in the given [`Segment`] starting from the given
/// offset in FIFO order.
pub fn segment_record_stream<'segment, T, M, S>(
    segment: &'segment Segment<T, M, S>,
    from_offset: u64,
) -> impl Stream<Item = Record<RecordMetadata<M>, T>> + 'segment
where
    T: Deref<Target = [u8]> + SplitAt<u8> + 'segment,
    M: Default + serde::Serialize + serde::de::DeserializeOwned,
    S: Store<T>,
{
    async_stream::stream! {
        let mut offset = from_offset;

        while let Ok((record, next_offset)) = segment.read(offset).await {
            yield record;
            offset = next_offset;
        }
    }
}

pub mod config {
    //! Module providing types for configuring [`Segment`](super::Segment) instances.
    use serde::{Deserialize, Serialize};

    /// Configuration pertaining to segment storage and buffer sizes.
    #[derive(Serialize, Deserialize, Default, Debug, Clone, Copy)]
    pub struct SegmentConfig {
        /// Segment store's write buffer size. The write buffer is flushed to the disk when it is
        /// filled. Reads of records in a write buffer are only possible when the write buffer is
        /// flushed.
        pub store_buffer_size: Option<usize>,

        /// Maximum segment storage size, after which this segment is demoted from the current
        /// write segment to a read segment.
        pub max_store_bytes: u64,
    }
}
