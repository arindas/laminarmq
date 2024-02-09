//! Provides components necessary for mapping record indices to store-file positions in segments.
//!
//! This module provides [`Index`] which maintains a mapping from logical record indices to
//! positions on the [`Store`](super::store::Store) of a [`Segment`](super::Segment).

use super::{
    super::super::{AsyncConsume, AsyncIndexedRead, AsyncTruncate, Sizable, Storage},
    store::common::RecordHeader,
};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use num::{CheckedSub, FromPrimitive, ToPrimitive, Unsigned};
use std::{
    io::{Cursor, Read, Write},
    ops::Deref,
};

/// Extension used by backing files for [Index] instances.
pub const INDEX_FILE_EXTENSION: &str = "index";

/// Number of bytes required for storing the base marker.
pub const INDEX_BASE_MARKER_LENGTH: usize = 16;

/// Number of bytes required for storing the record header.
pub const INDEX_RECORD_LENGTH: usize = 16;

/// Lowest underlying storage position
pub const INDEX_BASE_POSITION: u64 = 0;

/// Unit of storage on an Index. Stores position, length and checksum metadata for a single
/// [`Record`](super::Record) persisted in the `segment` [`Store`](super::store::Store).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IndexRecord {
    pub checksum: u64,
    pub length: u32,
    pub position: u32,
}

impl From<IndexRecord> for RecordHeader {
    fn from(value: IndexRecord) -> Self {
        RecordHeader {
            checksum: value.checksum,
            length: value.length as u64,
        }
    }
}

/// Marker to persist the starting `base_index` of an [`Index`] in the index [`Storage`].
pub struct IndexBaseMarker {
    pub base_index: u64,
    _padding: u64,
}

impl IndexBaseMarker {
    /// Creates a new [`IndexBaseMarker`] with the given `base_index`.
    pub fn new(base_index: u64) -> Self {
        Self {
            base_index,
            _padding: 0,
        }
    }
}

trait SizedRecord: Sized {
    fn write<W: Write>(&self, dest: &mut W) -> std::io::Result<()>;

    fn read<R: Read>(source: &mut R) -> std::io::Result<Self>;
}

struct PersistentSizedRecord<SR, const REPR_SIZE: usize>(SR);

impl<SR, const REPR_SIZE: usize> PersistentSizedRecord<SR, REPR_SIZE> {
    fn into_inner(self) -> SR {
        self.0
    }
}

impl<SR: SizedRecord, const REPR_SIZE: usize> PersistentSizedRecord<SR, REPR_SIZE> {
    async fn read_at<S>(source: &S, position: &S::Position) -> Result<Self, IndexError<S::Error>>
    where
        S: Storage,
    {
        let record_bytes = source
            .read(
                position,
                &<S::Size as FromPrimitive>::from_usize(REPR_SIZE)
                    .ok_or(IndexError::IncompatibleSizeType)?,
            )
            .await
            .map_err(IndexError::StorageError)?;

        let mut cursor = Cursor::new(record_bytes.deref());

        SR::read(&mut cursor).map(Self).map_err(IndexError::IoError)
    }

    async fn append_to<S>(&self, dest: &mut S) -> Result<S::Position, IndexError<S::Error>>
    where
        S: Storage,
    {
        let mut buffer = [0_u8; REPR_SIZE];
        let mut cursor = Cursor::new(&mut buffer as &mut [u8]);

        self.0.write(&mut cursor).map_err(IndexError::IoError)?;

        let (position, _) = dest
            .append_slice(&buffer)
            .await
            .map_err(IndexError::StorageError)?;

        Ok(position)
    }
}

impl SizedRecord for IndexRecord {
    fn write<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        dest.write_u64::<LittleEndian>(self.checksum)?;
        dest.write_u32::<LittleEndian>(self.length)?;
        dest.write_u32::<LittleEndian>(self.position)?;

        Ok(())
    }

    fn read<R: Read>(source: &mut R) -> std::io::Result<Self> {
        let checksum = source.read_u64::<LittleEndian>()?;
        let length = source.read_u32::<LittleEndian>()?;
        let position = source.read_u32::<LittleEndian>()?;

        Ok(IndexRecord {
            checksum,
            length,
            position,
        })
    }
}

impl SizedRecord for IndexBaseMarker {
    fn write<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        dest.write_u64::<LittleEndian>(self.base_index)?;

        Ok(())
    }

    fn read<R: Read>(source: &mut R) -> std::io::Result<Self> {
        let base_index = source.read_u64::<LittleEndian>()?;

        Ok(Self {
            base_index,
            _padding: 0_u64,
        })
    }
}

/// Error type associated with operations on [`Index`].
#[derive(Debug)]
pub enum IndexError<StorageError> {
    StorageError(StorageError),
    IoError(std::io::Error),
    IncompatiblePositionType,
    IncompatibleSizeType,
    IncompatibleIdxType,
    IndexOutOfBounds,
    IndexGapEncountered,
    NoBaseIndexFound,
    BaseIndexMismatch,
    InconsistentIndexSize,
}

impl<StorageError> std::fmt::Display for IndexError<StorageError>
where
    StorageError: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl<StorageError> std::error::Error for IndexError<StorageError> where
    StorageError: std::error::Error
{
}

#[doc(hidden)]
macro_rules! u64_as_position {
    ($value:ident, $PosType:ty) => {
        <$PosType as FromPrimitive>::from_u64($value).ok_or(IndexError::IncompatiblePositionType)
    };
}

#[doc(hidden)]
macro_rules! u64_as_idx {
    ($value:ident, $IdxType:ty) => {
        <$IdxType as FromPrimitive>::from_u64($value).ok_or(IndexError::IncompatibleIdxType)
    };
}

#[doc(hidden)]
macro_rules! idx_as_u64 {
    ($value:ident, $IdxType:ty) => {
        <$IdxType as ToPrimitive>::to_u64(&$value).ok_or(IndexError::IncompatibleIdxType)
    };
}

impl IndexRecord {
    /// Creates a new [`IndexRecord`] with the given position and [`RecordHeader`].
    pub fn with_position_and_record_header<P: ToPrimitive>(
        position: P,
        record_header: RecordHeader,
    ) -> Option<IndexRecord> {
        Some(IndexRecord {
            checksum: record_header.checksum,
            length: u32::try_from(record_header.length).ok()?,
            position: P::to_u32(&position)?,
        })
    }
}

/// [`Index`] for every [`Segment`](super::Segment) in a [`SegmentedLog`](super::SegmentedLog).
///
/// [`Index`] stores a mapping from [`Record`](super::Record) logical indices to positions on its
/// parent [`Segment`](super::Segment) instance's underlying [`Store`](super::store::Store). It
/// acts as an index to position translation-table when reading record contents from the underlying
/// [`Store`](super::store::Store) using the record's logical index.
///
/// <p align="center">
/// <img src="https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-indexed-segmented-log-segment.drawio.png" alt="segmented_log_segment" />
/// </p>
/// <p align="center">
/// <b>Fig:</b> <code>Segment</code> diagram showing <code>Index</code>, mapping logical indices
/// to<code>Store</code> positions.
/// </p>
///
/// [`Index`] also stores checksum and length information for every record with [`RecordHeader`].
/// This information is used to detect any data corruption on the underlying persistent media when
/// reading from [`Store`](super::store::Store).
///
/// ### Type parameters
/// - `S`
///     Underlying [`Storage`] implementation for storing [`IndexRecord`] instances
/// - `Idx`
///     Type to use for representing logical indices. (Usually an unsigned integer like u32, u64
///     usize, etc.)
pub struct Index<S, Idx> {
    index_records: Option<Vec<IndexRecord>>,
    base_index: Idx,
    next_index: Idx,
    storage: S,
}

impl<S, Idx> Index<S, Idx> {
    /// Maps this [`Index`] to the underlying [`Storage`] implementation instance.
    pub fn into_storage(self) -> S {
        self.storage
    }

    /// Obtains the logical index of the first record in this [`Index`].
    pub fn base_index(&self) -> &Idx {
        &self.base_index
    }
}

impl<S, Idx> Index<S, Idx>
where
    S: Storage,
    Idx: Unsigned + FromPrimitive + Copy + Eq,
{
    /// Returns the estimated number of [`IndexRecord`] instances stored in the given [`Storage`].
    ///
    /// This function calculates this estimate by using the [`Storage`] size and the size of a
    /// single [`IndexRecord`].
    pub fn estimated_index_records_len_in_storage(
        storage: &S,
    ) -> Result<usize, IndexError<S::Error>> {
        let index_storage_size = storage
            .size()
            .to_usize()
            .ok_or(IndexError::IncompatibleSizeType)?;

        let estimated_index_records_len =
            index_storage_size.saturating_sub(INDEX_BASE_MARKER_LENGTH) / INDEX_RECORD_LENGTH;

        Ok(estimated_index_records_len)
    }

    /// Reads and returns the `base_index` of the [`Index`] persisted on the provided [`Storage`]
    /// instance by reading the [`IndexBaseMarker`] at [`INDEX_BASE_POSITION`].
    pub async fn base_index_from_storage(storage: &S) -> Result<Idx, IndexError<S::Error>> {
        let index_base_marker =
            PersistentSizedRecord::<IndexBaseMarker, INDEX_BASE_MARKER_LENGTH>::read_at(
                storage,
                &u64_as_position!(INDEX_BASE_POSITION, S::Position)?,
            )
            .await
            .map(|x| x.into_inner());

        index_base_marker
            .map(|x| x.base_index)
            .and_then(|x| u64_as_idx!(x, Idx))
    }

    pub async fn index_records_from_storage(
        storage: &S,
    ) -> Result<Vec<IndexRecord>, IndexError<S::Error>> {
        let mut position = INDEX_BASE_MARKER_LENGTH as u64;

        let estimated_index_records_len = Self::estimated_index_records_len_in_storage(storage)?;

        let mut index_records = Vec::<IndexRecord>::with_capacity(estimated_index_records_len);

        while let Ok(index_record) =
            PersistentSizedRecord::<IndexRecord, INDEX_RECORD_LENGTH>::read_at(
                storage,
                &u64_as_position!(position, S::Position)?,
            )
            .await
        {
            index_records.push(index_record.into_inner());
            position += INDEX_RECORD_LENGTH as u64;
        }

        index_records.shrink_to_fit();

        if index_records.len() != estimated_index_records_len {
            Err(IndexError::InconsistentIndexSize)
        } else {
            Ok(index_records)
        }
    }

    pub async fn validated_base_index(
        storage: &S,
        base_index: Option<Idx>,
    ) -> Result<Idx, IndexError<S::Error>> {
        let read_base_index = Self::base_index_from_storage(storage).await.ok();

        match (read_base_index, base_index) {
            (None, None) => Err(IndexError::NoBaseIndexFound),
            (None, Some(base_index)) => Ok(base_index),
            (Some(base_index), None) => Ok(base_index),
            (Some(read), Some(provided)) if read != provided => Err(IndexError::BaseIndexMismatch),
            (Some(_), Some(provided)) => Ok(provided),
        }
    }

    pub async fn with_storage_and_base_index_option(
        storage: S,
        base_index: Option<Idx>,
    ) -> Result<Self, IndexError<S::Error>> {
        let base_index = Self::validated_base_index(&storage, base_index).await?;

        let index_records = Self::index_records_from_storage(&storage).await?;

        let len = index_records.len() as u64;

        let next_index = base_index + u64_as_idx!(len, Idx)?;

        Ok(Self {
            index_records: Some(index_records),
            base_index,
            next_index,
            storage,
        })
    }

    pub async fn with_storage_and_base_index(
        storage: S,
        base_index: Idx,
    ) -> Result<Self, IndexError<S::Error>> {
        Self::with_storage_and_base_index_option(storage, Some(base_index)).await
    }

    pub async fn with_storage(storage: S) -> Result<Self, IndexError<S::Error>> {
        Self::with_storage_and_base_index_option(storage, None).await
    }

    pub fn with_storage_index_records_option_and_validated_base_index(
        storage: S,
        index_records: Option<Vec<IndexRecord>>,
        validated_base_index: Idx,
    ) -> Result<Self, IndexError<S::Error>> {
        let len = Self::estimated_index_records_len_in_storage(&storage)? as u64;
        let next_index = validated_base_index + u64_as_idx!(len, Idx)?;

        Ok(Self {
            index_records,
            base_index: validated_base_index,
            next_index,
            storage,
        })
    }

    pub fn take_cached_index_records(&mut self) -> Option<Vec<IndexRecord>> {
        self.index_records.take()
    }

    pub fn cached_index_records(&self) -> Option<&Vec<IndexRecord>> {
        self.index_records.as_ref()
    }

    pub async fn cache(&mut self) -> Result<(), IndexError<S::Error>> {
        if self.index_records.as_ref().is_some() {
            return Ok(());
        }

        self.index_records = Some(Self::index_records_from_storage(&self.storage).await?);

        Ok(())
    }
}

impl<S, Idx> Index<S, Idx>
where
    S: Default,
    Idx: Copy,
{
    pub fn with_base_index(base_index: Idx) -> Self {
        Self {
            index_records: Some(Vec::new()),
            base_index,
            next_index: base_index,
            storage: S::default(),
        }
    }
}

impl<S: Storage, Idx> Sizable for Index<S, Idx> {
    type Size = S::Size;

    fn size(&self) -> Self::Size {
        self.storage.size()
    }
}

impl<S: Storage, Idx> Index<S, Idx> {
    #[inline]
    fn index_record_position(normalized_index: usize) -> Result<S::Position, IndexError<S::Error>> {
        let position = (INDEX_BASE_MARKER_LENGTH + INDEX_RECORD_LENGTH * normalized_index) as u64;
        u64_as_position!(position, S::Position)
    }
}

impl<S, Idx> Index<S, Idx>
where
    S: Storage,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
{
    #[inline]
    fn internal_normalized_index(&self, idx: &Idx) -> Result<usize, IndexError<S::Error>> {
        self.normalize_index(idx)
            .ok_or(IndexError::IndexOutOfBounds)?
            .to_usize()
            .ok_or(IndexError::IncompatibleIdxType)
    }
}

#[async_trait::async_trait(?Send)]
impl<S, Idx> AsyncIndexedRead for Index<S, Idx>
where
    S: Storage,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
{
    type ReadError = IndexError<S::Error>;

    type Idx = Idx;

    type Value = IndexRecord;

    fn highest_index(&self) -> Self::Idx {
        self.next_index
    }

    fn lowest_index(&self) -> Self::Idx {
        self.base_index
    }

    async fn read(&self, idx: &Self::Idx) -> Result<Self::Value, Self::ReadError> {
        let normalized_index = self.internal_normalized_index(idx)?;

        if let Some(index_records) = self.index_records.as_ref() {
            index_records
                .get(normalized_index)
                .ok_or(IndexError::IndexGapEncountered)
                .map(|&x| x)
        } else {
            PersistentSizedRecord::<IndexRecord, INDEX_RECORD_LENGTH>::read_at(
                &self.storage,
                &Self::index_record_position(normalized_index)?,
            )
            .await
            .map(|x| x.into_inner())
        }
    }
}

impl<S, Idx> Index<S, Idx>
where
    S: Storage,
    Idx: Unsigned + ToPrimitive + Copy,
{
    pub async fn append(&mut self, index_record: IndexRecord) -> Result<Idx, IndexError<S::Error>> {
        let write_index = self.next_index;

        if write_index == self.base_index {
            PersistentSizedRecord::<IndexBaseMarker, INDEX_BASE_MARKER_LENGTH>(
                IndexBaseMarker::new(idx_as_u64!(write_index, Idx)?),
            )
            .append_to(&mut self.storage)
            .await?;
        }

        PersistentSizedRecord::<IndexRecord, INDEX_RECORD_LENGTH>(index_record)
            .append_to(&mut self.storage)
            .await?;

        if let Some(index_records) = self.index_records.as_mut() {
            index_records.push(index_record);
        }

        self.next_index = write_index + Idx::one();
        Ok(write_index)
    }
}

#[async_trait::async_trait(?Send)]
impl<S, Idx> AsyncTruncate for Index<S, Idx>
where
    S: Storage,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
{
    type TruncError = IndexError<S::Error>;

    type Mark = Idx;

    async fn truncate(&mut self, idx: &Self::Mark) -> Result<(), Self::TruncError> {
        let normalized_index = self.internal_normalized_index(idx)?;

        self.storage
            .truncate(&Self::index_record_position(normalized_index)?)
            .await
            .map_err(IndexError::StorageError)?;

        if let Some(index_records) = self.index_records.as_mut() {
            index_records.truncate(normalized_index);
        }

        self.next_index = *idx;

        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl<S: Storage, Idx> AsyncConsume for Index<S, Idx> {
    type ConsumeError = IndexError<S::Error>;

    async fn remove(self) -> Result<(), Self::ConsumeError> {
        self.storage
            .remove()
            .await
            .map_err(IndexError::StorageError)
    }

    async fn close(self) -> Result<(), Self::ConsumeError> {
        self.storage.close().await.map_err(IndexError::StorageError)
    }
}

pub(crate) mod test {
    use super::{
        super::{
            super::super::{
                common::{_TestStorage, indexed_read_stream},
                AsyncConsume, AsyncIndexedRead, AsyncTruncate,
            },
            store::test::_RECORDS,
        },
        Index, IndexError, IndexRecord, RecordHeader, Storage,
    };
    use futures_lite::StreamExt;
    use num::{CheckedSub, FromPrimitive, ToPrimitive, Unsigned, Zero};
    use std::{future::Future, hash::Hasher, marker::PhantomData};

    fn _test_records_provider<'a, const N: usize>(
        record_source: &'a [&'a [u8; N]],
    ) -> impl Iterator<Item = &'a [u8]> {
        record_source.iter().cloned().map(|x| {
            let x: &[u8] = x;
            x
        })
    }

    fn _test_index_records_provider<'a, H>(
        record_source: impl Iterator<Item = &'a [u8]> + 'a,
    ) -> impl Iterator<Item = IndexRecord> + 'a
    where
        H: Hasher + Default,
    {
        record_source.map(|x| RecordHeader::compute::<H>(x)).scan(
            (0, 0),
            |(index, position), record_header| {
                let index_record =
                    IndexRecord::with_position_and_record_header::<u32>(*position, record_header)
                        .unwrap();

                *index += 1;
                *position += record_header.length as u32;

                Some(index_record)
            },
        )
    }

    async fn _test_index_contains_records<S, Idx, I>(
        index: &Index<S, Idx>,
        index_records: I,
        expected_record_count: usize,
    ) where
        S: Storage,
        I: Iterator<Item = IndexRecord>,
        Idx: Copy + Ord,
        Idx: Unsigned + CheckedSub,
        Idx: ToPrimitive,
    {
        let count = futures_lite::stream::iter(index_records)
            .zip(indexed_read_stream(index, ..))
            .map(|(x, y)| {
                assert_eq!(x, y);
                Some(())
            })
            .count()
            .await;
        assert_eq!(count, expected_record_count);
    }

    pub(crate) async fn _test_index_read_append_truncate_consistency<TSP, F, S, H, Idx>(
        test_storage_provider: TSP,
    ) where
        F: Future<Output = (_TestStorage<S>, PhantomData<(H, Idx)>)>,
        TSP: Fn() -> F,
        S: Storage,
        S::Position: Zero,
        H: Hasher + Default,
        Idx: Copy + Ord,
        Idx: Unsigned + CheckedSub,
        Idx: ToPrimitive + FromPrimitive,
    {
        let _TestStorage {
            storage,
            persistent: storage_is_persistent,
        } = test_storage_provider().await.0;

        match Index::<S, Idx>::with_storage(storage).await {
            Err(IndexError::NoBaseIndexFound) => {},
            _ => unreachable!("Wrong result returned when creating from empty storage without providing base index"),
        }

        let base_index = Idx::zero();

        let mut index =
            Index::with_storage_and_base_index(test_storage_provider().await.0.storage, base_index)
                .await
                .unwrap();

        match index.read(&Idx::zero()).await {
            Err(IndexError::IndexOutOfBounds) => {}
            _ => unreachable!("Wrong result returned for read on empty Index."),
        }

        for index_record in _test_index_records_provider::<H>(_test_records_provider(&_RECORDS)) {
            index.append(index_record).await.unwrap();
        }

        _test_index_contains_records(
            &index,
            _test_index_records_provider::<H>(_test_records_provider(&_RECORDS)),
            _RECORDS.len(),
        )
        .await;

        let index = if storage_is_persistent {
            index.close().await.unwrap();
            Index::<S, Idx>::with_storage(test_storage_provider().await.0.storage)
                .await
                .unwrap()
        } else {
            Index::<S, Idx>::with_storage(index.into_storage())
                .await
                .unwrap()
        };

        let index = Index::<S, Idx>::with_storage_index_records_option_and_validated_base_index(
            index.into_storage(),
            None,
            base_index,
        )
        .unwrap();

        _test_index_contains_records(
            &index,
            _test_index_records_provider::<H>(_test_records_provider(&_RECORDS)),
            _RECORDS.len(),
        )
        .await;

        let mut index =
            Index::<S, Idx>::with_storage_and_base_index(index.into_storage(), Idx::zero())
                .await
                .unwrap();

        _test_index_contains_records(
            &index,
            _test_index_records_provider::<H>(_test_records_provider(&_RECORDS)),
            _RECORDS.len(),
        )
        .await;

        let truncate_index = _RECORDS.len() / 2;

        index
            .truncate(&Idx::from_usize(truncate_index).unwrap())
            .await
            .unwrap();

        assert_eq!(index.len().to_usize().unwrap(), truncate_index);

        _test_index_contains_records(
            &index,
            _test_index_records_provider::<H>(_test_records_provider(&_RECORDS))
                .take(truncate_index),
            truncate_index,
        )
        .await;

        index.remove().await.unwrap();
    }
}
