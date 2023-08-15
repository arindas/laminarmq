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

/// Extension used by backing files for [`Index`](Index) instances.
pub const INDEX_FILE_EXTENSION: &str = "index";

/// Number of bytes required for storing the base marker.
pub const INDEX_BASE_MARKER_LENGTH: usize = 16;

/// Number of bytes required for storing the record header.
pub const INDEX_RECORD_LENGTH: usize = 16;

/// Lowest underlying storage position
pub const INDEX_BASE_POSITION: u64 = 0;

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

pub struct IndexBaseMarker {
    pub base_index: u64,
    _padding: u64,
}

impl IndexBaseMarker {
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
        let index_record_bytes = source
            .read(
                position,
                &<S::Size as FromPrimitive>::from_usize(REPR_SIZE)
                    .ok_or(IndexError::IncompatibleSizeType)?,
            )
            .await
            .map_err(IndexError::StorageError)?;

        let mut cursor = Cursor::new(index_record_bytes.deref());

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

pub struct Index<S, Idx> {
    index_records: Vec<IndexRecord>,
    base_index: Idx,
    next_index: Idx,
    storage: S,
}

impl<S, Idx> Index<S, Idx> {
    pub fn into_storage(self) -> S {
        self.storage
    }
}

impl<S, Idx> Index<S, Idx>
where
    S: Storage,
    Idx: Unsigned + FromPrimitive + Copy + Eq,
{
    async fn with_storage_and_base_index_option(
        storage: S,
        base_index: Option<Idx>,
    ) -> Result<Self, IndexError<S::Error>> {
        let index_base_marker =
            PersistentSizedRecord::<IndexBaseMarker, INDEX_BASE_MARKER_LENGTH>::read_at(
                &storage,
                &u64_as_position!(INDEX_BASE_POSITION, S::Position)?,
            )
            .await
            .map(|x| x.into_inner());
        let read_base_index = index_base_marker.map(|x| x.base_index);

        let mut position = INDEX_BASE_MARKER_LENGTH as u64;
        let mut index_records = Vec::<IndexRecord>::new();

        while let Ok(index_record) =
            PersistentSizedRecord::<IndexRecord, INDEX_RECORD_LENGTH>::read_at(
                &storage,
                &u64_as_position!(position, S::Position)?,
            )
            .await
        {
            index_records.push(index_record.into_inner());
            position += INDEX_RECORD_LENGTH as u64;
        }

        let base_index = match (read_base_index, base_index) {
            (Err(_), None) => Err(IndexError::NoBaseIndexFound),
            (Err(_), Some(base_index)) => Ok(base_index),
            (Ok(base_index), None) => u64_as_idx!(base_index, Idx),
            (Ok(read), Some(provided)) if u64_as_idx!(read, Idx)? != provided => {
                Err(IndexError::BaseIndexMismatch)
            }
            (Ok(_), Some(provided)) => Ok(provided),
        }?;

        let len = index_records.len() as u64;

        let next_index = base_index + u64_as_idx!(len, Idx)?;

        Ok(Self {
            index_records,
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

    pub fn with_storage_index_records_and_base_index(
        storage: S,
        index_records: Vec<IndexRecord>,
        base_index: Idx,
    ) -> Result<Self, IndexError<S::Error>> {
        let len = index_records.len() as u64;
        let next_index = base_index + u64_as_idx!(len, Idx)?;

        Ok(Self {
            index_records,
            base_index,
            next_index,
            storage,
        })
    }

    pub fn into_storage_index_records_and_base_index(self) -> (S, Vec<IndexRecord>, Idx) {
        (self.storage, self.index_records, self.base_index)
    }
}

impl<S, Idx> Index<S, Idx>
where
    S: Default,
    Idx: Copy,
{
    pub fn with_base_index(base_index: Idx) -> Self {
        Self {
            index_records: Vec::new(),
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
        let vec_index = self
            .normalize_index(idx)
            .ok_or(IndexError::IndexOutOfBounds)?
            .to_usize()
            .ok_or(IndexError::IncompatibleIdxType)?;

        self.index_records
            .get(vec_index)
            .ok_or(IndexError::IndexGapEncountered)
            .map(|&x| x)
    }
}

impl<S, Idx> Index<S, Idx>
where
    S: Storage,
    Idx: Unsigned + ToPrimitive + Copy,
{
    pub async fn append(&mut self, index_record: IndexRecord) -> Result<Idx, IndexError<S::Error>> {
        let write_index = self.next_index;

        // if index is empty, first write index base marker
        if write_index == self.base_index {
            let base_index = self.base_index;
            let base_index = idx_as_u64!(base_index, Idx)?;
            PersistentSizedRecord::<IndexBaseMarker, INDEX_BASE_MARKER_LENGTH>(
                IndexBaseMarker::new(base_index),
            )
            .append_to(&mut self.storage)
            .await?;
        }

        PersistentSizedRecord::<IndexRecord, INDEX_RECORD_LENGTH>(index_record)
            .append_to(&mut self.storage)
            .await?;
        self.index_records.push(index_record);

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
        let vec_index = self
            .normalize_index(idx)
            .ok_or(IndexError::IndexOutOfBounds)?
            .to_usize()
            .ok_or(IndexError::IncompatibleIdxType)?;

        let truncate_position = (INDEX_BASE_MARKER_LENGTH + INDEX_RECORD_LENGTH * vec_index) as u64;
        let truncate_position = u64_as_position!(truncate_position, S::Position)?;

        self.storage
            .truncate(&truncate_position)
            .await
            .map_err(IndexError::StorageError)?;

        self.index_records.truncate(vec_index);

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
    use std::{future::Future, hash::Hasher, marker::PhantomData, ops::Deref};

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
        record_source
            .map(|x| RecordHeader::compute::<H>(x.deref()))
            .scan((0, 0), |(index, position), record_header| {
                let index_record =
                    IndexRecord::with_position_and_record_header::<u32>(*position, record_header)
                        .unwrap();

                *index += 1;
                *position += record_header.length as u32;

                Some(index_record)
            })
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

        let mut index = Index::with_storage_and_base_index(
            test_storage_provider().await.0.storage,
            Idx::zero(),
        )
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
