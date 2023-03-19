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

/// Extension used by backing files for [`Index`](super::Index) instances.
pub const INDEX_FILE_EXTENSION: &str = "index";

/// Number of bytes required for storing the record header.
pub const INDEX_RECORD_LENGTH: usize = 32;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IndexRecord {
    pub record_header: RecordHeader,
    pub index: u64,
    pub position: u64,
}

impl IndexRecord {
    pub fn read<R: Read>(source: &mut R) -> std::io::Result<IndexRecord> {
        let record_header = RecordHeader::read(source)?;

        let index = source.read_u64::<LittleEndian>()?;
        let position = source.read_u64::<LittleEndian>()?;

        Ok(IndexRecord {
            record_header,
            index,
            position,
        })
    }

    pub fn write<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        self.record_header.write(dest)?;

        dest.write_u64::<LittleEndian>(self.index)?;
        dest.write_u64::<LittleEndian>(self.position)?;

        Ok(())
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
    InvalidAppendIdx,
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

impl IndexRecord {
    async fn read_from_storage_at<S>(
        source: &S,
        position: &S::Position,
    ) -> Result<IndexRecord, IndexError<S::Error>>
    where
        S: Storage,
    {
        let index_record_bytes = source
            .read(
                position,
                &<S::Size as FromPrimitive>::from_usize(INDEX_RECORD_LENGTH)
                    .ok_or(IndexError::IncompatibleSizeType)?,
            )
            .await
            .map_err(IndexError::StorageError)?;

        let mut cursor = Cursor::new(index_record_bytes.deref());

        IndexRecord::read(&mut cursor).map_err(IndexError::IoError)
    }

    async fn append_to_storage<S>(&self, dest: &mut S) -> Result<S::Position, IndexError<S::Error>>
    where
        S: Storage,
    {
        let mut buffer = [0_u8; INDEX_RECORD_LENGTH];
        let mut cursor = Cursor::new(&mut buffer as &mut [u8]);

        self.write(&mut cursor).map_err(IndexError::IoError)?;

        let (position, _) = dest
            .append_slice(&buffer)
            .await
            .map_err(IndexError::StorageError)?;

        Ok(position)
    }
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
    pub fn with_position_index_and_record_header<Position, Idx>(
        position: Position,
        index: Idx,
        record_header: RecordHeader,
    ) -> Option<IndexRecord>
    where
        Position: ToPrimitive,
        Idx: ToPrimitive,
    {
        Some(IndexRecord {
            record_header,
            index: Idx::to_u64(&index)?,
            position: Position::to_u64(&position)?,
        })
    }
}

pub async fn index_records_in_storage<S>(
    source: &S,
) -> Result<Vec<IndexRecord>, IndexError<S::Error>>
where
    S: Storage,
{
    let mut position = 0_u64;
    let mut index_records = Vec::<IndexRecord>::new();

    while let Ok(index_record) =
        IndexRecord::read_from_storage_at(source, &u64_as_position!(position, S::Position)?).await
    {
        index_records.push(index_record);
        position += INDEX_RECORD_LENGTH as u64;
    }

    Ok(index_records)
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
    Idx: FromPrimitive + Copy + Eq,
{
    async fn with_storage_and_base_index_option(
        storage: S,
        base_index: Option<Idx>,
    ) -> Result<Self, IndexError<S::Error>> {
        let index_records = index_records_in_storage(&storage).await?;
        let read_base_index = index_records.first().map(|x| x.index);

        let base_index = match (read_base_index, base_index) {
            (None, None) => Err(IndexError::NoBaseIndexFound),
            (None, Some(base_index)) => Ok(base_index),
            (Some(base_index), None) => u64_as_idx!(base_index, Idx),
            (Some(read), Some(provided)) if u64_as_idx!(read, Idx)? != provided => {
                Err(IndexError::BaseIndexMismatch)
            }
            (Some(_), Some(provided)) => Ok(provided),
        }?;

        let next_index = match index_records.last() {
            Some(index_record) => {
                let idx = index_record.index + 1;
                u64_as_idx!(idx, Idx)
            }
            None => Ok(base_index),
        }?;

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

        if index_record.index != idx_as_u64!(write_index, Idx)? {
            return Err(IndexError::<S::Error>::InvalidAppendIdx);
        }

        index_record.append_to_storage(&mut self.storage).await?;
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

        let truncate_position = (INDEX_RECORD_LENGTH * vec_index) as u64;
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
    use futures_lite::StreamExt;
    use num::{CheckedSub, FromPrimitive, ToPrimitive, Unsigned, Zero};

    use crate::storage::AsyncTruncate;

    use super::{
        super::super::super::{common::indexed_read_stream, AsyncConsume, AsyncIndexedRead},
        Index, IndexError, IndexRecord, RecordHeader, Storage,
    };
    use std::{future::Future, hash::Hasher, marker::PhantomData, ops::Deref};

    use super::super::store::test::_RECORDS;

    fn _index_records_test_data<H>() -> impl Iterator<Item = IndexRecord>
    where
        H: Hasher + Default,
    {
        _RECORDS
            .iter()
            .map(|x| RecordHeader::compute::<H>(x.deref()))
            .scan((0, 0), |(index, position), record_header| {
                let index_record = IndexRecord::with_position_index_and_record_header::<u32, u32>(
                    *position,
                    *index,
                    record_header,
                )
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
            .zip(indexed_read_stream(index, ..).await)
            .map(|(x, y)| {
                assert_eq!(x, y);
                Some(())
            })
            .count()
            .await;
        assert_eq!(count, expected_record_count);
    }

    pub(crate) async fn _test_index_read_append_truncate_consistency<SP, F, S, H, Idx>(
        storage_provider: SP,
    ) where
        F: Future<Output = (S, PhantomData<(H, Idx)>)>,
        SP: Fn() -> F,
        S: Storage,
        S::Position: Zero,
        H: Hasher + Default,
        Idx: Copy + Ord,
        Idx: Unsigned + CheckedSub,
        Idx: ToPrimitive + FromPrimitive,
    {
        match Index::<S, Idx>::with_storage(storage_provider().await.0).await {
            Err(IndexError::NoBaseIndexFound) => {},
            _ => unreachable!("Wrong result returned when creating from empty storage without providing base index"),
        }

        let mut index = Index::with_storage_and_base_index(storage_provider().await.0, Idx::zero())
            .await
            .unwrap();

        match index.read(&Idx::zero()).await {
            Err(IndexError::IndexOutOfBounds) => {}
            _ => unreachable!("Wrong result returned for read on empty Index."),
        }

        for index_record in _index_records_test_data::<H>() {
            index.append(index_record).await.unwrap();
        }

        match index.append(IndexRecord::default()).await {
            Err(IndexError::InvalidAppendIdx) => {}
            _ => unreachable!(
                "Wrong result returned when appending IndexRecord with invalid append index"
            ),
        }

        _test_index_contains_records(&index, _index_records_test_data::<H>(), _RECORDS.len()).await;

        let index = if S::is_persistent() {
            index.close().await.unwrap();
            Index::<S, Idx>::with_storage(storage_provider().await.0)
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

        _test_index_contains_records(&index, _index_records_test_data::<H>(), _RECORDS.len()).await;

        let truncate_index = _RECORDS.len() / 2;

        index
            .truncate(&Idx::from_usize(truncate_index).unwrap())
            .await
            .unwrap();

        assert_eq!(index.len().to_usize().unwrap(), truncate_index);

        _test_index_contains_records(
            &index,
            _index_records_test_data::<H>().take(truncate_index),
            truncate_index,
        )
        .await;

        index.remove().await.unwrap();
    }
}
