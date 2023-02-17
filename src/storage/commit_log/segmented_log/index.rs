use crate::storage::common::write_stream;

use super::super::super::{AsyncConsume, AsyncIndexedRead, AsyncTruncate};
use super::{super::super::Storage, store::common::RecordHeader};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use futures_util::stream;
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
    record_header: RecordHeader,
    index: Option<u64>,
    position: u64,
}

impl IndexRecord {
    pub fn read<R: Read>(source: &mut R) -> std::io::Result<IndexRecord> {
        let record_header = RecordHeader::read(source)?;

        let index = source.read_u64::<LittleEndian>()?;
        let position = source.read_u64::<LittleEndian>()?;

        Ok(IndexRecord {
            record_header,
            index: Some(index),
            position,
        })
    }

    pub fn write<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
        self.record_header.write(dest)?;

        dest.write_u64::<LittleEndian>(self.index.unwrap_or_default())?;
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
        write!(f, "{:?}", self)
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
        let mut buffer = [0 as u8; INDEX_RECORD_LENGTH];
        let mut cursor = Cursor::new(&mut buffer as &mut [u8]);

        self.write(&mut cursor).map_err(IndexError::IoError)?;

        let mut stream = stream::iter(std::iter::once(&buffer as &[u8]));

        let (position, _) = dest
            .append(&mut stream, &mut write_stream)
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
macro_rules! position_as_u64 {
    ($value:ident, $PosType:ty) => {
        <$PosType as ToPrimitive>::to_u64(&$value).ok_or(IndexError::IncompatiblePositionType)
    };
}

impl IndexRecord {
    pub fn with_position_and_record_header<Pos, Error>(
        position: Pos,
        record_header: RecordHeader,
    ) -> Result<Self, IndexError<Error>>
    where
        Pos: ToPrimitive,
        Error: std::error::Error,
    {
        let index_record = IndexRecord {
            record_header,
            index: None,
            position: position_as_u64!(position, Pos)?,
        };

        Ok(index_record)
    }
}

pub async fn index_records_in_storage<S>(
    source: &S,
) -> Result<(Option<u64>, Vec<IndexRecord>), IndexError<S::Error>>
where
    S: Storage,
{
    let mut position = 0 as u64;
    let mut index_records = Vec::<IndexRecord>::new();

    while let Ok(index_record) =
        IndexRecord::read_from_storage_at(source, &u64_as_position!(position, S::Position)?).await
    {
        index_records.push(index_record);
        position += INDEX_RECORD_LENGTH as u64;
    }

    Ok((index_records.first().and_then(|x| x.index), index_records))
}

pub struct Index<S, Idx> {
    index_records: Vec<IndexRecord>,
    base_index: Idx,
    next_index: Idx,
    storage: S,
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

impl<S, Idx> Index<S, Idx>
where
    S: Storage,
    Idx: FromPrimitive + Copy + Eq,
{
    async fn with_storage_and_base_index_option(
        storage: S,
        base_index: Option<Idx>,
    ) -> Result<Self, IndexError<S::Error>> {
        let (read_base_index, index_records) = index_records_in_storage(&storage).await?;

        let base_index = match (read_base_index, base_index) {
            (None, None) => Err(IndexError::NoBaseIndexFound),
            (None, Some(base_index)) => Ok(base_index),
            (Some(base_index), None) => u64_as_idx!(base_index, Idx),
            (Some(read), Some(provided)) if u64_as_idx!(read, Idx)? != provided => {
                Err(IndexError::BaseIndexMismatch)
            }
            (Some(_), Some(provided)) => Ok(provided),
        }?;

        Ok(Self {
            index_records,
            base_index,
            next_index: base_index,
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
        let next_index = self.next_index;

        let index_record = match index_record.index {
            Some(idx) if idx != idx_as_u64!(next_index, Idx)? => {
                Err(IndexError::<S::Error>::InvalidAppendIdx)
            }
            _ => Ok(IndexRecord {
                index: Some(idx_as_u64!(next_index, Idx)?),
                ..index_record
            }),
        }?;

        index_record.append_to_storage(&mut self.storage).await?;
        self.index_records.push(index_record);

        let write_idx = next_index;
        self.next_index = next_index + Idx::one();

        Ok(write_idx)
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
        let index_record = self.read(idx).await?;

        let position = index_record.position;
        let position = u64_as_position!(position, S::Position)?;

        self.storage
            .truncate(&position)
            .await
            .map_err(IndexError::StorageError)?;

        let vec_index = self
            .normalize_index(idx)
            .ok_or(IndexError::IndexOutOfBounds)?
            .to_usize()
            .ok_or(IndexError::IncompatibleIdxType)?;

        self.index_records.truncate(vec_index);

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
