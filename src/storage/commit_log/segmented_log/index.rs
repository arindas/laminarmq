use crate::storage::common::write_stream;

use super::{super::super::Storage, store::common::RecordHeader};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use futures_util::stream;
use num::FromPrimitive;
use std::{
    io::{Cursor, Read, Write},
    ops::Deref,
};

/// Extension used by backing files for [`Index`](super::Index) instances.
pub const INDEX_FILE_EXTENSION: &str = "index";

/// Number of bytes required for storing the record header.
pub const INDEX_RECORD_LENGTH: usize = 32;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct IndexRecord {
    record_header: RecordHeader,
    index: u64,
    position: u64,
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
    IncompatibleSizeType,
    IncompatibleIdxType,
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
    pub async fn read_from_storage_at<S>(
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

    pub async fn append_to_storage<S>(
        &self,
        dest: &mut S,
    ) -> Result<S::Position, IndexError<S::Error>>
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

pub async fn index_records_in_storage<S>(
    source: &S,
) -> Result<(Option<u64>, Vec<IndexRecord>), IndexError<S::Error>>
where
    S: Storage,
{
    let mut position = 0 as u64;
    let mut index_records = Vec::<IndexRecord>::new();

    while let Ok(index_record) = IndexRecord::read_from_storage_at(
        source,
        &<S::Position as FromPrimitive>::from_u64(position)
            .ok_or(IndexError::IncompatibleSizeType)?,
    )
    .await
    {
        index_records.push(index_record);
        position += INDEX_RECORD_LENGTH as u64;
    }

    Ok((index_records.first().map(|x| x.index), index_records))
}

pub struct Index<S, Idx> {
    _index_records: Vec<IndexRecord>,
    _base_index: Idx,
    _storage: S,
}

#[doc(hidden)]
macro_rules! u64_as_idx {
    ($value:ident, $IdxType:ty) => {
        <$IdxType as FromPrimitive>::from_u64($value).ok_or(IndexError::IncompatibleIdxType)
    };
}

impl<S, Idx> Index<S, Idx>
where
    S: Storage,
    Idx: FromPrimitive + Eq,
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
            _index_records: index_records,
            _base_index: base_index,
            _storage: storage,
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
