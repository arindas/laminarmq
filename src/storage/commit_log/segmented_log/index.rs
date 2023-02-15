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

pub enum IndexError<StorageError> {
    StorageError(StorageError),
    IoError(std::io::Error),
    IncompatibleSizeType,
}

impl IndexRecord {
    pub async fn read_from_storage<S>(
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

    pub async fn write_to_storage<S>(
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
