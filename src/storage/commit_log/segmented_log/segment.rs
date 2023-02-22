use std::{hash::Hasher, marker::PhantomData};

use async_trait::async_trait;
use bytes::Buf;
use futures_core::Stream;
use num::{CheckedSub, FromPrimitive, ToPrimitive, Unsigned};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{common::split::SplitAt, storage::Sizable};

use super::{
    super::super::{AsyncIndexedRead, Storage},
    index::{Index, IndexError, IndexRecord},
    store::{Store, StoreError},
    MetaWithIdx, Record,
};

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct Config<Size> {
    pub max_store_size: Size,
    pub max_index_size: Size,
}

pub struct Segment<S, M, H, Idx, Size> {
    index: Index<S, Idx>,
    store: Store<S, H>,

    config: Config<Size>,

    _phantom_date: PhantomData<M>,
}

impl<S, M, H, Idx> Segment<S, M, H, Idx, S::Size>
where
    S: Storage,
{
    pub fn is_maxed(&self) -> bool {
        self.store.size() >= self.config.max_store_size
            || self.index.size() >= self.config.max_index_size
    }
}

#[derive(Debug)]
pub enum SegmentError<StorageError> {
    StoreError(StoreError<StorageError>),
    IndexError(IndexError<StorageError>),
    IncompatiblePositionType,
    SerializationError,
    InvalidAppendIdx,
    SegmentMaxed,
    DummyError,
}

impl<StorageError: std::error::Error> std::fmt::Display for SegmentError<StorageError> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<StorageError: std::error::Error> std::error::Error for SegmentError<StorageError> {}

#[async_trait(?Send)]
impl<S, M, H, Idx> AsyncIndexedRead for Segment<S, M, H, Idx, S::Size>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    Idx: Serialize + DeserializeOwned,
    M: Default + Serialize + DeserializeOwned,
{
    type ReadError = SegmentError<S::Error>;

    type Idx = Idx;

    type Value = Record<M, Idx, S::Content>;

    fn highest_index(&self) -> Self::Idx {
        self.index.highest_index()
    }

    fn lowest_index(&self) -> Self::Idx {
        self.index.lowest_index()
    }

    async fn read(&self, idx: &Self::Idx) -> Result<Self::Value, Self::ReadError> {
        let index_record = self
            .index
            .read(idx)
            .await
            .map_err(SegmentError::IndexError)?;

        let position = S::Position::from_u64(index_record.position)
            .ok_or(SegmentError::IncompatiblePositionType)?;

        let record_content = self
            .store
            .read(&position, &index_record.record_header)
            .await
            .map_err(SegmentError::StoreError)?;

        let metadata_size = bincode::serialized_size(&MetaWithIdx::<M, Idx> {
            index: None,
            metadata: M::default(),
        })
        .map_err(|_| SegmentError::SerializationError)? as usize;

        let (metadata_bytes, value) = record_content
            .split_at(metadata_size)
            .ok_or(SegmentError::SerializationError)?;

        let metadata =
            bincode::deserialize(&metadata_bytes).map_err(|_| SegmentError::SerializationError)?;

        Ok(Record { metadata, value })
    }
}

impl<S, M, H, Idx> Segment<S, M, H, Idx, S::Size>
where
    S: Storage,
    S::Position: ToPrimitive,
    H: Hasher + Default + Serialize,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    Idx: Serialize,
{
    pub async fn append<XBuf, X>(
        &mut self,
        record: Record<M, Idx, X>,
    ) -> Result<Idx, SegmentError<S::Error>>
    where
        XBuf: Buf,
        X: Stream<Item = XBuf> + Unpin,
    {
        if self.is_maxed() {
            return Err(SegmentError::SegmentMaxed);
        }

        let write_index = self.index.highest_index();

        let mut record = match record.metadata.index {
            Some(idx) if write_index != idx => Err(SegmentError::<S::Error>::InvalidAppendIdx),
            _ => Ok(Record {
                metadata: MetaWithIdx {
                    index: Some(write_index),
                    ..record.metadata
                },
                ..record
            }),
        }?;

        let (position, record_header) = self
            .store
            .append(&mut record.value)
            .await
            .map_err(SegmentError::StoreError)?;

        let index_record = IndexRecord::with_position_index_and_record_header(
            position,
            write_index,
            record_header,
        )
        .map_err(SegmentError::IndexError)?;

        self.index
            .append(index_record)
            .await
            .map_err(SegmentError::IndexError)?;

        Ok(write_index)
    }
}
