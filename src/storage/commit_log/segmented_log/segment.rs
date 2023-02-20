use std::{hash::Hasher, marker::PhantomData};

use async_trait::async_trait;
use num::{CheckedSub, FromPrimitive, ToPrimitive, Unsigned};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::common::split::SplitAt;

use super::{
    super::super::{AsyncIndexedRead, Storage},
    index::{Index, IndexError},
    store::{Store, StoreError},
    MetaWithIdx, Record,
};

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct Config<Size> {
    pub max_segment_size: Size,
}

pub struct Segment<S, M, H, Idx, Size> {
    _index: Index<S, Idx>,
    _store: Store<S, H>,

    _config: Config<Size>,

    _phantom_date: PhantomData<M>,
}

#[derive(Debug)]
pub enum SegmentError<StorageError> {
    StoreError(StoreError<StorageError>),
    IndexError(IndexError<StorageError>),
    IncompatiblePositionType,
    SerializationError,
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
        self._index.highest_index()
    }

    fn lowest_index(&self) -> Self::Idx {
        self._index.lowest_index()
    }

    async fn read(&self, idx: &Self::Idx) -> Result<Self::Value, Self::ReadError> {
        let index_record = self
            ._index
            .read(idx)
            .await
            .map_err(SegmentError::IndexError)?;

        let position = S::Position::from_u64(index_record.position)
            .ok_or(SegmentError::IncompatiblePositionType)?;

        let record_content = self
            ._store
            .read(&position, &index_record.record_header)
            .await
            .map_err(SegmentError::StoreError)?;

        let metadata_size = bincode::serialized_size(&MetaWithIdx {
            index: num::zero::<Self::Idx>(),
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
