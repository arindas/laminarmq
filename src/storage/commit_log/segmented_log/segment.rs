use crate::common::split::SplitAt;

use super::store::Store;
use super::{super::super::*, index::Index};
use super::{MetaWithIdx, Record};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{marker::PhantomData, time::Instant};

/// Configuration pertaining to segment storage and buffer sizes.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Copy)]
pub struct Config {
    /// Segment store's write buffer size. The write buffer is flushed to the disk when it is
    /// filled. Reads of records in a write buffer are only possible when the write buffer is
    /// flushed.
    pub store_buffer_size: Option<usize>,

    /// Maximum segment storage size, after which this segment is demoted from the current
    /// write segment to a read segment.
    pub max_store_bytes: usize,
}

pub struct Segment<M, X, I, S> {
    index: I,
    store: S,

    config: Config,

    created_at: Instant,

    _phantom_data: PhantomData<(M, X)>,
}

impl<M, X, I, S> Segment<M, X, I, S> {
    pub fn new(index: I, store: S, config: Config) -> Self {
        Self {
            index,
            store,
            config,
            created_at: Instant::now(),
            _phantom_data: PhantomData,
        }
    }

    pub fn created_at(&self) -> Instant {
        self.created_at
    }
}

impl<M, X, I, S: Store> SizedStorage for Segment<M, X, I, S> {
    fn size(&self) -> usize {
        self.store.size()
    }
}

impl<M, X, I, S: Store> Segment<M, X, I, S> {
    pub fn is_maxed(&self) -> bool {
        self.store.size() >= self.config.max_store_bytes
    }
}

#[derive(Debug)]
pub enum SegmentError<StoreError, IndexError> {
    StoreError(StoreError),

    IndexError(IndexError),

    SegmentMaxed,

    /// Error when ser/deser-ializing a record.
    SerializationError,
}

impl<StoreError, IndexError> std::fmt::Display for SegmentError<StoreError, IndexError>
where
    StoreError: std::error::Error,
    IndexError: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self)
    }
}

impl<StoreError, IndexError> std::error::Error for SegmentError<StoreError, IndexError>
where
    StoreError: std::error::Error,
    IndexError: std::error::Error,
{
}

#[async_trait::async_trait(?Send)]
impl<M, X, I, S> AsyncIndexedRead for Segment<M, X, I, S>
where
    S: Store,
    I: Index<Position = S::Position>,
    I::Idx: Serialize + DeserializeOwned,
    M: Default + Serialize + DeserializeOwned,
{
    type ReadError = SegmentError<S::Error, I::Error>;

    type Idx = I::Idx;

    type Value = Record<M, Self::Idx, S::Content>;

    fn highest_index(&self) -> &Self::Idx {
        self.index.highest_index()
    }

    fn lowest_index(&self) -> &Self::Idx {
        self.index.lowest_index()
    }

    async fn read(&self, idx: &Self::Idx) -> Result<Self::Value, Self::ReadError> {
        let (position, record_header) = self
            .index
            .read(idx)
            .await
            .map_err(SegmentError::IndexError)?;

        let record_bytes = self
            .store
            .read(&position, &record_header)
            .await
            .map_err(SegmentError::StoreError)?;

        let metadata_size = bincode::serialized_size(&MetaWithIdx {
            index: num::zero::<Self::Idx>(),
            metadata: M::default(),
        })
        .map_err(|_| SegmentError::SerializationError)? as usize;

        let (metadata, value) = record_bytes
            .split_at(metadata_size)
            .ok_or(SegmentError::SerializationError)?;

        let metadata =
            bincode::deserialize(&metadata).map_err(|_| SegmentError::SerializationError)?;

        Ok(Record { metadata, value })
    }
}