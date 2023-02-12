use async_trait::async_trait;
use bytes::Buf;
use num::cast::FromPrimitive;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::{
    super::super::{super::common::split::SplitAt, *},
    index::Index,
    store::Store,
    MetaWithIdx, Record,
};
use std::{
    marker::PhantomData,
    time::{Duration, Instant},
};

/// Configuration pertaining to segment storage and buffer sizes.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Copy)]
pub struct Config<Size> {
    /// Segment store's write buffer size. The write buffer is flushed to the disk when it is
    /// filled. Reads of records in a write buffer are only possible when the write buffer is
    /// flushed.
    pub store_buffer_size: Option<Size>,

    /// Maximum segment storage size, after which this segment is demoted from the current
    /// write segment to a read segment.
    pub max_store_bytes: Size,
}

pub struct Segment<M, X, I, S, Size> {
    index: I,
    store: S,

    config: Config<Size>,

    created_at: Instant,

    _phantom_data: PhantomData<(M, X)>,
}

impl<M, X, I, S, Size> Segment<M, X, I, S, Size> {
    pub fn has_expired(&self, expiry_duration: Duration) -> bool {
        self.created_at.elapsed() >= expiry_duration
    }

    pub fn new(index: I, store: S, config: Config<Size>) -> Self {
        Self {
            index,
            store,
            config,
            created_at: Instant::now(),
            _phantom_data: PhantomData,
        }
    }
}

impl<M, X, I, S: Store> Sizable for Segment<M, X, I, S, S::Size> {
    type Size = S::Size;

    fn size(&self) -> Self::Size {
        self.store.size()
    }
}

impl<M, X, I, S: Store> Segment<M, X, I, S, S::Size> {
    pub fn is_maxed(&self) -> bool {
        self.store.size() >= self.config.max_store_bytes
    }
}

#[derive(Debug)]
pub enum SegmentError<StoreError, IndexError> {
    StoreError(StoreError),

    IndexError(IndexError),

    InvalidInputIndex,

    SegmentMaxed,

    IncompatibleSizeType,

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

#[async_trait(?Send)]
impl<M, X, I, S> AsyncIndexedRead for Segment<M, X, I, S, S::Size>
where
    S: Store,
    I: Index<Position = S::Position>,
    I::Idx: Serialize + DeserializeOwned,
    M: Default + Serialize + DeserializeOwned,
{
    type ReadError = SegmentError<S::Error, I::Error>;

    type Idx = I::Idx;

    type Value = Record<M, Self::Idx, S::Content>;

    fn highest_index(&self) -> Self::Idx {
        self.index.highest_index()
    }

    fn lowest_index(&self) -> Self::Idx {
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

pub(crate) type SegError<S, I> = SegmentError<<S as Store>::Error, <I as Index>::Error>;

impl<M, X, I, S, ReqBuf> Segment<M, X, I, S, S::Size>
where
    S: Store,
    I: Index<Position = S::Position>,
    ReqBuf: Buf,
    X: Stream<Item = ReqBuf> + Unpin,
{
    pub async fn append(
        &mut self,
        record: &mut Record<M, I::Idx, X>,
    ) -> Result<S::Size, SegError<S, I>> {
        if record.metadata.index != self.index.highest_index() {
            return Err(SegmentError::InvalidInputIndex);
        }

        if self.is_maxed() {
            return Err(SegmentError::SegmentMaxed);
        }

        let index_record = self
            .store
            .append(&mut record.value)
            .await
            .map_err(SegmentError::StoreError)?;

        self.index
            .append(&index_record)
            .await
            .map_err(SegmentError::IndexError)?;

        let (_, record_header) = index_record;

        <S::Size as FromPrimitive>::from_u64(record_header.length)
            .ok_or(SegmentError::IncompatibleSizeType)
    }
}

#[async_trait(?Send)]
impl<M, X, I, S> AsyncTruncate for Segment<M, X, I, S, S::Size>
where
    S: Store,
    I: Index<Position = S::Position>,
{
    type TruncError = SegError<S, I>;

    type Mark = I::Idx;

    async fn truncate(&mut self, idx: &Self::Mark) -> Result<(), Self::TruncError> {
        let (position, _) = self
            .index
            .read(idx)
            .await
            .map_err(SegmentError::IndexError)?;

        self.store
            .truncate(&position)
            .await
            .map_err(SegmentError::StoreError)?;

        self.index
            .truncate(idx)
            .await
            .map_err(SegmentError::IndexError)?;

        Ok(())
    }
}

#[async_trait(?Send)]
impl<M, X, I, S> AsyncConsume for Segment<M, X, I, S, S::Size>
where
    S: Store,
    I: Index,
{
    type ConsumeError = SegError<S, I>;

    async fn close(self) -> Result<(), Self::ConsumeError> {
        self.store.close().await.map_err(SegmentError::StoreError)?;
        self.index.close().await.map_err(SegmentError::IndexError)?;

        Ok(())
    }

    async fn remove(self) -> Result<(), Self::ConsumeError> {
        self.store
            .remove()
            .await
            .map_err(SegmentError::StoreError)?;
        self.index
            .remove()
            .await
            .map_err(SegmentError::IndexError)?;

        Ok(())
    }
}

#[async_trait(?Send)]
pub trait SegmentCreator {
    type Error: std::error::Error;

    type Idx: Unsigned;

    type SegmentConfig;
    type Segment;

    async fn create(
        &self,
        segment_base_index: &Self::Idx,
        segment_config: &Self::SegmentConfig,
    ) -> Result<Self::Segment, Self::Error>;
}
