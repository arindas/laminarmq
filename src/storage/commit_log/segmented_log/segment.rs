use super::{
    super::super::{
        super::common::{serde::SerDe, split::SplitAt},
        AsyncConsume, AsyncIndexedRead, AsyncTruncate, Sizable, Storage,
    },
    index::{Index, IndexError, IndexRecord},
    store::{Store, StoreError},
    MetaWithIdx, Record,
};
use async_trait::async_trait;
use bytes::Buf;
use futures_core::Stream;
use futures_lite::StreamExt;
use num::{CheckedSub, FromPrimitive, ToPrimitive, Unsigned};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{hash::Hasher, marker::PhantomData, ops::Deref};

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct Config<Size> {
    pub max_store_size: Size,
    pub max_index_size: Size,
}

pub struct Segment<S, M, H, Idx, Size, SD> {
    index: Index<S, Idx>,
    store: Store<S, H>,

    config: Config<Size>,

    _phantom_date: PhantomData<(M, SD)>,
}

impl<S, M, H, Idx, SD> Segment<S, M, H, Idx, S::Size, SD>
where
    S: Storage,
{
    pub fn is_maxed(&self) -> bool {
        self.store.size() >= self.config.max_store_size
            || self.index.size() >= self.config.max_index_size
    }
}

#[derive(Debug)]
pub enum SegmentError<StorageError, SerDeError> {
    StoreError(StoreError<StorageError>),
    IndexError(IndexError<StorageError>),
    IncompatiblePositionType,
    SerializationError(SerDeError),
    RecordMetadataNotFound,
    InvalidAppendIdx,
    SegmentMaxed,
}

impl<StorageError, SerDeError> std::fmt::Display for SegmentError<StorageError, SerDeError>
where
    StorageError: std::error::Error,
    SerDeError: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<StorageError, SerDeError> std::error::Error for SegmentError<StorageError, SerDeError>
where
    StorageError: std::error::Error,
    SerDeError: std::error::Error,
{
}

pub type SegmentOpError<S, SD> = SegmentError<<S as Storage>::Error, <SD as SerDe>::Error>;

#[async_trait(?Send)]
impl<S, M, H, Idx, SD> AsyncIndexedRead for Segment<S, M, H, Idx, S::Size, SD>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    SD: SerDe,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    Idx: Serialize + DeserializeOwned,
    M: Default + Serialize + DeserializeOwned,
{
    type ReadError = SegmentOpError<S, SD>;

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

        let metadata_size = SD::serialized_size(&MetaWithIdx::<M, Idx> {
            index: None,
            metadata: M::default(),
        })
        .map_err(SegmentError::SerializationError)? as usize;

        let (metadata_bytes, value) = record_content
            .split_at(metadata_size)
            .ok_or(SegmentError::RecordMetadataNotFound)?;

        let metadata =
            SD::deserialize(&metadata_bytes).map_err(SegmentError::SerializationError)?;

        Ok(Record { metadata, value })
    }
}

impl<S, M, H, Idx, SD> Segment<S, M, H, Idx, S::Size, SD>
where
    S: Storage,
    S::Position: ToPrimitive,
    M: Serialize,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    Idx: Serialize,
    SD: SerDe,
{
    async fn append_serialized_record<XBuf, X>(
        &mut self,
        stream: &mut X,
    ) -> Result<Idx, SegmentOpError<S, SD>>
    where
        XBuf: Buf,
        X: Stream<Item = XBuf> + Unpin,
    {
        let write_index = self.index.highest_index();

        let (position, record_header) = self
            .store
            .append(stream)
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

    pub async fn append<XBuf, X>(
        &mut self,
        record: Record<M, Idx, X>,
    ) -> Result<Idx, SegmentOpError<S, SD>>
    where
        XBuf: Buf + From<SD::SerBytes>,
        X: Stream<Item = XBuf> + Unpin,
    {
        if self.is_maxed() {
            return Err(SegmentError::SegmentMaxed);
        }

        let metadata = record
            .metadata
            .anchored_with_index(self.index.highest_index())
            .ok_or(SegmentOpError::<S, SD>::InvalidAppendIdx)?;

        let metadata_bytes = SD::serialize(&metadata).map_err(SegmentError::SerializationError)?;

        let stream = futures_lite::stream::once(metadata_bytes.into());
        let mut stream = stream.chain(record.value);

        self.append_serialized_record(&mut stream).await
    }
}

impl<S, M, H, Idx, SD> Segment<S, M, H, Idx, S::Size, SD>
where
    S: Storage,
    S::Position: ToPrimitive,
    M: Serialize + Clone,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    Idx: Serialize,
    SD: SerDe,
{
    pub async fn append_record_with_contiguous_bytes<X>(
        &mut self,
        record: &Record<M, Idx, X>,
    ) -> Result<Idx, SegmentOpError<S, SD>>
    where
        X: Deref<Target = [u8]>,
    {
        if self.is_maxed() {
            return Err(SegmentError::SegmentMaxed);
        }

        let metadata = record
            .metadata
            .clone()
            .anchored_with_index(self.index.highest_index())
            .ok_or(SegmentOpError::<S, SD>::InvalidAppendIdx)?;

        let metadata_bytes = SD::serialize(&metadata).map_err(SegmentError::SerializationError)?;

        let metadata_stream = futures_lite::stream::once(metadata_bytes.deref());
        let value_stream = futures_lite::stream::once(record.value.deref());
        let mut stream = metadata_stream.chain(value_stream);

        self.append_serialized_record(&mut stream).await
    }
}

#[async_trait(?Send)]
impl<S, M, H, Idx, SD> AsyncTruncate for Segment<S, M, H, Idx, S::Size, SD>
where
    S: Storage,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    SD: SerDe,
{
    type Mark = Idx;

    type TruncError = SegmentError<S::Error, SD::Error>;

    async fn truncate(&mut self, mark: &Self::Mark) -> Result<(), Self::TruncError> {
        let index_record = self
            .index
            .read(mark)
            .await
            .map_err(SegmentError::IndexError)?;

        let position = S::Position::from_u64(index_record.position)
            .ok_or(SegmentError::IncompatiblePositionType)?;

        self.store
            .truncate(&position)
            .await
            .map_err(SegmentError::StoreError)?;

        self.index
            .truncate(mark)
            .await
            .map_err(SegmentError::IndexError)?;

        Ok(())
    }
}

#[async_trait(?Send)]
impl<S, M, H, Idx, SD> AsyncConsume for Segment<S, M, H, Idx, S::Size, SD>
where
    S: Storage,
    SD: SerDe,
{
    type ConsumeError = SegmentError<S::Error, SD::Error>;

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

    async fn close(self) -> Result<(), Self::ConsumeError> {
        self.store.close().await.map_err(SegmentError::StoreError)?;
        self.index.close().await.map_err(SegmentError::IndexError)?;
        Ok(())
    }
}
