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
use futures_core::Stream;
use futures_lite::StreamExt;
use num::{CheckedSub, FromPrimitive, ToPrimitive, Unsigned};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    convert::Infallible,
    hash::Hasher,
    marker::PhantomData,
    ops::Deref,
    time::{Duration, Instant},
};

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct Config<Size> {
    pub max_store_size: Size,
    pub max_index_size: Size,
}

pub struct Segment<S, M, H, Idx, Size, SD> {
    index: Index<S, Idx>,
    store: Store<S, H>,

    config: Config<Size>,

    created_at: Instant,

    _phantom_date: PhantomData<(M, SD)>,
}

impl<S, M, H, Idx, SD> Segment<S, M, H, Idx, S::Size, SD>
where
    S: Storage,
{
    pub fn new(index: Index<S, Idx>, store: Store<S, H>, config: Config<S::Size>) -> Self {
        Self {
            index,
            store,
            config,
            created_at: Instant::now(),
            _phantom_date: PhantomData,
        }
    }

    pub fn is_maxed(&self) -> bool {
        self.store.size() >= self.config.max_store_size
            || self.index.size() >= self.config.max_index_size
    }

    pub fn has_expired(&self, expiry_duration: Duration) -> bool {
        self.created_at.elapsed() >= expiry_duration
    }
}

impl<S, M, H, Idx, SD> Sizable for Segment<S, M, H, Idx, S::Size, SD>
where
    S: Storage,
{
    type Size = S::Size;

    fn size(&self) -> Self::Size {
        self.index.size() + self.store.size()
    }
}

#[derive(Debug)]
pub enum SegmentError<StorageError, SerDeError> {
    StorageError(StorageError),
    StoreError(StoreError<StorageError>),
    IndexError(IndexError<StorageError>),
    IncompatiblePositionType,
    SerializationError(SerDeError),
    RecordMetadataNotFound,
    InvalidAppendIdx,
    InvalidIndexRecordGenerated,
    SegmentMaxed,
}

impl<StorageError, SerDeError> std::fmt::Display for SegmentError<StorageError, SerDeError>
where
    StorageError: std::error::Error,
    SerDeError: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
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
        .map_err(SegmentError::SerializationError)?;

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
    async fn append_serialized_record<XBuf, X, XE>(
        &mut self,
        stream: X,
    ) -> Result<Idx, SegmentOpError<S, SD>>
    where
        XBuf: Deref<Target = [u8]>,
        X: Stream<Item = Result<XBuf, XE>> + Unpin,
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
        .ok_or(SegmentError::InvalidIndexRecordGenerated)?;

        self.index
            .append(index_record)
            .await
            .map_err(SegmentError::IndexError)?;

        Ok(write_index)
    }

    pub async fn append<XBuf, X, XE>(
        &mut self,
        record: Record<M, Idx, X>,
    ) -> Result<Idx, SegmentOpError<S, SD>>
    where
        XBuf: Deref<Target = [u8]>,
        X: Stream<Item = Result<XBuf, XE>> + Unpin,
    {
        if self.is_maxed() {
            return Err(SegmentError::SegmentMaxed);
        }

        let metadata = record
            .metadata
            .anchored_with_index(self.index.highest_index())
            .ok_or(SegmentOpError::<S, SD>::InvalidAppendIdx)?;

        let metadata_bytes = SD::serialize(&metadata).map_err(SegmentError::SerializationError)?;

        enum SBuf<XBuf, YBuf> {
            XBuf(XBuf),
            YBuf(YBuf),
        }

        impl<XBuf, YBuf> Deref for SBuf<XBuf, YBuf>
        where
            XBuf: Deref<Target = [u8]>,
            YBuf: Deref<Target = [u8]>,
        {
            type Target = [u8];

            fn deref(&self) -> &Self::Target {
                match &self {
                    SBuf::XBuf(x_buf) => x_buf.deref(),
                    SBuf::YBuf(y_buf) => y_buf.deref(),
                }
            }
        }

        let stream = futures_lite::stream::once(Ok(SBuf::YBuf(metadata_bytes)));
        let stream = stream.chain(
            record
                .value
                .map(|x_buf| x_buf.map(|x_buf| SBuf::XBuf(x_buf))),
        );

        self.append_serialized_record(stream).await
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

        let metadata_stream =
            futures_lite::stream::once(Ok::<&[u8], Infallible>(metadata_bytes.deref()));
        let value_stream =
            futures_lite::stream::once(Ok::<&[u8], Infallible>(record.value.deref()));
        let stream = metadata_stream.chain(value_stream);

        self.append_serialized_record(stream).await
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

pub struct SegmentStorage<S> {
    pub store: S,
    pub index: S,
}

#[async_trait(?Send)]
pub trait SegmentStorageProvider<S, Idx>
where
    S: Storage,
{
    async fn base_indices_of_stored_segments(&self) -> Result<Vec<Idx>, S::Error>;

    async fn obtain(&mut self, idx: &Idx) -> Result<SegmentStorage<S>, S::Error>;
}

impl<S, M, H, Idx, SD> Segment<S, M, H, Idx, S::Size, SD>
where
    S: Storage,
    H: Default,
    Idx: FromPrimitive + Copy + Eq,
    SD: SerDe,
{
    pub async fn with_segment_storage_provider_config_and_base_index<SSP>(
        segment_storage_provider: &mut SSP,
        config: Config<S::Size>,
        base_index: Idx,
    ) -> Result<Self, SegmentError<S::Error, SD::Error>>
    where
        SSP: SegmentStorageProvider<S, Idx>,
    {
        let segment_storage = segment_storage_provider
            .obtain(&base_index)
            .await
            .map_err(SegmentError::StorageError)?;

        let index = Index::with_storage_and_base_index(segment_storage.index, base_index)
            .await
            .map_err(SegmentError::IndexError)?;

        let store = Store::<S, H>::new(segment_storage.store);

        Ok(Self::new(index, store, config))
    }
}
