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
    fmt::Debug,
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
    M: Serialize + DeserializeOwned,
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

        let metadata_bytes_len_bytes_len =
            SD::serialized_size(&0_usize).map_err(SegmentError::SerializationError)?;

        let (metadata_bytes_len_bytes, metadata_with_value) = record_content
            .split_at(metadata_bytes_len_bytes_len)
            .ok_or(SegmentError::RecordMetadataNotFound)?;

        let metadata_size =
            SD::deserialize(&metadata_bytes_len_bytes).map_err(SegmentError::SerializationError)?;

        let (metadata_bytes, value) = metadata_with_value
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

        let metadata_bytes_len_bytes =
            SD::serialize(&metadata_bytes.len()).map_err(SegmentError::SerializationError)?;

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

        let stream = futures_lite::stream::iter([
            Ok(SBuf::YBuf(metadata_bytes_len_bytes)),
            Ok(SBuf::YBuf(metadata_bytes)),
        ]);
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

        let metadata_bytes_len_bytes =
            SD::serialize(&metadata_bytes.len()).map_err(SegmentError::SerializationError)?;

        let stream = futures_lite::stream::iter([
            Ok::<&[u8], Infallible>(metadata_bytes_len_bytes.deref()),
            Ok::<&[u8], Infallible>(metadata_bytes.deref()),
            Ok::<&[u8], Infallible>(record.value.deref()),
        ]);

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

pub(crate) mod test {

    use super::{
        super::{
            super::super::common::indexed_read_stream, index::INDEX_RECORD_LENGTH,
            store::test::_RECORDS,
        },
        *,
    };
    use num::Zero;
    use std::fmt::Debug;

    async fn _test_indexed_read_contains_expected_records<R, Idx, M, X, I, Y>(
        indexed_read: &R,
        expected_records: I,
        expected_record_count: usize,
    ) where
        R: AsyncIndexedRead<Value = Record<M, Idx, X>>,
        X: Deref<Target = [u8]>,
        I: Iterator<Item = Y>,
        Y: Deref<Target = [u8]>,
    {
        let count = futures_lite::stream::iter(expected_records)
            .zip(indexed_read_stream(indexed_read, ..).await)
            .map(|(y, record)| {
                assert_eq!(y.deref(), record.value.deref());
                Some(())
            })
            .count()
            .await;

        assert_eq!(count, expected_record_count);
    }

    pub(crate) async fn _test_segment_read_append_truncate<S, M, H, Idx, SD, SSP>(
        mut _segment_storage_provider: SSP,
        _: PhantomData<(M, H, SD)>,
    ) where
        S: Storage,
        S::Size: FromPrimitive + Copy,
        S::Content: SplitAt<u8>,
        S::Position: ToPrimitive + Debug,
        M: Default + Serialize + DeserializeOwned + Clone,
        H: Hasher + Default,
        Idx: Unsigned + CheckedSub + FromPrimitive + ToPrimitive + Zero,
        Idx: Ord + Copy + Debug,
        Idx: Serialize + DeserializeOwned,
        SD: SerDe,
        SSP: SegmentStorageProvider<S, Idx>,
    {
        let segment_base_index = Idx::zero();

        let metadata_len_serialized_size = SD::serialized_size(&0_usize).unwrap();

        let metadata_serialized_size = SD::serialized_size(&MetaWithIdx {
            metadata: M::default(),
            index: Some(segment_base_index),
        })
        .unwrap();

        let expected_store_record_length =
            metadata_len_serialized_size + metadata_serialized_size + _RECORDS[0].len();

        let expected_store_size = _RECORDS.len() * expected_store_record_length;
        let expected_index_size = _RECORDS.len() * INDEX_RECORD_LENGTH;

        let config = Config {
            max_store_size: S::Size::from_usize(expected_store_size).unwrap(),
            max_index_size: S::Size::from_usize(expected_index_size).unwrap(),
        };

        let mut segment = Segment::<S, M, H, Idx, S::Size, SD>::with_segment_storage_provider_config_and_base_index(
            &mut _segment_storage_provider,
            config,
            segment_base_index,
        )
        .await
        .unwrap();

        for record in _RECORDS {
            let record_value: &[u8] = record;
            let record = Record {
                metadata: MetaWithIdx {
                    metadata: M::default(),
                    index: Option::<Idx>::None,
                },
                value: record_value,
            };
            segment
                .append_record_with_contiguous_bytes(&record)
                .await
                .unwrap();
        }

        assert!(
            segment.is_maxed(),
            "segment not maxed even after filling to max index and store capacity"
        );

        segment.close().await.unwrap();

        let mut segment = Segment::<S, M, H, Idx, S::Size, SD>::with_segment_storage_provider_config_and_base_index(
            &mut _segment_storage_provider,
            config,
            segment_base_index,
        )
        .await
        .unwrap();

        segment.read(&segment.lowest_index()).await.unwrap();

        _test_indexed_read_contains_expected_records(
            &segment,
            _RECORDS.iter().cloned().map(|x| {
                let x: &[u8] = x;
                x
            }),
            _RECORDS.len(),
        )
        .await;

        let truncate_index =
            (segment.lowest_index() + segment.highest_index()) / Idx::from_u64(2).unwrap();

        let expected_length_after_truncate = truncate_index - segment.lowest_index();

        segment.truncate(&truncate_index).await.unwrap();

        assert!(!segment.is_maxed());

        assert_eq!(segment.len(), expected_length_after_truncate);

        _test_indexed_read_contains_expected_records(
            &segment,
            _RECORDS.iter().cloned().map(|x| {
                let x: &[u8] = x;
                x
            }),
            segment.len().to_usize().unwrap(),
        )
        .await;

        assert!(
            segment.has_expired(Duration::from_micros(0)),
            "segment not older than 0 micro second"
        );

        const TEST_RECORD_VALUE: &[u8] = b"Hello World!";

        segment
            .append(Record {
                metadata: MetaWithIdx {
                    metadata: M::default(),
                    index: Some(segment.highest_index()),
                },
                value: futures_lite::stream::once(Ok::<&[u8], Infallible>(TEST_RECORD_VALUE)),
            })
            .await
            .unwrap();

        if let Err(SegmentError::InvalidAppendIdx) = segment
            .append_record_with_contiguous_bytes(&Record {
                metadata: MetaWithIdx {
                    metadata: M::default(),
                    index: Some(segment.lowest_index()),
                },
                value: &[0_u8] as &[u8],
            })
            .await
        {
        } else {
            unreachable!("Wrong result type returned on append with invalid append index")
        }

        segment.remove().await.unwrap();

        let segment = Segment::<S, M, H, Idx, S::Size, SD>::with_segment_storage_provider_config_and_base_index(
            &mut _segment_storage_provider,
            config,
            segment_base_index,
        )
        .await
        .unwrap();

        assert!(segment.is_empty(), "segment contains data after removal");

        segment.remove().await.unwrap();
    }
}
