//! Presents the `segment` units that a `segmented-log` is made out of.
//!
//! Each `segment` contains an `index` for addressing reocrds and a `store` for backing storage.
//! The `index` stores a mapping from record indices to positions on the `store` file. The `store`
//! file stores the actual records on the underlying storage.

use super::{
    super::super::{
        super::common::{serde_compat::SerializationProvider, split::SplitAt},
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

/// [`Store`] and [`Index`] size configuration for a [`Segment`].
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct Config<Size> {
    pub max_store_size: Size,
    pub max_store_overflow: Size,
    pub max_index_size: Size,
}

/// A segment unit in a [`SegmentedLog`](super::SegmentedLog).
///
/// <p align="center">
/// <img src="https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-indexed-segmented-log-segment.drawio.png" alt="segmented_log_segment" />
/// </p>
/// <p align="center">
/// <b>Fig:</b> <code>Segment</code> diagram showing <code>Index</code>, mapping logical indices
/// to<code>Store</code> positions and a <code>Store</code> persisting record bytes at the
/// demarcated positions.
/// </p>

pub struct Segment<S, M, H, Idx, Size, SERP> {
    index: Index<S, Idx>,
    store: Store<S, H>,

    config: Config<Size>,

    created_at: Instant,

    _phantom_date: PhantomData<(M, SERP)>,
}

impl<S, M, H, Idx, SERP> Segment<S, M, H, Idx, S::Size, SERP>
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

impl<S, M, H, Idx, SERP> Sizable for Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
{
    type Size = S::Size;

    fn size(&self) -> Self::Size {
        self.index.size() + self.store.size()
    }
}

/// Error type associated with operations on [`Segment`].
#[derive(Debug)]
pub enum SegmentError<StorageError, SerDeError> {
    /// Used to denote errors from the backing [`Storage`] implementation.
    StorageError(StorageError),

    /// Used to denote errors from the underlying [`Store`].
    StoreError(StoreError<StorageError>),

    /// Used to denote errors from the underlying [`Index`].
    IndexError(IndexError<StorageError>),

    /// Used when the type used for representing positions is incompatible with [`u64`].
    IncompatiblePositionType,

    /// Used to denote errors when serializing or deserializing data. (for instance, [`Record`]
    /// metadata)
    SerializationError(SerDeError),

    /// Used when the metadata associated with a [`Record`] is not found.
    RecordMetadataNotFound,

    /// Used when the provided append index is not the hghest index of the [`Segment`].
    InvalidAppendIdx,

    /// Used when the [`Segment`] is unable to regenerate an [`IndexRecord`] from the position and
    /// [`RecordHeader`](super::store::common::RecordHeader).
    InvalidIndexRecordGenerated,

    /// Used when usize cannot be coerced to u32 and vice versa.
    UsizeU32Inconvertible,

    /// Used when a given [`Segment`] maxes out its capacity when we append to it.
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

#[doc(hidden)]
pub type SegmentOpError<S, SERP> =
    SegmentError<<S as Storage>::Error, <SERP as SerializationProvider>::Error>;

#[async_trait(?Send)]
impl<S, M, H, Idx, SERP> AsyncIndexedRead for Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    SERP: SerializationProvider,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    Idx: Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned,
{
    type ReadError = SegmentOpError<S, SERP>;

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

        let position = S::Position::from_u64(index_record.position as u64)
            .ok_or(SegmentError::IncompatiblePositionType)?;

        let record_content = self
            .store
            .read(&position, &index_record.into())
            .await
            .map_err(SegmentError::StoreError)?;

        let metadata_bytes_len_bytes_len =
            SERP::serialized_size(&0_u32).map_err(SegmentError::SerializationError)?;

        let (metadata_bytes_len_bytes, metadata_with_value) = record_content
            .split_at(metadata_bytes_len_bytes_len)
            .ok_or(SegmentError::RecordMetadataNotFound)?;

        let metadata_bytes_len: u32 = SERP::deserialize(&metadata_bytes_len_bytes)
            .map_err(SegmentError::SerializationError)?;

        let metadata_bytes_len: usize = metadata_bytes_len
            .try_into()
            .map_err(|_| SegmentError::UsizeU32Inconvertible)?;

        let (metadata_bytes, value) = metadata_with_value
            .split_at(metadata_bytes_len)
            .ok_or(SegmentError::RecordMetadataNotFound)?;

        let metadata =
            SERP::deserialize(&metadata_bytes).map_err(SegmentError::SerializationError)?;

        Ok(Record { metadata, value })
    }
}

impl<S, M, H, Idx, SERP> Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
    M: Serialize,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    Idx: Serialize,
    SERP: SerializationProvider,
{
    async fn append_serialized_record<XBuf, X, XE>(
        &mut self,
        stream: X,
    ) -> Result<Idx, SegmentOpError<S, SERP>>
    where
        XBuf: Deref<Target = [u8]>,
        X: Stream<Item = Result<XBuf, XE>> + Unpin,
    {
        let write_index = self.index.highest_index();

        let remaining_store_capacity = self.config.max_store_size - self.store.size();

        let append_threshold = remaining_store_capacity + self.config.max_store_overflow;

        let (position, record_header) = self
            .store
            .append(stream, Some(append_threshold))
            .await
            .map_err(SegmentError::StoreError)?;

        let index_record = IndexRecord::with_position_and_record_header(position, record_header)
            .ok_or(SegmentError::InvalidIndexRecordGenerated)?;

        self.index
            .append(index_record)
            .await
            .map_err(SegmentError::IndexError)?;

        Ok(write_index)
    }

    /// Appends a new [`Record`] to this [`Segment`].
    ///
    /// Serializes the record metadata and bytes and writes them to the backing [`Store`]. Also
    /// makes an [`IndexRecord`] entry in the underlying [`Index`] to keep track of the [`Record`].
    ///
    /// Returns the index at which the [`Record`] was written.
    ///
    /// Errors out with a [`SegmentError`] when necessary. Refer to [`SegmentError`] for more info
    /// about error situations and types.
    pub async fn append<XBuf, X, XE>(
        &mut self,
        record: Record<M, Idx, X>,
    ) -> Result<Idx, SegmentOpError<S, SERP>>
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
            .ok_or(SegmentOpError::<S, SERP>::InvalidAppendIdx)?;

        let metadata_bytes =
            SERP::serialize(&metadata).map_err(SegmentError::SerializationError)?;

        let metadata_bytes_len: u32 = metadata_bytes
            .len()
            .try_into()
            .map_err(|_| SegmentError::UsizeU32Inconvertible)?;

        let metadata_bytes_len_bytes =
            SERP::serialize(&metadata_bytes_len).map_err(SegmentError::SerializationError)?;

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

impl<S, M, H, Idx, SERP> Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
    M: Serialize + Clone,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    Idx: Serialize,
    SERP: SerializationProvider,
{
    /// Like [`Segment::append`] but the [`Record`] contains a contiguous slice of bytes, as
    /// opposed to a stream.
    pub async fn append_record_with_contiguous_bytes<X>(
        &mut self,
        record: &Record<M, Idx, X>,
    ) -> Result<Idx, SegmentOpError<S, SERP>>
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
            .ok_or(SegmentOpError::<S, SERP>::InvalidAppendIdx)?;

        let metadata_bytes =
            SERP::serialize(&metadata).map_err(SegmentError::SerializationError)?;

        let metadata_bytes_len: u32 = metadata_bytes
            .len()
            .try_into()
            .map_err(|_| SegmentError::UsizeU32Inconvertible)?;

        let metadata_bytes_len_bytes =
            SERP::serialize(&metadata_bytes_len).map_err(SegmentError::SerializationError)?;

        let stream = futures_lite::stream::iter([
            Ok::<&[u8], Infallible>(metadata_bytes_len_bytes.deref()),
            Ok::<&[u8], Infallible>(metadata_bytes.deref()),
            Ok::<&[u8], Infallible>(record.value.deref()),
        ]);

        self.append_serialized_record(stream).await
    }
}

#[async_trait(?Send)]
impl<S, M, H, Idx, SERP> AsyncTruncate for Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    SERP: SerializationProvider,
{
    type Mark = Idx;

    type TruncError = SegmentError<S::Error, SERP::Error>;

    async fn truncate(&mut self, mark: &Self::Mark) -> Result<(), Self::TruncError> {
        let index_record = self
            .index
            .read(mark)
            .await
            .map_err(SegmentError::IndexError)?;

        let position = S::Position::from_u64(index_record.position as u64)
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
impl<S, M, H, Idx, SERP> AsyncConsume for Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
    SERP: SerializationProvider,
{
    type ConsumeError = SegmentError<S::Error, SERP::Error>;

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

impl<S, M, H, Idx, SERP> Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
    SERP: SerializationProvider,
    Idx: Unsigned + FromPrimitive + Copy + Eq,
{
    /// Caches the [`Index`] contents i.e [`IndexRecord`] instances in memory for fast lookup.
    pub async fn cache_index(&mut self) -> Result<(), SegmentError<S::Error, SERP::Error>> {
        self.index.cache().await.map_err(SegmentError::IndexError)
    }

    /// Takes the cached [`IndexRecord`] instances from this [`Segment`], leaving [`None`] in their
    /// place.
    pub fn take_cached_index_records(&mut self) -> Option<Vec<IndexRecord>> {
        self.index.take_cached_index_records()
    }

    /// Returns a reference to the cached [`IndexRecord`] instances.
    pub fn cached_index_records(&self) -> Option<&Vec<IndexRecord>> {
        self.index.cached_index_records()
    }
}

/// Backing storage for an [`Index`] and [`Store`] within a [`Segment`].
pub struct SegmentStorage<S> {
    pub store: S,
    pub index: S,
}

/// Provides backing storage for [`Segment`] instances.
///
/// Used to abstract the mechanism of acquiring storage handles from the underlying persistent
/// media.
#[async_trait(?Send)]
pub trait SegmentStorageProvider<S, Idx>
where
    S: Storage,
{
    /// Returns the base indices of all the [`Segment`] instances persisted in this storage media.
    async fn obtain_base_indices_of_stored_segments(&mut self) -> Result<Vec<Idx>, S::Error>;

    /// Obtains a [`SegmentStorage`] instance for a [`Segment`] with the given `idx` as their base
    /// index.
    ///
    /// Implementations are required to allocate/arrange new storage handles if a [`Segment`] with
    /// the given base index is not already persisted on the underlying storage media.
    async fn obtain(&mut self, idx: &Idx) -> Result<SegmentStorage<S>, S::Error>;
}

impl<S, M, H, Idx, SERP> Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
    H: Default,
    Idx: Unsigned + FromPrimitive + Copy + Eq,
    SERP: SerializationProvider,
{
    pub async fn with_segment_storage_provider_config_base_index_and_cache_index_records_flag<SSP>(
        segment_storage_provider: &mut SSP,
        config: Config<S::Size>,
        base_index: Idx,
        cache_index_records_flag: bool,
    ) -> Result<Self, SegmentError<S::Error, SERP::Error>>
    where
        SSP: SegmentStorageProvider<S, Idx>,
    {
        let segment_storage = segment_storage_provider
            .obtain(&base_index)
            .await
            .map_err(SegmentError::StorageError)?;

        let index = if cache_index_records_flag {
            Index::with_storage_and_base_index(segment_storage.index, base_index).await
        } else {
            Index::with_storage_index_records_option_and_validated_base_index(
                segment_storage.index,
                None,
                base_index,
            )
        }
        .map_err(SegmentError::IndexError)?;

        let store = Store::<S, H>::new(segment_storage.store);

        Ok(Self::new(index, store, config))
    }
}

impl<S, M, H, Idx, SERP> Segment<S, M, H, Idx, S::Size, SERP>
where
    S: Storage,
    H: Default,
    Idx: Unsigned + FromPrimitive + Copy + Eq,
    SERP: SerializationProvider,
{
    pub async fn flush<SSP>(
        mut self,
        segment_storage_provider: &mut SSP,
    ) -> Result<Self, SegmentError<S::Error, SERP::Error>>
    where
        SSP: SegmentStorageProvider<S, Idx>,
    {
        let base_index = *self.index.base_index();
        let cached_index_records = self.index.take_cached_index_records();

        self.index.close().await.map_err(SegmentError::IndexError)?;
        self.store.close().await.map_err(SegmentError::StoreError)?;

        let segment_storage = segment_storage_provider
            .obtain(&base_index)
            .await
            .map_err(SegmentError::StorageError)?;

        self.index = Index::with_storage_index_records_option_and_validated_base_index(
            segment_storage.index,
            cached_index_records,
            base_index,
        )
        .map_err(SegmentError::IndexError)?;

        self.store = Store::<S, H>::new(segment_storage.store);

        Ok(self)
    }
}

pub(crate) mod test {

    use super::{
        super::{
            super::super::commit_log::test::_test_indexed_read_contains_expected_records,
            index::{INDEX_BASE_MARKER_LENGTH, INDEX_RECORD_LENGTH},
            store::test::_RECORDS,
        },
        *,
    };
    use num::Zero;
    use std::fmt::Debug;

    pub(crate) fn _segment_config<M, Idx, Size, SERP>(
        record_len: usize,
        num_records: usize,
    ) -> Option<Config<Size>>
    where
        M: Default + Serialize,
        Idx: Serialize + Zero,
        Size: FromPrimitive,
        SERP: SerializationProvider,
    {
        let metadata_len_serialized_size = SERP::serialized_size(&0_u32).ok()?;

        let metadata_serialized_size = SERP::serialized_size(&MetaWithIdx {
            metadata: M::default(),
            index: Some(Idx::zero()),
        })
        .ok()?;

        let expected_store_record_length =
            metadata_len_serialized_size + metadata_serialized_size + record_len;

        let expected_store_size = num_records * expected_store_record_length;
        let expected_index_size = INDEX_BASE_MARKER_LENGTH + num_records * INDEX_RECORD_LENGTH;

        Some(Config {
            max_store_size: Size::from_usize(expected_store_size)?,
            max_store_overflow: Size::from_usize(0_usize)?,
            max_index_size: Size::from_usize(expected_index_size)?,
        })
    }

    pub(crate) async fn _test_segment_read_append_truncate_consistency<S, M, H, Idx, SERP, SSP>(
        mut _segment_storage_provider: SSP,
        _: PhantomData<(M, H, SERP)>,
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
        SERP: SerializationProvider,
        SSP: SegmentStorageProvider<S, Idx>,
    {
        let segment_base_index = Idx::zero();

        let config =
            _segment_config::<M, Idx, S::Size, SERP>(_RECORDS[0].len(), _RECORDS.len()).unwrap();

        let mut segment = Segment::<S, M, H, Idx, S::Size, SERP>::with_segment_storage_provider_config_base_index_and_cache_index_records_flag(
            &mut _segment_storage_provider,
            config,
            segment_base_index,
            false,
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

        let mut segment = Segment::<S, M, H, Idx, S::Size, SERP>::with_segment_storage_provider_config_base_index_and_cache_index_records_flag(
            &mut _segment_storage_provider,
            config,
            segment_base_index,
            true,
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

        let segment = Segment::<S, M, H, Idx, S::Size, SERP>::with_segment_storage_provider_config_base_index_and_cache_index_records_flag(
            &mut _segment_storage_provider,
            config,
            segment_base_index,
            true,
        )
        .await
        .unwrap();

        assert!(segment.is_empty(), "segment contains data after removal");

        segment.remove().await.unwrap();
    }
}
