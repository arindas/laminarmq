pub mod index;
pub mod segment;
pub mod store;

use self::segment::{Segment, SegmentStorageProvider};
use super::{
    super::super::{
        common::{
            cache::{AllocLRUCache, Cache, Eviction, Lookup},
            serde_compat::SerializationProvider,
            split::SplitAt,
        },
        storage::common::{index_bounds_for_range, indexed_read_stream},
        storage::{AsyncConsume, AsyncIndexedExclusiveRead, AsyncIndexedRead, AsyncTruncate},
        storage::{Sizable, Storage},
    },
    CommitLog,
};

use async_trait::async_trait;
use futures_core::Stream;
use futures_lite::{stream, StreamExt};
use num::{CheckedSub, FromPrimitive, ToPrimitive, Unsigned};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    cmp::Ordering,
    error::Error,
    fmt::Debug,
    hash::Hasher,
    ops::{Deref, RangeBounds},
    time::Duration,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MetaWithIdx<M, Idx> {
    pub metadata: M,
    pub index: Option<Idx>,
}

impl<M, Idx> MetaWithIdx<M, Idx>
where
    Idx: Eq,
{
    pub fn anchored_with_index(self, anchor_idx: Idx) -> Option<Self> {
        let index = match self.index {
            Some(idx) if idx != anchor_idx => None,
            _ => Some(anchor_idx),
        }?;

        Some(Self {
            index: Some(index),
            ..self
        })
    }
}

pub type Record<M, Idx, T> = super::Record<MetaWithIdx<M, Idx>, T>;

#[derive(Debug)]
pub enum SegmentedLogError<SE, SDE, CE> {
    StorageError(SE),
    SegmentError(segment::SegmentError<SE, SDE>),
    CacheError(CE),
    CacheNotFound,
    BaseIndexLesserThanInitialIndex,
    WriteSegmentLost,
    IndexOutOfBounds,
    IndexGapEncountered,
}

impl<SE, SDE, CE> std::fmt::Display for SegmentedLogError<SE, SDE, CE>
where
    SE: Error,
    SDE: Error,
    CE: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl<SE, SDE, CE> Error for SegmentedLogError<SE, SDE, CE>
where
    SE: Error,
    SDE: Error,
    CE: Debug,
{
}

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Config<Idx, Size> {
    pub num_index_cached_read_segments: Option<usize>,
    pub segment_config: segment::Config<Size>,
    pub initial_index: Idx,
}

pub struct SegmentedLog<S, M, H, Idx, Size, SERP, SSP, C> {
    write_segment: Option<Segment<S, M, H, Idx, Size, SERP>>,
    read_segments: Vec<Segment<S, M, H, Idx, Size, SERP>>,

    config: Config<Idx, Size>,

    segments_with_cached_index: Option<C>,

    segment_storage_provider: SSP,
}

pub type LogError<S, SERP, C> = SegmentedLogError<
    <S as Storage>::Error,
    <SERP as SerializationProvider>::Error,
    <C as Cache<usize, ()>>::Error,
>;

impl<S, M, H, Idx, Size, SERP, SSP, C> SegmentedLog<S, M, H, Idx, Size, SERP, SSP, C> {
    fn segments(&self) -> impl Iterator<Item = &Segment<S, M, H, Idx, Size, SERP>> {
        self.read_segments.iter().chain(self.write_segment.iter())
    }
}

impl<S, M, H, Idx, SERP, SSP, C> Sizable for SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP, C>
where
    S: Storage,
{
    type Size = S::Size;

    fn size(&self) -> Self::Size {
        self.segments().map(|x| x.size()).sum()
    }
}

impl<S, M, H, Idx, SERP, SSP, C> SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP, C>
where
    S: Storage,
    S::Size: Copy,
    H: Default,
    Idx: Unsigned + FromPrimitive + Copy + Ord,
    SERP: SerializationProvider,
    SSP: SegmentStorageProvider<S, Idx>,
    C: Cache<usize, ()> + Default,
    C::Error: Debug,
{
    pub async fn new(
        config: Config<Idx, S::Size>,
        mut segment_storage_provider: SSP,
    ) -> Result<Self, LogError<S, SERP, C>> {
        let mut segment_base_indices = segment_storage_provider
            .obtain_base_indices_of_stored_segments()
            .await
            .map_err(SegmentedLogError::StorageError)?;

        match segment_base_indices.first() {
            Some(base_index) if base_index < &config.initial_index => {
                Err(SegmentedLogError::BaseIndexLesserThanInitialIndex)
            }
            _ => Ok(()),
        }?;

        let write_segment_base_index = segment_base_indices.pop().unwrap_or(config.initial_index);

        let read_segment_base_indices = segment_base_indices;

        let mut read_segments = Vec::<Segment<S, M, H, Idx, S::Size, SERP>>::with_capacity(
            read_segment_base_indices.len(),
        );

        let index_cache_read_segments = config.num_index_cached_read_segments.is_none();

        for segment_base_index in read_segment_base_indices {
            read_segments.push(
                Segment::with_segment_storage_provider_config_and_base_index(
                    &mut segment_storage_provider,
                    config.segment_config,
                    segment_base_index,
                    index_cache_read_segments,
                )
                .await
                .map_err(SegmentedLogError::SegmentError)?,
            );
        }

        let write_segment = Segment::with_segment_storage_provider_config_and_base_index(
            &mut segment_storage_provider,
            config.segment_config,
            write_segment_base_index,
            true, // write segment is always cached
        )
        .await
        .map_err(SegmentedLogError::SegmentError)?;

        let cache = match config.num_index_cached_read_segments {
            Some(cache_capacity) => {
                let mut cache = C::default();
                cache
                    .reserve(cache_capacity)
                    .map_err(SegmentedLogError::CacheError)?;
                cache
                    .shrink(cache_capacity)
                    .map_err(SegmentedLogError::CacheError)?;
                Some(cache)
            }
            None => None,
        };

        Ok(Self {
            write_segment: Some(write_segment),
            read_segments,
            config,
            segments_with_cached_index: cache,
            segment_storage_provider,
        })
    }
}

macro_rules! new_write_segment {
    ($segmented_log:ident, $base_index:ident) => {
        Segment::with_segment_storage_provider_config_and_base_index(
            &mut $segmented_log.segment_storage_provider,
            $segmented_log.config.segment_config,
            $base_index,
            true,
        )
        .await
        .map_err(SegmentedLogError::SegmentError)
    };
}

macro_rules! consume_segment {
    ($segment:ident, $consume_method:ident) => {
        $segment
            .$consume_method()
            .await
            .map_err(SegmentedLogError::SegmentError)
    };
}

macro_rules! take_write_segment {
    ($segmented_log:ident) => {
        $segmented_log
            .write_segment
            .take()
            .ok_or(SegmentedLogError::WriteSegmentLost)
    };
}

macro_rules! write_segment_ref {
    ($segmented_log:ident, $ref_method:ident) => {
        $segmented_log
            .write_segment
            .$ref_method()
            .ok_or(SegmentedLogError::WriteSegmentLost)
    };
}

#[async_trait(?Send)]
impl<S, M, H, Idx, SERP, SSP, C> AsyncIndexedRead
    for SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP, C>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    SERP: SerializationProvider,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    Idx: Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned,
    C: Cache<usize, ()>,
    C::Error: Debug,
{
    type ReadError = LogError<S, SERP, C>;

    type Idx = Idx;

    type Value = Record<M, Idx, S::Content>;

    fn highest_index(&self) -> Self::Idx {
        self.write_segment
            .as_ref()
            .map(|segment| segment.highest_index())
            .unwrap_or(self.config.initial_index)
    }

    fn lowest_index(&self) -> Self::Idx {
        self.segments()
            .next()
            .map(|segment| segment.lowest_index())
            .unwrap_or(self.config.initial_index)
    }

    async fn read(&self, idx: &Self::Idx) -> Result<Self::Value, Self::ReadError> {
        if !self.has_index(idx) {
            return Err(SegmentedLogError::IndexOutOfBounds);
        }

        self.resolve_segment(self.position_read_segment_with_idx(idx))?
            .read(idx)
            .await
            .map_err(SegmentedLogError::SegmentError)
    }
}

#[derive(Debug)]
enum CacheOpKind {
    Uncache,
    Cache,
    None,
}

#[derive(Debug)]
struct CacheOp {
    segment_id: usize,
    kind: CacheOpKind,
}

impl CacheOp {
    fn new(segment_id: usize, kind: CacheOpKind) -> Self {
        Self { segment_id, kind }
    }
}

impl<S, M, H, Idx, SERP, SSP, C> SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP, C>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    SERP: SerializationProvider,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + FromPrimitive + ToPrimitive + Ord + Copy,
    Idx: Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned,
    C: Cache<usize, ()>,
    C::Error: Debug,
{
    async fn probe_segment(
        &mut self,
        segment_id: Option<usize>,
    ) -> Result<(), LogError<S, SERP, C>> {
        if self.config.num_index_cached_read_segments.is_none() {
            return Ok(());
        }

        let mut cache_op_buf = [
            CacheOp::new(0, CacheOpKind::None),
            CacheOp::new(0, CacheOpKind::None),
        ];

        let cache = self
            .segments_with_cached_index
            .as_mut()
            .ok_or(SegmentedLogError::CacheNotFound)?;

        let cache_ops = match (cache.capacity(), segment_id) {
            (0, _) | (_, None) => Ok(&cache_op_buf[..0]),
            (_, Some(segment_id)) => match cache.query(&segment_id) {
                Ok(Lookup::Hit(_)) => Ok(&cache_op_buf[..0]),
                Ok(Lookup::Miss) => match cache.insert(segment_id, ()) {
                    Ok(Eviction::None) => {
                        cache_op_buf[0] = CacheOp::new(segment_id, CacheOpKind::Cache);
                        Ok(&cache_op_buf[..1])
                    }
                    Ok(Eviction::Block {
                        key: evicted_id,
                        value: _,
                    }) => {
                        cache_op_buf[0] = CacheOp::new(evicted_id, CacheOpKind::Uncache);
                        cache_op_buf[1] = CacheOp::new(segment_id, CacheOpKind::Cache);
                        Ok(&cache_op_buf[..])
                    }
                    Ok(Eviction::Value(_)) => Ok(&cache_op_buf[..0]),
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            },
        }
        .map_err(SegmentedLogError::CacheError)?;

        for segment_cache_op in cache_ops {
            let segment = self.resolve_segment_mut(Some(segment_cache_op.segment_id))?;

            match segment_cache_op.kind {
                CacheOpKind::Uncache => drop(segment.take_cached_index_records()),
                CacheOpKind::Cache => segment
                    .cache_index()
                    .await
                    .map_err(SegmentedLogError::SegmentError)?,
                CacheOpKind::None => {}
            }
        }

        Ok(())
    }

    fn unregister_cache_for_segments<SI>(
        &mut self,
        segment_ids: SI,
    ) -> Result<(), LogError<S, SERP, C>>
    where
        SI: Iterator<Item = usize>,
    {
        if self.config.num_index_cached_read_segments.is_none() {
            return Ok(());
        }

        let cache = self
            .segments_with_cached_index
            .as_mut()
            .ok_or(SegmentedLogError::CacheNotFound)?;

        if cache.capacity() == 0 {
            return Ok(());
        }

        for segment_id in segment_ids {
            cache
                .remove(&segment_id)
                .map_err(SegmentedLogError::CacheError)?;
        }

        Ok(())
    }
}

impl<S, M, H, Idx, SERP, SSP, C> SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP, C>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    SERP: SerializationProvider,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + FromPrimitive + ToPrimitive + Ord + Copy,
    Idx: Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned,
    C: Cache<usize, ()>,
    C::Error: Debug,
{
    pub async fn read_seq_exclusive(
        &mut self,
        segment_id: usize,
        idx: &Idx,
    ) -> Result<SeqRead<M, Idx, S::Content>, LogError<S, SERP, C>> {
        if !self.has_index(idx) {
            return Err(SegmentedLogError::IndexOutOfBounds);
        }

        let num_read_segments = self.read_segments.len();

        let segment_id = Some(segment_id).filter(|x| x < &num_read_segments);

        self.probe_segment(segment_id).await?;

        self.read_seq_unchecked(segment_id, idx).await
    }
}

#[async_trait(?Send)]
impl<S, M, H, Idx, SERP, SSP, C> AsyncIndexedExclusiveRead
    for SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP, C>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    SERP: SerializationProvider,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + FromPrimitive + ToPrimitive + Ord + Copy,
    Idx: Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned,
    C: Cache<usize, ()>,
    C::Error: Debug,
{
    async fn exclusive_read(&mut self, idx: &Self::Idx) -> Result<Self::Value, Self::ReadError> {
        if !self.has_index(idx) {
            return Err(SegmentedLogError::IndexOutOfBounds);
        }

        let segment_id = self.position_read_segment_with_idx(idx);

        self.probe_segment(segment_id).await?;

        self.resolve_segment(segment_id)?
            .read(idx)
            .await
            .map_err(SegmentedLogError::SegmentError)
    }
}

pub enum SeqRead<M, Idx, C> {
    Read {
        record: Record<M, Idx, C>,
        next_idx: Idx,
    },
    Seek {
        next_segment: usize,
        next_idx: Idx,
    },
}

pub type ResolvedSegmentMutResult<'a, S, M, H, Idx, SERP, C> =
    Result<&'a mut Segment<S, M, H, Idx, <S as Sizable>::Size, SERP>, LogError<S, SERP, C>>;

pub type ResolvedSegmentResult<'a, S, M, H, Idx, SERP, C> =
    Result<&'a Segment<S, M, H, Idx, <S as Sizable>::Size, SERP>, LogError<S, SERP, C>>;

impl<S, M, H, Idx, SERP, SSP, C> SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP, C>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    SERP: SerializationProvider,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    Idx: Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned,
    C: Cache<usize, ()>,
    C::Error: Debug,
{
    fn position_read_segment_with_idx(&self, idx: &Idx) -> Option<usize> {
        self.has_index(idx).then_some(())?;

        self.read_segments
            .binary_search_by(|segment| match idx {
                idx if &segment.lowest_index() > idx => Ordering::Greater,
                idx if &segment.highest_index() <= idx => Ordering::Less,
                _ => Ordering::Equal,
            })
            .ok()
    }

    fn resolve_segment_mut(
        &mut self,
        segment_id: Option<usize>,
    ) -> ResolvedSegmentMutResult<S, M, H, Idx, SERP, C> {
        match segment_id {
            Some(segment_id) => self
                .read_segments
                .get_mut(segment_id)
                .ok_or(SegmentedLogError::IndexGapEncountered),
            None => write_segment_ref!(self, as_mut),
        }
    }

    fn resolve_segment(
        &self,
        segment_id: Option<usize>,
    ) -> ResolvedSegmentResult<S, M, H, Idx, SERP, C> {
        match segment_id {
            Some(segment_id) => self
                .read_segments
                .get(segment_id)
                .ok_or(SegmentedLogError::IndexGapEncountered),
            None => write_segment_ref!(self, as_ref),
        }
    }

    async fn read_seq_unchecked(
        &self,
        segment_id: Option<usize>,
        idx: &Idx,
    ) -> Result<SeqRead<M, Idx, S::Content>, LogError<S, SERP, C>> {
        let segment = self.resolve_segment(segment_id)?;

        match (idx, segment_id) {
            (idx, Some(segment_id)) if idx >= &segment.highest_index() => Ok(SeqRead::Seek {
                next_segment: segment_id + 1,
                next_idx: *idx,
            }),
            _ => segment
                .read(idx)
                .await
                .map_err(SegmentedLogError::SegmentError)
                .map(|record| SeqRead::Read {
                    record,
                    next_idx: *idx + Idx::one(),
                }),
        }
    }

    pub async fn read_seq(
        &self,
        segment_id: usize,
        idx: &Idx,
    ) -> Result<SeqRead<M, Idx, S::Content>, LogError<S, SERP, C>> {
        if !self.has_index(idx) {
            return Err(SegmentedLogError::IndexOutOfBounds);
        }

        let num_read_segments = self.read_segments.len();

        let segment_id = Some(segment_id).filter(|x| x < &num_read_segments);

        self.read_seq_unchecked(segment_id, idx).await
    }

    pub fn stream<RB>(
        &self,
        index_bounds: RB,
    ) -> impl Stream<Item = Record<M, Idx, S::Content>> + '_
    where
        RB: RangeBounds<Idx>,
    {
        let (lo, hi) =
            index_bounds_for_range(index_bounds, self.lowest_index(), self.highest_index());

        let segments = match (
            self.position_read_segment_with_idx(&lo),
            self.position_read_segment_with_idx(&hi),
        ) {
            (Some(lo_seg), Some(hi_seg)) if lo_seg <= hi_seg => {
                &self.read_segments[lo_seg..=hi_seg]
            }
            (Some(lo_seg), None) => &self.read_segments[lo_seg..],
            _ => &[],
        }
        .iter()
        .chain(self.write_segment.iter());

        stream::iter(segments)
            .map(move |segment| indexed_read_stream(segment, lo..=hi))
            .flatten()
    }

    pub fn stream_unbounded(&self) -> impl Stream<Item = Record<M, Idx, S::Content>> + '_ {
        stream::iter(self.segments())
            .map(move |segment| indexed_read_stream(segment, ..))
            .flatten()
    }
}

impl<S, M, H, Idx, SERP, SSP, C> SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP, C>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    S::Size: Copy,
    H: Hasher + Default,
    Idx: FromPrimitive + ToPrimitive + Unsigned + CheckedSub,
    Idx: Copy + Ord + Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned,
    SERP: SerializationProvider,
    SSP: SegmentStorageProvider<S, Idx>,
    C: Cache<usize, ()>,
    C::Error: Debug,
{
    pub async fn rotate_new_write_segment(&mut self) -> Result<(), LogError<S, SERP, C>> {
        self.flush().await?;

        let mut write_segment = take_write_segment!(self)?;
        let next_index = write_segment.highest_index();

        if let Some(0) = self.config.num_index_cached_read_segments {
            drop(write_segment.take_cached_index_records());
        }

        let rotated_segment_id = self.read_segments.len();
        self.read_segments.push(write_segment);

        self.probe_segment(Some(rotated_segment_id)).await?;

        self.write_segment = Some(new_write_segment!(self, next_index)?);

        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), LogError<S, SERP, C>> {
        let write_segment = take_write_segment!(self)?;

        let write_segment = write_segment
            .flush(&mut self.segment_storage_provider)
            .await
            .map_err(SegmentedLogError::SegmentError)?;

        self.write_segment = Some(write_segment);

        Ok(())
    }

    pub async fn remove_expired_segments(
        &mut self,
        expiry_duration: Duration,
    ) -> Result<Idx, LogError<S, SERP, C>> {
        if write_segment_ref!(self, as_ref)?.is_empty() {
            self.flush().await?
        }

        let next_index = self.highest_index();

        let mut segments = std::mem::take(&mut self.read_segments);
        segments.push(take_write_segment!(self)?);

        let segment_pos_in_vec = segments
            .iter()
            .position(|segment| !segment.has_expired(expiry_duration));

        let (mut to_remove, mut to_keep) = if let Some(pos) = segment_pos_in_vec {
            let non_expired_segments = segments.split_off(pos);
            (segments, non_expired_segments)
        } else {
            (segments, Vec::new())
        };

        let write_segment = if let Some(write_segment) = to_keep.pop() {
            write_segment
        } else {
            new_write_segment!(self, next_index)?
        };

        self.read_segments = to_keep;
        self.write_segment = Some(write_segment);

        let to_remove_len = to_remove.len();

        let mut num_records_removed = <Idx as num::Zero>::zero();
        for segment in to_remove.drain(..) {
            num_records_removed = num_records_removed + segment.len();
            consume_segment!(segment, remove)?;
        }

        self.unregister_cache_for_segments(0..to_remove_len)?;

        Ok(num_records_removed)
    }
}

impl<S, M, H, Idx, SERP, SSP, C> SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP, C>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    S::Size: Copy,
    H: Hasher + Default,
    Idx: FromPrimitive + ToPrimitive + Unsigned + CheckedSub,
    Idx: Copy + Ord + Serialize + DeserializeOwned,
    M: Clone + Serialize + DeserializeOwned,
    SERP: SerializationProvider,
    SSP: SegmentStorageProvider<S, Idx>,
    C: Cache<usize, ()>,
    C::Error: Debug,
{
    pub async fn append_record_with_contiguous_bytes<X>(
        &mut self,
        record: &Record<M, Idx, X>,
    ) -> Result<Idx, LogError<S, SERP, C>>
    where
        X: Deref<Target = [u8]>,
    {
        if write_segment_ref!(self, as_ref)?.is_maxed() {
            self.rotate_new_write_segment().await?;
        }

        write_segment_ref!(self, as_mut)?
            .append_record_with_contiguous_bytes(record)
            .await
            .map_err(SegmentedLogError::SegmentError)
    }
}

#[async_trait(?Send)]
impl<S, M, H, Idx, SERP, SSP, C> AsyncTruncate for SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP, C>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    S::Size: Copy,
    SERP: SerializationProvider,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + FromPrimitive + ToPrimitive + Ord + Copy,
    Idx: Serialize + DeserializeOwned,
    M: Default + Serialize + DeserializeOwned,
    SSP: SegmentStorageProvider<S, Idx>,
    C: Cache<usize, ()>,
    C::Error: Debug,
{
    type TruncError = LogError<S, SERP, C>;

    type Mark = Idx;

    async fn truncate(&mut self, idx: &Self::Mark) -> Result<(), Self::TruncError> {
        if !self.has_index(idx) {
            return Err(SegmentedLogError::IndexOutOfBounds);
        }

        let write_segment = write_segment_ref!(self, as_mut)?;

        if idx >= &write_segment.lowest_index() {
            return write_segment
                .truncate(idx)
                .await
                .map_err(SegmentedLogError::SegmentError);
        }

        let segment_pos_in_vec = self
            .position_read_segment_with_idx(idx)
            .ok_or(SegmentedLogError::IndexGapEncountered)?;

        let segment_to_truncate = self
            .read_segments
            .get_mut(segment_pos_in_vec)
            .ok_or(SegmentedLogError::IndexGapEncountered)?;

        segment_to_truncate
            .truncate(idx)
            .await
            .map_err(SegmentedLogError::SegmentError)?;

        let next_index = segment_to_truncate.highest_index();

        let mut segments_to_remove = self.read_segments.split_off(segment_pos_in_vec + 1);
        segments_to_remove.push(take_write_segment!(self)?);

        let segments_to_remove_len = segments_to_remove.len();

        for segment in segments_to_remove.drain(..) {
            consume_segment!(segment, remove)?;
        }

        self.write_segment = Some(new_write_segment!(self, next_index)?);

        self.unregister_cache_for_segments(
            (0..segments_to_remove_len).map(|x| x + segment_pos_in_vec + 1),
        )?;

        Ok(())
    }
}

macro_rules! consume_segmented_log {
    ($segmented_log:ident, $consume_method:ident) => {
        let segments = &mut $segmented_log.read_segments;
        segments.push(take_write_segment!($segmented_log)?);
        for segment in segments.drain(..) {
            consume_segment!(segment, $consume_method)?;
        }
    };
}

#[async_trait(?Send)]
impl<S, M, H, Idx, SERP, SSP, C> AsyncConsume for SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP, C>
where
    S: Storage,
    SERP: SerializationProvider,
    C: Cache<usize, ()>,
    C::Error: Debug,
{
    type ConsumeError = LogError<S, SERP, C>;

    async fn remove(mut self) -> Result<(), Self::ConsumeError> {
        consume_segmented_log!(self, remove);
        Ok(())
    }

    async fn close(mut self) -> Result<(), Self::ConsumeError> {
        consume_segmented_log!(self, close);
        Ok(())
    }
}

#[async_trait(?Send)]
impl<S, M, H, Idx, SERP, SSP, C> CommitLog<MetaWithIdx<M, Idx>, S::Content>
    for SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP, C>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    S::Size: Copy,
    H: Hasher + Default,
    Idx: FromPrimitive + ToPrimitive + Unsigned + CheckedSub,
    Idx: Copy + Ord + Serialize + DeserializeOwned,
    M: Default + Serialize + DeserializeOwned,
    SERP: SerializationProvider,
    SSP: SegmentStorageProvider<S, Idx>,
    C: Cache<usize, ()>,
    C::Error: Debug,
{
    type Error = LogError<S, SERP, C>;

    async fn remove_expired(
        &mut self,
        expiry_duration: std::time::Duration,
    ) -> Result<Self::Idx, Self::Error> {
        self.remove_expired_segments(expiry_duration).await
    }

    async fn append<X, XBuf, XE>(
        &mut self,
        record: Record<M, Idx, X>,
    ) -> Result<Self::Idx, Self::Error>
    where
        X: Stream<Item = Result<XBuf, XE>>,
        X: Unpin + 'async_trait,
        XBuf: Deref<Target = [u8]>,
    {
        if write_segment_ref!(self, as_ref)?.is_maxed() {
            self.rotate_new_write_segment().await?;
        }

        write_segment_ref!(self, as_mut)?
            .append(record)
            .await
            .map_err(SegmentedLogError::SegmentError)
    }
}

pub(crate) mod test {
    use crate::common::cache::NoOpCache;

    use super::{
        super::super::commit_log::test::_test_indexed_read_contains_expected_records,
        segment::test::_segment_config, store::test::_RECORDS, *,
    };
    use std::{convert::Infallible, fmt::Debug, future::Future, marker::PhantomData};

    pub fn _test_records_provider<'a, const N: usize>(
        record_source: &'a [&'a [u8; N]],
        num_segments: usize,
        records_per_segment: usize,
    ) -> impl Iterator<Item = &'a [u8]> {
        record_source
            .iter()
            .cycle()
            .take(records_per_segment * num_segments)
            .cloned()
            .map(|x| {
                let x: &[u8] = x;
                x
            })
    }

    pub(crate) async fn _test_segmented_log_read_append_truncate_consistency<
        S,
        M,
        H,
        Idx,
        SERP,
        SSP,
    >(
        _segment_storage_provider: SSP,
        _: PhantomData<(M, H, SERP)>,
    ) where
        S: Storage,
        S::Size: FromPrimitive + Copy,
        S::Content: SplitAt<u8>,
        S::Position: ToPrimitive + Debug,
        M: Default + Serialize + DeserializeOwned + Clone,
        H: Hasher + Default,
        Idx: Unsigned + CheckedSub + FromPrimitive + ToPrimitive,
        Idx: Ord + Copy + Debug,
        Idx: Serialize + DeserializeOwned,
        SERP: SerializationProvider,
        SSP: SegmentStorageProvider<S, Idx> + Clone,
    {
        const INITIAL_INDEX: usize = 42;

        let initial_index = Idx::from_usize(INITIAL_INDEX).unwrap();

        const NUM_SEGMENTS: usize = 10;

        let config = Config {
            segment_config: _segment_config::<M, Idx, S::Size, SERP>(
                _RECORDS[0].len(),
                _RECORDS.len(),
            )
            .unwrap(),
            initial_index,
            num_index_cached_read_segments: None,
        };

        let mut segmented_log =
            SegmentedLog::<S, M, H, Idx, S::Size, SERP, SSP, NoOpCache<usize, ()>>::new(
                config,
                _segment_storage_provider.clone(),
            )
            .await
            .unwrap();

        for record in _test_records_provider(&_RECORDS, NUM_SEGMENTS, _RECORDS.len()) {
            let record = Record {
                metadata: MetaWithIdx {
                    metadata: M::default(),
                    index: Option::<Idx>::None,
                },
                value: record,
            };
            segmented_log
                .append_record_with_contiguous_bytes(&record)
                .await
                .unwrap();
        }

        let expected_minimum_written_records =
            Idx::from_usize(_RECORDS.len() * (NUM_SEGMENTS - 1)).unwrap();

        assert!(
            segmented_log.len() > expected_minimum_written_records,
            "Maxed segments not rotated"
        );

        segmented_log.close().await.unwrap();

        let mut segmented_log =
            SegmentedLog::<S, M, H, Idx, S::Size, SERP, SSP, NoOpCache<usize, ()>>::new(
                config,
                _segment_storage_provider.clone(),
            )
            .await
            .unwrap();

        _test_indexed_read_contains_expected_records(
            &segmented_log,
            _test_records_provider(&_RECORDS, NUM_SEGMENTS, _RECORDS.len()),
            _RECORDS.len() * NUM_SEGMENTS,
        )
        .await;

        {
            let expected_record_count = _RECORDS.len();

            let segmented_log_stream = segmented_log
                .stream(..(Idx::from_usize(expected_record_count).unwrap() + initial_index));

            let expected_records = _test_records_provider(&_RECORDS, NUM_SEGMENTS, _RECORDS.len());

            let record_count = segmented_log_stream
                .zip(futures_lite::stream::iter(expected_records))
                .map(|(record, expected_record_value)| {
                    assert_eq!(record.value.deref(), expected_record_value);
                    Some(())
                })
                .count()
                .await;

            assert_eq!(record_count, expected_record_count);

            let segmented_log_stream_unbounded = segmented_log.stream_unbounded();
            let segmented_log_stream_bounded = segmented_log.stream(..);

            let expected_records = _test_records_provider(&_RECORDS, NUM_SEGMENTS, _RECORDS.len());

            let expected_record_count = _RECORDS.len() * NUM_SEGMENTS;

            let record_count = segmented_log_stream_unbounded
                .zip(segmented_log_stream_bounded)
                .zip(futures_lite::stream::iter(expected_records))
                .map(|((record_x, record_y), expected_record_value)| {
                    assert_eq!(record_x.value.deref(), expected_record_value);
                    assert_eq!(record_y.value.deref(), expected_record_value);
                    Some(())
                })
                .count()
                .await;

            assert_eq!(record_count, expected_record_count);
        };

        {
            let (mut segment_id, mut idx) = (0_usize, segmented_log.lowest_index());

            let mut expected_records =
                _test_records_provider(&_RECORDS, NUM_SEGMENTS, _RECORDS.len());

            loop {
                let seq_read = segmented_log.read_seq(segment_id, &idx).await;

                (segment_id, idx) = match seq_read {
                    Ok(SeqRead::Read { record, next_idx }) => {
                        assert_eq!(Some(record.value.deref()), expected_records.next());
                        (segment_id, next_idx)
                    }

                    Ok(SeqRead::Seek {
                        next_segment,
                        next_idx,
                    }) => (next_segment, next_idx),

                    _ => break,
                }
            }

            assert_eq!(idx, segmented_log.highest_index());
        }

        let truncate_index = INITIAL_INDEX + NUM_SEGMENTS / 2 * _RECORDS.len() + _RECORDS.len() / 2;

        let truncate_index = Idx::from_usize(truncate_index).unwrap();

        let expected_length_after_truncate = truncate_index - segmented_log.lowest_index();

        segmented_log.truncate(&truncate_index).await.unwrap();

        assert_eq!(segmented_log.len(), expected_length_after_truncate);

        segmented_log
            .append(Record {
                metadata: MetaWithIdx {
                    metadata: M::default(),
                    index: None,
                },
                value: futures_lite::stream::once(Ok::<&[u8], Infallible>(_RECORDS[0])),
            })
            .await
            .unwrap();

        if segmented_log
            .append(Record {
                metadata: MetaWithIdx {
                    metadata: M::default(),
                    index: None,
                },
                value: futures_lite::stream::iter(
                    _test_records_provider(&_RECORDS, 2, _RECORDS.len())
                        .map(Ok::<&[u8], Infallible>),
                ),
            })
            .await
            .is_ok()
        {
            unreachable!("Wrong result on exceeding max_store_bytes");
        }

        let write_segment_truncate_index = Idx::from_usize(INITIAL_INDEX).unwrap()
            + segmented_log.len()
            - Idx::from_usize(1).unwrap();

        segmented_log
            .truncate(&write_segment_truncate_index)
            .await
            .unwrap();

        let len_before_close = segmented_log.len();

        segmented_log.close().await.unwrap();

        let segmented_log =
            SegmentedLog::<S, M, H, Idx, S::Size, SERP, SSP, NoOpCache<usize, ()>>::new(
                config,
                _segment_storage_provider.clone(),
            )
            .await
            .unwrap();

        let len_after_close = segmented_log.len();

        assert_eq!(len_before_close, len_after_close);

        segmented_log.remove().await.unwrap();

        let segmented_log =
            SegmentedLog::<S, M, H, Idx, S::Size, SERP, SSP, NoOpCache<usize, ()>>::new(
                config,
                _segment_storage_provider.clone(),
            )
            .await
            .unwrap();

        assert!(
            segmented_log.is_empty(),
            "SegmentedLog not empty after removing."
        );

        segmented_log.remove().await.unwrap();
    }

    pub(crate) async fn _test_segmented_log_remove_expired_segments<
        S,
        M,
        H,
        Idx,
        SERP,
        SSP,
        MTF,
        TF,
    >(
        _segment_storage_provider: SSP,
        _make_sleep_future: MTF,
        _: PhantomData<(M, H, SERP)>,
    ) where
        S: Storage,
        S::Size: FromPrimitive + Copy,
        S::Content: SplitAt<u8>,
        S::Position: ToPrimitive + Debug,
        M: Default + Serialize + DeserializeOwned + Clone,
        H: Hasher + Default,
        Idx: Unsigned + CheckedSub + FromPrimitive + ToPrimitive,
        Idx: Ord + Copy + Debug,
        Idx: Serialize + DeserializeOwned,
        SERP: SerializationProvider,
        SSP: SegmentStorageProvider<S, Idx> + Clone,
        MTF: Fn(Duration) -> TF,
        TF: Future<Output = ()>,
    {
        const INITIAL_INDEX: usize = 42;

        let initial_index = Idx::from_usize(INITIAL_INDEX).unwrap();

        const NUM_SEGMENTS: usize = 10;

        let config = Config {
            segment_config: _segment_config::<M, Idx, S::Size, SERP>(
                _RECORDS[0].len(),
                _RECORDS.len(),
            )
            .unwrap(),
            initial_index,
            num_index_cached_read_segments: None,
        };

        let mut segmented_log =
            SegmentedLog::<S, M, H, Idx, S::Size, SERP, SSP, NoOpCache<usize, ()>>::new(
                config,
                _segment_storage_provider.clone(),
            )
            .await
            .unwrap();

        for record in _test_records_provider(&_RECORDS, NUM_SEGMENTS / 2, _RECORDS.len()) {
            let stream = futures_lite::stream::once(Ok::<&[u8], Infallible>(record));

            segmented_log
                .append(Record {
                    metadata: MetaWithIdx {
                        metadata: M::default(),
                        index: Option::<Idx>::None,
                    },
                    value: stream,
                })
                .await
                .unwrap();
        }

        let segmented_log_highest_index_before_sleep = segmented_log.highest_index();

        let expiry_duration = Duration::from_millis(10);

        // we keep a flag variable to sleep only after the last write segment has
        // rotated back to the vec of read segments
        let mut need_to_sleep = true;

        for record in _test_records_provider(&_RECORDS, NUM_SEGMENTS / 2, _RECORDS.len()) {
            let stream = futures_lite::stream::once(Ok::<&[u8], Infallible>(record));

            segmented_log
                .append(Record {
                    metadata: MetaWithIdx {
                        metadata: M::default(),
                        index: Option::<Idx>::None,
                    },
                    value: stream,
                })
                .await
                .unwrap();

            if need_to_sleep {
                _make_sleep_future(expiry_duration).await;
            }

            need_to_sleep = false;
        }

        segmented_log.remove_expired(expiry_duration).await.unwrap();

        assert!(
            segmented_log_highest_index_before_sleep <= segmented_log.lowest_index(),
            "Expired segments not removed."
        );

        _make_sleep_future(expiry_duration).await;

        segmented_log.remove_expired(expiry_duration).await.unwrap();

        assert!(
            segmented_log.is_empty(),
            "Segmented log not cleared after all segments expired."
        );

        segmented_log.remove().await.unwrap();
    }

    pub(crate) async fn _test_segmented_log_segment_index_caching<
        S,
        M,
        H,
        Idx,
        SERP,
        SSP,
        MTF,
        TF,
    >(
        _segment_storage_provider: SSP,
        _make_sleep_future: MTF,
        _test_zero_cap_cache: bool,
        _: PhantomData<(M, H, SERP)>,
    ) where
        S: Storage,
        S::Size: FromPrimitive + Copy,
        S::Content: SplitAt<u8>,
        S::Position: ToPrimitive + Debug,
        M: Default + Serialize + DeserializeOwned + Clone,
        H: Hasher + Default,
        Idx: Unsigned + CheckedSub + FromPrimitive + ToPrimitive,
        Idx: Ord + Copy + Debug,
        Idx: Serialize + DeserializeOwned,
        SERP: SerializationProvider,
        SSP: SegmentStorageProvider<S, Idx> + Clone,
        MTF: Fn(Duration) -> TF,
        TF: Future<Output = ()>,
    {
        const INITIAL_INDEX: usize = 42;

        const NUM_INDEX_CACHED_SEGMENTS: usize = 5;

        const RECORDS_PER_SEGMENT: usize = _RECORDS.len();

        const RECORD_LEN: usize = _RECORDS[0].len();

        let initial_index = Idx::from_usize(INITIAL_INDEX).unwrap();

        let config = Config {
            num_index_cached_read_segments: Some(NUM_INDEX_CACHED_SEGMENTS),
            segment_config: _segment_config::<M, Idx, S::Size, SERP>(
                RECORD_LEN,
                RECORDS_PER_SEGMENT,
            )
            .unwrap(),
            initial_index,
        };

        let mut segmented_log =
            SegmentedLog::<_, M, H, _, _, SERP, _, AllocLRUCache<usize, ()>>::new(
                config,
                _segment_storage_provider.clone(),
            )
            .await
            .unwrap();

        segmented_log
            .append(Record {
                metadata: MetaWithIdx {
                    metadata: M::default(),
                    index: None,
                },
                value: futures_lite::stream::once(Ok::<&[u8], Infallible>(_RECORDS[0])),
            })
            .await
            .unwrap();

        assert_eq!(segmented_log.segments().count(), 1);

        assert!(segmented_log
            .segments()
            .next()
            .unwrap()
            .cached_index_records()
            .is_some());

        for record in _test_records_provider(&_RECORDS, 1, RECORDS_PER_SEGMENT) {
            let stream = futures_lite::stream::once(Ok::<&[u8], Infallible>(record));

            segmented_log
                .append(Record {
                    metadata: MetaWithIdx {
                        metadata: M::default(),
                        index: None,
                    },
                    value: stream,
                })
                .await
                .unwrap();
        }

        assert_eq!(segmented_log.segments().count(), 2);

        assert!(segmented_log
            .segments()
            .all(|x| x.cached_index_records().is_some()));

        for record in
            _test_records_provider(&_RECORDS, NUM_INDEX_CACHED_SEGMENTS, RECORDS_PER_SEGMENT)
        {
            let stream = futures_lite::stream::once(Ok::<&[u8], Infallible>(record));

            segmented_log
                .append(Record {
                    metadata: MetaWithIdx {
                        metadata: M::default(),
                        index: None,
                    },
                    value: stream,
                })
                .await
                .unwrap();
        }

        assert_eq!(
            segmented_log.segments().count(),
            NUM_INDEX_CACHED_SEGMENTS + 2
        );

        {
            let mut segments = segmented_log.segments();

            assert!(segments.next().unwrap().cached_index_records().is_none());
            assert!(segments.all(|x| x.cached_index_records().is_some()));
        }

        {
            segmented_log.exclusive_read(&initial_index).await.unwrap();
            segmented_log.exclusive_read(&initial_index).await.unwrap();

            let mut segments = segmented_log.segments();

            assert!(segments.next().unwrap().cached_index_records().is_some());
            assert!(segments.next().unwrap().cached_index_records().is_none());
            assert!(segments.all(|x| x.cached_index_records().is_some()));
        }

        segmented_log = if _test_zero_cap_cache {
            segmented_log.close().await.unwrap();

            let cache_disabled_config = Config {
                num_index_cached_read_segments: Some(0),
                ..config
            };

            let segmented_log =
                SegmentedLog::<_, M, H, _, _, SERP, _, AllocLRUCache<usize, ()>>::new(
                    cache_disabled_config,
                    _segment_storage_provider.clone(),
                )
                .await
                .unwrap();

            assert!(segmented_log
                .segments()
                .last()
                .unwrap()
                .cached_index_records()
                .is_some());

            segmented_log
        } else {
            segmented_log
        };

        segmented_log.exclusive_read(&initial_index).await.unwrap();

        for record in _test_records_provider(&_RECORDS, 1, RECORDS_PER_SEGMENT) {
            let stream = futures_lite::stream::once(Ok::<&[u8], Infallible>(record));

            segmented_log
                .append(Record {
                    metadata: MetaWithIdx {
                        metadata: M::default(),
                        index: None,
                    },
                    value: stream,
                })
                .await
                .unwrap();
        }

        if _test_zero_cap_cache {
            assert_eq!(
                segmented_log.segments().count(),
                NUM_INDEX_CACHED_SEGMENTS + 3
            );

            assert!(segmented_log
                .segments()
                .take(NUM_INDEX_CACHED_SEGMENTS + 2)
                .all(|x| x.cached_index_records().is_none()));

            assert!(segmented_log
                .segments()
                .last()
                .unwrap()
                .cached_index_records()
                .is_some());
        }

        segmented_log.close().await.unwrap();

        let mut segmented_log =
            SegmentedLog::<_, M, H, _, _, SERP, _, AllocLRUCache<usize, ()>>::new(
                config,
                _segment_storage_provider.clone(),
            )
            .await
            .unwrap();

        const NUM_SEGMENTS: usize = NUM_INDEX_CACHED_SEGMENTS + 3;

        assert!(segmented_log
            .segments()
            .take(NUM_SEGMENTS - 1)
            .all(|x| x.cached_index_records().is_none()));

        {
            let expected_records =
                _test_records_provider(&_RECORDS, NUM_SEGMENTS - 1, RECORDS_PER_SEGMENT);

            let mut expected_records =
                std::iter::once(_RECORDS[0] as &[u8]).chain(expected_records);

            let (mut segment_id, mut idx) = (0_usize, segmented_log.lowest_index());

            loop {
                let seq_read = segmented_log.read_seq_exclusive(segment_id, &idx).await;

                (segment_id, idx) = match seq_read {
                    Ok(SeqRead::Read { record, next_idx }) => {
                        assert_eq!(Some(record.value.deref()), expected_records.next());

                        assert!(segmented_log
                            .segments()
                            .nth(segment_id)
                            .unwrap()
                            .cached_index_records()
                            .is_some());

                        if segment_id < NUM_SEGMENTS - 1 {
                            let segment = segment_id
                                .checked_sub(NUM_INDEX_CACHED_SEGMENTS)
                                .and_then(|x| segmented_log.segments().nth(x));

                            assert!(segment
                                .map(|x| x.cached_index_records().is_none())
                                .unwrap_or(true));
                        }

                        (segment_id, next_idx)
                    }

                    Ok(SeqRead::Seek {
                        next_segment,
                        next_idx,
                    }) => (next_segment, next_idx),

                    _ => break,
                }
            }

            assert_eq!(idx, segmented_log.highest_index());

            assert!(expected_records.next().is_none());
        }

        const WRITE_SEGMENT_ID: usize = NUM_SEGMENTS - 1;

        const SECOND_LAST_READ_SEGMENT_ID: usize = WRITE_SEGMENT_ID - 2;

        let truncate_index =
            INITIAL_INDEX + (SECOND_LAST_READ_SEGMENT_ID) * _RECORDS.len() + _RECORDS.len() / 2;

        let truncate_index = Idx::from_usize(truncate_index).unwrap();

        let expected_length_after_truncate = truncate_index - segmented_log.lowest_index();

        segmented_log.truncate(&truncate_index).await.unwrap();

        assert_eq!(segmented_log.len(), expected_length_after_truncate);

        const NUM_SEGMENTS_AFTER_TRUNCATE: usize = NUM_SEGMENTS - 1;

        assert_eq!(
            segmented_log.segments().count(),
            NUM_SEGMENTS_AFTER_TRUNCATE
        );

        const FIRST_REMAINING_CACHED_SEGMENT: usize =
            NUM_SEGMENTS_AFTER_TRUNCATE - NUM_INDEX_CACHED_SEGMENTS;

        for (segment_id, segment) in segmented_log.segments().enumerate() {
            if segment_id >= FIRST_REMAINING_CACHED_SEGMENT {
                assert!(segment.cached_index_records().is_some());
            } else {
                assert!(segment.cached_index_records().is_none());
            }
        }

        let expiry_duration = Duration::from_millis(50);

        _make_sleep_future(expiry_duration).await;

        for record in _test_records_provider(&_RECORDS, 3, RECORDS_PER_SEGMENT) {
            let stream = futures_lite::stream::once(Ok::<&[u8], Infallible>(record));

            segmented_log
                .append(Record {
                    metadata: MetaWithIdx {
                        metadata: M::default(),
                        index: None,
                    },
                    value: stream,
                })
                .await
                .unwrap();
        }

        const NUM_SEGMENTS_ON_APPEND_AFTER_SLEEP: usize = NUM_SEGMENTS_AFTER_TRUNCATE + 2;

        assert_eq!(
            segmented_log.segments().count(),
            NUM_SEGMENTS_ON_APPEND_AFTER_SLEEP
        );

        segmented_log.remove_expired(expiry_duration).await.unwrap();

        assert_eq!(segmented_log.segments().count(), 2);

        assert!(segmented_log
            .segments()
            .all(|x| x.cached_index_records().is_some()));

        segmented_log.remove().await.unwrap();
    }
}
