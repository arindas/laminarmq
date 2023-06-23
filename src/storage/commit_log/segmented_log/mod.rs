pub mod index;
pub mod segment;
pub mod store;

use self::segment::{Segment, SegmentStorageProvider};
use super::{
    super::super::{
        common::{serde_compat::SerializationProvider, split::SplitAt},
        storage::common::{index_bounds_for_range, indexed_read_stream},
        storage::{AsyncConsume, AsyncIndexedRead, AsyncTruncate},
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
pub enum SegmentedLogError<SE, SDE> {
    StorageError(SE),
    SegmentError(segment::SegmentError<SE, SDE>),
    BaseIndexLesserThanInitialIndex,
    NoSegmentsCreated,
    WriteSegmentLost,
    IndexOutOfBounds,
    IndexGapEncountered,
}

impl<SE, SDE> std::fmt::Display for SegmentedLogError<SE, SDE>
where
    SE: std::error::Error,
    SDE: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl<SE, SDE> std::error::Error for SegmentedLogError<SE, SDE>
where
    SE: std::error::Error,
    SDE: std::error::Error,
{
}

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Config<Idx, Size> {
    pub segment_config: segment::Config<Size>,
    pub initial_index: Idx,
}

pub struct SegmentedLog<S, M, H, Idx, Size, SERP, SSP> {
    write_segment: Option<Segment<S, M, H, Idx, Size, SERP>>,
    read_segments: Vec<Segment<S, M, H, Idx, Size, SERP>>,

    config: Config<Idx, Size>,

    segment_storage_provider: SSP,
}

pub type LogError<S, SERP> =
    SegmentedLogError<<S as Storage>::Error, <SERP as SerializationProvider>::Error>;

impl<S, M, H, Idx, Size, SERP, SSP> SegmentedLog<S, M, H, Idx, Size, SERP, SSP> {
    fn segments(&self) -> impl Iterator<Item = &Segment<S, M, H, Idx, Size, SERP>> {
        self.read_segments.iter().chain(self.write_segment.iter())
    }
}

impl<S, M, H, Idx, SERP, SSP> Sizable for SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP>
where
    S: Storage,
{
    type Size = S::Size;

    fn size(&self) -> Self::Size {
        self.segments().map(|x| x.size()).sum()
    }
}

impl<S, M, H, Idx, SERP, SSP> SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP>
where
    S: Storage,
    S::Size: Copy,
    H: Default,
    Idx: FromPrimitive + Copy + Ord,
    SERP: SerializationProvider,
    SSP: SegmentStorageProvider<S, Idx>,
{
    pub async fn new(
        config: Config<Idx, S::Size>,
        mut segment_storage_provider: SSP,
    ) -> Result<Self, LogError<S, SERP>> {
        let mut segment_base_indices = segment_storage_provider
            .obtain_base_indices_of_stored_segments()
            .await
            .map_err(SegmentedLogError::StorageError)?;

        match segment_base_indices.first() {
            Some(base_index) if base_index < &config.initial_index => {
                return Err(SegmentedLogError::BaseIndexLesserThanInitialIndex);
            }
            None => segment_base_indices.push(config.initial_index),
            _ => (),
        };

        let mut read_segments = Vec::<Segment<S, M, H, Idx, S::Size, SERP>>::new();

        for segment_base_index in segment_base_indices {
            read_segments.push(
                Segment::with_segment_storage_provider_config_and_base_index(
                    &mut segment_storage_provider,
                    config.segment_config,
                    segment_base_index,
                )
                .await
                .map_err(SegmentedLogError::SegmentError)?,
            );
        }

        let write_segment = read_segments
            .pop()
            .ok_or(SegmentedLogError::NoSegmentsCreated)?;

        Ok(Self {
            write_segment: Some(write_segment),
            read_segments,
            config,
            segment_storage_provider,
        })
    }
}

macro_rules! new_segment {
    ($segmented_log:ident, $base_index:ident) => {
        Segment::with_segment_storage_provider_config_and_base_index(
            &mut $segmented_log.segment_storage_provider,
            $segmented_log.config.segment_config,
            $base_index,
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
impl<S, M, H, Idx, SERP, SSP> AsyncIndexedRead for SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    SERP: SerializationProvider,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    Idx: Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned,
{
    type ReadError = LogError<S, SERP>;

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

        self.read_from_segment_raw(
            self.read_segment_with_idx_position_in_read_segments_vec(idx),
            idx,
        )
        .await
    }
}

impl<S, M, H, Idx, SERP, SSP> SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    SERP: SerializationProvider,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    Idx: Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned,
{
    fn read_segment_with_idx_position_in_read_segments_vec(&self, idx: &Idx) -> Option<usize> {
        self.has_index(idx).then_some(())?;

        self.read_segments
            .binary_search_by(|segment| match idx {
                idx if &segment.lowest_index() > idx => Ordering::Greater,
                idx if &segment.highest_index() <= idx => Ordering::Less,
                _ => Ordering::Equal,
            })
            .ok()
    }

    async fn read_from_segment_raw(
        &self,
        read_segment_position_in_read_segments_vec: Option<usize>,
        idx: &Idx,
    ) -> Result<Record<M, Idx, S::Content>, LogError<S, SERP>> {
        let segment = match (read_segment_position_in_read_segments_vec, idx) {
            (_, idx) if !self.has_index(idx) => return Err(SegmentedLogError::IndexOutOfBounds),
            (Some(position), _) => self
                .read_segments
                .get(position)
                .ok_or(SegmentedLogError::IndexGapEncountered)?,
            (None, _) => write_segment_ref!(self, as_ref)?,
        };

        segment
            .read(idx)
            .await
            .map_err(SegmentedLogError::SegmentError)
    }

    pub async fn read_from_segment(
        &self,
        segment_pos_in_log: usize,
        idx: &Idx,
    ) -> Result<(Record<M, Idx, S::Content>, Idx), Option<(usize, Idx)>> {
        let position_in_read_segments_vec = match segment_pos_in_log {
            _ if segment_pos_in_log < self.read_segments.len() => Ok(Some(segment_pos_in_log)),
            _ if segment_pos_in_log == self.read_segments.len() => Ok(None),
            _ => Err(None),
        }?;

        self.read_from_segment_raw(position_in_read_segments_vec, idx)
            .await
            .map(|record| (record, *idx + Idx::one()))
            .map_err(|_| Some((segment_pos_in_log + 1, *idx)))
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
            self.read_segment_with_idx_position_in_read_segments_vec(&lo),
            self.read_segment_with_idx_position_in_read_segments_vec(&hi),
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

impl<S, M, H, Idx, SERP, SSP> SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP>
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
{
    pub async fn rotate_new_write_segment(&mut self) -> Result<(), LogError<S, SERP>> {
        self.reopen_write_segment().await?;

        let write_segment = take_write_segment!(self)?;
        let next_index = write_segment.highest_index();

        self.read_segments.push(write_segment);
        self.write_segment = Some(new_segment!(self, next_index)?);

        Ok(())
    }

    pub async fn reopen_write_segment(&mut self) -> Result<(), LogError<S, SERP>> {
        let write_segment = take_write_segment!(self)?;
        let write_segment_base_index = write_segment.lowest_index();

        consume_segment!(write_segment, close)?;

        self.write_segment = Some(new_segment!(self, write_segment_base_index)?);

        Ok(())
    }

    pub async fn remove_expired_segments(
        &mut self,
        expiry_duration: Duration,
    ) -> Result<Idx, LogError<S, SERP>> {
        if write_segment_ref!(self, as_ref)?.is_empty() {
            self.reopen_write_segment().await?
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
            new_segment!(self, next_index)?
        };

        self.read_segments = to_keep;
        self.write_segment = Some(write_segment);

        let mut num_records_removed = <Idx as num::Zero>::zero();
        for segment in to_remove.drain(..) {
            num_records_removed = num_records_removed + segment.len();
            consume_segment!(segment, remove)?;
        }
        Ok(num_records_removed)
    }
}

impl<S, M, H, Idx, SERP, SSP> SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP>
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
{
    pub async fn append_record_with_contiguous_bytes<X>(
        &mut self,
        record: &Record<M, Idx, X>,
    ) -> Result<Idx, LogError<S, SERP>>
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
impl<S, M, H, Idx, SERP, SSP> AsyncTruncate for SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP>
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
{
    type TruncError = LogError<S, SERP>;

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
            .read_segment_with_idx_position_in_read_segments_vec(idx)
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

        for segment in segments_to_remove.drain(..) {
            consume_segment!(segment, remove)?;
        }

        self.write_segment = Some(new_segment!(self, next_index)?);

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
impl<S, M, H, Idx, SERP, SSP> AsyncConsume for SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP>
where
    S: Storage,
    SERP: SerializationProvider,
{
    type ConsumeError = LogError<S, SERP>;

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
impl<S, M, H, Idx, SERP, SSP, XBuf, X, XE> CommitLog<MetaWithIdx<M, Idx>, X, S::Content>
    for SegmentedLog<S, M, H, Idx, S::Size, SERP, SSP>
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
    XBuf: Deref<Target = [u8]>,
    X: Stream<Item = Result<XBuf, XE>> + Unpin,
{
    type Error = LogError<S, SERP>;

    async fn remove_expired(
        &mut self,
        expiry_duration: std::time::Duration,
    ) -> Result<Self::Idx, Self::Error> {
        self.remove_expired_segments(expiry_duration).await
    }

    async fn append(&mut self, record: Record<M, Idx, X>) -> Result<Self::Idx, Self::Error>
    where
        X: 'async_trait,
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
        };

        let mut segmented_log = SegmentedLog::<S, M, H, Idx, S::Size, SERP, SSP>::new(
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

        let mut segmented_log = SegmentedLog::<S, M, H, Idx, S::Size, SERP, SSP>::new(
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
                    assert_eq!(record.value.deref(), expected_record_value.deref());
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
                    assert_eq!(record_x.value.deref(), expected_record_value.deref());
                    assert_eq!(record_y.value.deref(), expected_record_value.deref());
                    Some(())
                })
                .count()
                .await;

            assert_eq!(record_count, expected_record_count);
        };

        {
            let (mut segment_pos_in_log, mut idx) = (0_usize, segmented_log.lowest_index());

            let mut expected_records =
                _test_records_provider(&_RECORDS, NUM_SEGMENTS, _RECORDS.len());

            loop {
                let record = segmented_log
                    .read_from_segment(segment_pos_in_log, &idx)
                    .await;

                (segment_pos_in_log, idx) = match record {
                    Ok((record, next_idx)) => {
                        assert_eq!(Some(record.value.deref()), expected_records.next());
                        (segment_pos_in_log, next_idx)
                    }
                    Err(Some((next_segment_pos, idx))) => (next_segment_pos, idx),
                    Err(None) => break,
                };
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

        let len_before_close = segmented_log.len();

        segmented_log.close().await.unwrap();

        let segmented_log = SegmentedLog::<S, M, H, Idx, S::Size, SERP, SSP>::new(
            config,
            _segment_storage_provider.clone(),
        )
        .await
        .unwrap();

        let len_after_close = segmented_log.len();

        assert_eq!(len_before_close, len_after_close);

        segmented_log.remove().await.unwrap();

        let segmented_log = SegmentedLog::<S, M, H, Idx, S::Size, SERP, SSP>::new(
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
        };

        let mut segmented_log = SegmentedLog::<S, M, H, Idx, S::Size, SERP, SSP>::new(
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

        segmented_log
            .remove_expired_segments(expiry_duration)
            .await
            .unwrap();

        assert!(
            segmented_log_highest_index_before_sleep <= segmented_log.lowest_index(),
            "Expired segments not removed."
        );

        _make_sleep_future(expiry_duration).await;

        segmented_log
            .remove_expired_segments(expiry_duration)
            .await
            .unwrap();

        assert!(
            segmented_log.is_empty(),
            "Segmented log not cleared after all segments expired."
        );

        segmented_log.remove().await.unwrap();
    }
}
