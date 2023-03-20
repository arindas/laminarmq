pub mod index;
pub mod segment;
pub mod store;

use self::segment::{Segment, SegmentStorageProvider};
use super::{
    super::super::{
        common::{serde::SerDe, split::SplitAt},
        storage::{AsyncConsume, AsyncIndexedRead, AsyncTruncate, Sizable, Storage},
    },
    CommitLog,
};
use async_trait::async_trait;
use futures_core::Stream;
use num::{CheckedSub, FromPrimitive, ToPrimitive, Unsigned};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{hash::Hasher, ops::Deref, time::Duration};

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

pub struct SegmentedLog<S, M, H, Idx, Size, SD, SSP> {
    _write_segment: Option<Segment<S, M, H, Idx, Size, SD>>,
    _read_segments: Vec<Segment<S, M, H, Idx, Size, SD>>,

    _config: Config<Idx, Size>,

    _segment_storage_provider: SSP,
}

pub type LogError<S, SD> = SegmentedLogError<<S as Storage>::Error, <SD as SerDe>::Error>;

impl<S, M, H, Idx, SD, SSP> Sizable for SegmentedLog<S, M, H, Idx, S::Size, SD, SSP>
where
    S: Storage,
{
    type Size = S::Size;

    fn size(&self) -> Self::Size {
        self._read_segments
            .iter()
            .chain(self._write_segment.iter())
            .map(|x| x.size())
            .sum()
    }
}

impl<S, M, H, Idx, SD, SSP> SegmentedLog<S, M, H, Idx, S::Size, SD, SSP>
where
    S: Storage,
    S::Size: Copy,
    H: Default,
    Idx: FromPrimitive + Copy + Ord,
    SD: SerDe,
    SSP: SegmentStorageProvider<S, Idx>,
{
    pub async fn new(
        config: Config<Idx, S::Size>,
        mut segment_storage_provider: SSP,
    ) -> Result<Self, LogError<S, SD>> {
        let mut segment_base_indices = segment_storage_provider
            .base_indices_of_stored_segments()
            .await
            .map_err(SegmentedLogError::StorageError)?;

        match segment_base_indices.first() {
            Some(base_index) if base_index < &config.initial_index => {
                return Err(SegmentedLogError::BaseIndexLesserThanInitialIndex);
            }
            None => segment_base_indices.push(config.initial_index),
            _ => (),
        };

        let mut read_segments = Vec::<Segment<S, M, H, Idx, S::Size, SD>>::new();

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
            _write_segment: Some(write_segment),
            _read_segments: read_segments,
            _config: config,
            _segment_storage_provider: segment_storage_provider,
        })
    }
}

macro_rules! new_segment {
    ($segmented_log:ident, $base_index:ident) => {
        Segment::with_segment_storage_provider_config_and_base_index(
            &mut $segmented_log._segment_storage_provider,
            $segmented_log._config.segment_config,
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
            ._write_segment
            .take()
            .ok_or(SegmentedLogError::WriteSegmentLost)
    };
}

macro_rules! write_segment_ref {
    ($segmented_log:ident, $ref_method:ident) => {
        $segmented_log
            ._write_segment
            .$ref_method()
            .ok_or(SegmentedLogError::WriteSegmentLost)
    };
}

#[async_trait(?Send)]
impl<S, M, H, Idx, SD, SSP> AsyncIndexedRead for SegmentedLog<S, M, H, Idx, S::Size, SD, SSP>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    SD: SerDe,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + ToPrimitive + Ord + Copy,
    Idx: Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned,
{
    type ReadError = LogError<S, SD>;

    type Idx = Idx;

    type Value = Record<M, Idx, S::Content>;

    fn highest_index(&self) -> Self::Idx {
        self._write_segment
            .as_ref()
            .map(|segment| segment.highest_index())
            .unwrap_or(self._config.initial_index)
    }

    fn lowest_index(&self) -> Self::Idx {
        self._read_segments
            .first()
            .map(|segment| segment.lowest_index())
            .unwrap_or(self._config.initial_index)
    }

    async fn read(&self, idx: &Self::Idx) -> Result<Self::Value, Self::ReadError> {
        if !self.has_index(idx) {
            return Err(SegmentedLogError::IndexOutOfBounds);
        }

        self._read_segments
            .iter()
            .chain(self._write_segment.iter())
            .find(|segment| segment.has_index(idx))
            .ok_or(SegmentedLogError::IndexGapEncountered)?
            .read(idx)
            .await
            .map_err(SegmentedLogError::SegmentError)
    }
}

impl<S, M, H, Idx, SD, SSP> SegmentedLog<S, M, H, Idx, S::Size, SD, SSP>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    S::Size: Copy,
    H: Hasher + Default,
    Idx: FromPrimitive + ToPrimitive + Unsigned + CheckedSub,
    Idx: Copy + Ord + Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned,
    SD: SerDe,
    SSP: SegmentStorageProvider<S, Idx>,
{
    pub async fn rotate_new_write_segment(&mut self) -> Result<(), LogError<S, SD>> {
        self.reopen_write_segment().await?;

        let write_segment = take_write_segment!(self)?;
        let next_index = write_segment.highest_index();

        self._read_segments.push(write_segment);
        self._write_segment = Some(new_segment!(self, next_index)?);

        Ok(())
    }

    pub async fn reopen_write_segment(&mut self) -> Result<(), LogError<S, SD>> {
        let write_segment = take_write_segment!(self)?;
        let write_segment_base_index = write_segment.lowest_index();

        consume_segment!(write_segment, close)?;

        self._write_segment = Some(new_segment!(self, write_segment_base_index)?);

        Ok(())
    }

    pub async fn remove_expired_segments(
        &mut self,
        expiry_duration: Duration,
    ) -> Result<Idx, LogError<S, SD>> {
        let next_index = self.highest_index();

        let mut segments = std::mem::take(&mut self._read_segments);
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

        self._read_segments = to_keep;
        self._write_segment = Some(write_segment);

        let mut num_records_removed = <Idx as num::Zero>::zero();
        for segment in to_remove.drain(..) {
            num_records_removed = num_records_removed + segment.len();
            consume_segment!(segment, remove)?;
        }
        Ok(num_records_removed)
    }
}

impl<S, M, H, Idx, SD, SSP> SegmentedLog<S, M, H, Idx, S::Size, SD, SSP>
where
    S: Storage,
    S::Position: ToPrimitive,
    S::Content: SplitAt<u8>,
    S::Size: Copy,
    H: Hasher + Default,
    Idx: FromPrimitive + ToPrimitive + Unsigned + CheckedSub,
    Idx: Copy + Ord + Serialize + DeserializeOwned,
    M: Clone + Serialize + DeserializeOwned,
    SD: SerDe,
    SSP: SegmentStorageProvider<S, Idx>,
{
    pub async fn append_record_with_contiguous_bytes<X>(
        &mut self,
        record: &Record<M, Idx, X>,
    ) -> Result<Idx, LogError<S, SD>>
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
impl<S, M, H, Idx, SD, SSP> AsyncTruncate for SegmentedLog<S, M, H, Idx, S::Size, SD, SSP>
where
    S: Storage,
    S::Content: SplitAt<u8>,
    S::Size: Copy,
    SD: SerDe,
    H: Hasher + Default,
    Idx: Unsigned + CheckedSub + FromPrimitive + ToPrimitive + Ord + Copy,
    Idx: Serialize + DeserializeOwned,
    M: Default + Serialize + DeserializeOwned,
    SSP: SegmentStorageProvider<S, Idx>,
{
    type TruncError = LogError<S, SD>;

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
            ._read_segments
            .iter()
            .position(|seg| seg.has_index(idx))
            .ok_or(SegmentedLogError::IndexGapEncountered)?;

        let segment_to_truncate = self
            ._read_segments
            .get_mut(segment_pos_in_vec)
            .ok_or(SegmentedLogError::IndexGapEncountered)?;

        segment_to_truncate
            .truncate(idx)
            .await
            .map_err(SegmentedLogError::SegmentError)?;

        let next_index = segment_to_truncate.highest_index();

        let mut segments_to_remove = self._read_segments.split_off(segment_pos_in_vec + 1);
        segments_to_remove.push(take_write_segment!(self)?);

        for segment in segments_to_remove.drain(..) {
            consume_segment!(segment, remove)?;
        }

        self._write_segment = Some(new_segment!(self, next_index)?);

        Ok(())
    }
}

macro_rules! consume_segmented_log {
    ($segmented_log:ident, $consume_method:ident) => {
        let segments = &mut $segmented_log._read_segments;
        segments.push(take_write_segment!($segmented_log)?);
        for segment in segments.drain(..) {
            consume_segment!(segment, $consume_method)?;
        }
    };
}

#[async_trait(?Send)]
impl<S, M, H, Idx, SD, SSP> AsyncConsume for SegmentedLog<S, M, H, Idx, S::Size, SD, SSP>
where
    S: Storage,
    SD: SerDe,
{
    type ConsumeError = LogError<S, SD>;

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
impl<S, M, H, Idx, SD, SSP, XBuf, X, XE> CommitLog<MetaWithIdx<M, Idx>, X, S::Content>
    for SegmentedLog<S, M, H, Idx, S::Size, SD, SSP>
where
    S: Storage,
    S::Position: ToPrimitive,
    S::Content: SplitAt<u8>,
    S::Size: Copy,
    H: Hasher + Default,
    Idx: FromPrimitive + ToPrimitive + Unsigned + CheckedSub,
    Idx: Copy + Ord + Serialize + DeserializeOwned,
    M: Default + Serialize + DeserializeOwned,
    SD: SerDe,
    SSP: SegmentStorageProvider<S, Idx>,
    XBuf: Deref<Target = [u8]>,
    X: Stream<Item = Result<XBuf, XE>> + Unpin,
{
    type Error = LogError<S, SD>;

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

    use num::Zero;
    use std::{fmt::Debug, marker::PhantomData};

    async fn _test_segmented_log_read_append_truncate_consistency<S, M, H, Idx, SD, SSP>(
        _segment_storage_provider: SSP,
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
        SSP: SegmentStorageProvider<S, Idx> + Clone,
    {
        // arbitrary initial index for testing purposes
        let initial_index = Idx::from_usize(42).unwrap();

        const NUM_SEGMENTS: usize = 10;

        let config = Config {
            segment_config: _segment_config::<M, Idx, S::Size, SD>(
                _RECORDS[0].len(),
                _RECORDS.len(),
            )
            .unwrap(),
            initial_index,
        };

        let mut segmented_log = SegmentedLog::<S, M, H, Idx, S::Size, SD, SSP>::new(
            config,
            _segment_storage_provider.clone(),
        )
        .await
        .unwrap();

        let records = |num_segments: usize| {
            _RECORDS
                .iter()
                .cycle()
                .take(_RECORDS.len() * num_segments)
                .cloned()
        };

        let x = records(NUM_SEGMENTS);

        segmented_log.close().await.unwrap();

        let segmented_log = SegmentedLog::<S, M, H, Idx, S::Size, SD, SSP>::new(
            config,
            _segment_storage_provider.clone(),
        )
        .await
        .unwrap();

        segmented_log.remove().await.unwrap();

        let segmented_log = SegmentedLog::<S, M, H, Idx, S::Size, SD, SSP>::new(
            config,
            _segment_storage_provider.clone(),
        )
        .await
        .unwrap();

        segmented_log.remove().await.unwrap();
    }
}
