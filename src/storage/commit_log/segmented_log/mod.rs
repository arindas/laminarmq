pub mod index;
pub mod segment;
pub mod store;

use std::time::Duration;

use self::{
    index::Index,
    segment::{Config as SegmentConfig, SegError, Segment, SegmentCreator},
    store::Store,
};
use super::{
    super::{AsyncConsume, AsyncIndexedRead, AsyncTruncate, Sizable, Stream},
    CommitLog,
};
use async_trait::async_trait;
use bytes::Buf;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub type Record<M, Idx, T> = super::Record<MetaWithIdx<M, Idx>, T>;

#[derive(Debug, Serialize, Deserialize)]
pub struct MetaWithIdx<M, Idx> {
    pub metadata: M,
    pub index: Idx,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, Copy)]
pub struct Config<Idx, Size> {
    /// Config for every segment in this Log.
    pub segment_config: SegmentConfig<Size>,

    /// Index from which the first index of the Log starts.
    pub initial_index: Idx,
}

pub struct SegmentedLog<M, X, I, S, Idx, Size, SegC> {
    write_segment: Option<Segment<M, X, I, S, Size>>,
    read_segments: Vec<Segment<M, X, I, S, Size>>,

    config: Config<Idx, Size>,

    segment_creator: SegC,
}

#[derive(Debug)]
pub enum SegmentedLogError<SegError, CreateError> {
    SegmentError(SegError),

    SegmentCreationError(CreateError),

    WriteSegmentLost,

    IndexOutOfBounds,

    IndexGapEncountered,
}

impl<SegError, CreateError> std::fmt::Display for SegmentedLogError<SegError, CreateError>
where
    SegError: std::error::Error,
    CreateError: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self)
    }
}

impl<SegError, CreateError> std::error::Error for SegmentedLogError<SegError, CreateError>
where
    SegError: std::error::Error,
    CreateError: std::error::Error,
{
}

#[doc(hidden)]
macro_rules! new_segment {
    ($segmented_log:ident, $idx:ident) => {
        $segmented_log
            .segment_creator
            .create(&$idx, &$segmented_log.config.segment_config)
            .await
            .map_err(SegmentedLogError::SegmentCreationError)
    };
}

macro_rules! consume_segments {
    ($segment_collection:ident, $async_consume_method:ident) => {
        for segment in $segment_collection.drain(..) {
            segment
                .$async_consume_method()
                .await
                .map_err(SegmentedLogError::SegmentError)?;
        }
    };
}

#[doc(hidden)]
macro_rules! taken_write_segment {
    ($segmented_log:ident) => {
        $segmented_log
            .write_segment
            .take()
            .ok_or(SegmentedLogError::WriteSegmentLost)
    };
}

#[doc(hidden)]
macro_rules! write_segment_ref {
    ($segmented_log:ident, $ref_method:ident) => {
        $segmented_log
            .write_segment
            .$ref_method()
            .ok_or(SegmentedLogError::WriteSegmentLost)
    };
}

type SegLogError<S, I, SegC> = SegmentedLogError<SegError<S, I>, <SegC as SegmentCreator>::Error>;

#[async_trait(?Send)]
impl<M, X, I, S, SegC> AsyncIndexedRead for SegmentedLog<M, X, I, S, I::Idx, S::Size, SegC>
where
    S: Store,
    I: Index<Position = S::Position>,
    I::Idx: Serialize + DeserializeOwned,
    M: Default + Serialize + DeserializeOwned,
    SegC: SegmentCreator,
{
    type ReadError = SegmentedLogError<SegError<S, I>, SegC::Error>;

    type Idx = I::Idx;

    type Value = Record<M, Self::Idx, S::Content>;

    fn highest_index(&self) -> Self::Idx {
        self.write_segment
            .as_ref()
            .map(|segment| segment.highest_index())
            .unwrap_or(self.config.initial_index)
    }

    fn lowest_index(&self) -> Self::Idx {
        self.read_segments
            .first()
            .map(|segment| segment.lowest_index())
            .unwrap_or(self.config.initial_index)
    }

    async fn read(&self, idx: &Self::Idx) -> Result<Self::Value, Self::ReadError> {
        self.read_segments
            .iter()
            .chain(self.write_segment.iter())
            .find(|segment| segment.has_index(idx))
            .ok_or(SegmentedLogError::IndexOutOfBounds)?
            .read(idx)
            .await
            .map_err(SegmentedLogError::SegmentError)
    }
}

impl<M, X, I, S, SegC> SegmentedLog<M, X, I, S, I::Idx, S::Size, SegC>
where
    S: Store,
    I: Index<Position = S::Position>,
    I::Idx: Serialize + DeserializeOwned,
    M: Default + Serialize + DeserializeOwned,
    SegC: SegmentCreator<
        SegmentConfig = SegmentConfig<S::Size>,
        Segment = Segment<M, X, I, S, S::Size>,
        Idx = I::Idx,
    >,
{
    pub async fn rotate_new_write_segment(&mut self) -> Result<(), SegLogError<S, I, SegC>> {
        self.reopen_write_segment().await?;

        let write_segment = taken_write_segment!(self)?;
        let next_index = write_segment.highest_index();

        self.read_segments.push(write_segment);
        self.write_segment = Some(new_segment!(self, next_index)?);

        Ok(())
    }

    pub async fn reopen_write_segment(&mut self) -> Result<(), SegLogError<S, I, SegC>> {
        let write_segment = taken_write_segment!(self)?;

        let write_segment_base_index = write_segment.lowest_index();

        write_segment
            .close()
            .await
            .map_err(SegmentedLogError::SegmentError)?;

        self.write_segment = Some(new_segment!(self, write_segment_base_index)?);

        Ok(())
    }

    /// Removes expired [`Segment`]`(s)` within this [`SegmentedLog`] instance.
    ///
    /// ## Returns
    /// The number of records removed due to the removal of the segments.
    pub async fn remove_expired_segments(
        &mut self,
        expiry_duration: Duration,
    ) -> Result<I::Idx, SegLogError<S, I, SegC>> {
        let next_index = self.highest_index();

        let mut segments = std::mem::replace(&mut self.read_segments, Vec::new());
        segments.push(taken_write_segment!(self)?);

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

        let mut num_records_removed = <I::Idx as num::Zero>::zero();
        for segment in to_remove.drain(..) {
            num_records_removed = num_records_removed + segment.len();

            segment
                .remove()
                .await
                .map_err(SegmentedLogError::SegmentError)?;
        }
        Ok(num_records_removed)
    }
}

#[async_trait(?Send)]
impl<M, X, I, S, SegC> AsyncTruncate for SegmentedLog<M, X, I, S, I::Idx, S::Size, SegC>
where
    S: Store,
    I: Index<Position = S::Position>,
    I::Idx: Serialize + DeserializeOwned,
    M: Default + Serialize + DeserializeOwned,
    SegC: SegmentCreator<
        SegmentConfig = SegmentConfig<S::Size>,
        Segment = Segment<M, X, I, S, S::Size>,
        Idx = I::Idx,
    >,
{
    type TruncError = SegLogError<S, I, SegC>;

    type Mark = I::Idx;

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
            .read_segments
            .iter()
            .position(|seg| seg.has_index(idx))
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
        segments_to_remove.push(taken_write_segment!(self)?);

        consume_segments!(segments_to_remove, remove);

        self.write_segment = Some(new_segment!(self, next_index)?);

        Ok(())
    }
}

#[doc(hidden)]
macro_rules! consume_segmented_log {
    ($segmented_log:ident, $consume_method:ident) => {
        let segments = &mut $segmented_log.read_segments;
        segments.push(taken_write_segment!($segmented_log)?);
        consume_segments!(segments, $consume_method);
    };
}

#[async_trait(?Send)]
impl<M, X, I, S, SegC> AsyncConsume for SegmentedLog<M, X, I, S, I::Idx, S::Size, SegC>
where
    S: Store,
    I: Index,
    SegC: SegmentCreator,
{
    type ConsumeError = SegLogError<S, I, SegC>;

    async fn remove(mut self) -> Result<(), Self::ConsumeError> {
        consume_segmented_log!(self, remove);
        Ok(())
    }

    async fn close(mut self) -> Result<(), Self::ConsumeError> {
        consume_segmented_log!(self, close);
        Ok(())
    }
}

impl<M, X, I, S, Idx, SegC> Sizable for SegmentedLog<M, X, I, S, Idx, S::Size, SegC>
where
    S: Store,
{
    type Size = S::Size;

    fn size(&self) -> Self::Size {
        self.read_segments
            .iter()
            .chain(self.write_segment.as_ref())
            .map(|x| x.size())
            .sum()
    }
}

#[async_trait(?Send)]
impl<M, ReqBuf, X, I, S, SegC> CommitLog<MetaWithIdx<M, I::Idx>, X, S::Content>
    for SegmentedLog<M, X, I, S, I::Idx, S::Size, SegC>
where
    S: Store,
    I: Index<Position = S::Position>,
    I::Idx: Serialize + DeserializeOwned,
    M: Default + Serialize + DeserializeOwned,
    ReqBuf: Buf,
    X: Stream<Item = ReqBuf> + Unpin,
    SegC: SegmentCreator<
        SegmentConfig = SegmentConfig<S::Size>,
        Segment = Segment<M, X, I, S, S::Size>,
        Idx = I::Idx,
    >,
{
    type Error = SegLogError<S, I, SegC>;

    async fn remove_expired(&mut self, expiry_duration: Duration) -> Result<I::Idx, Self::Error> {
        self.remove_expired_segments(expiry_duration).await
    }

    async fn append(
        &mut self,
        record: &mut Record<M, I::Idx, X>,
    ) -> Result<Self::Size, Self::Error> {
        if write_segment_ref!(self, as_ref)?.is_maxed() {
            self.rotate_new_write_segment().await?;
        }

        write_segment_ref!(self, as_mut)?
            .append(record)
            .await
            .map_err(SegmentedLogError::SegmentError)
    }
}
