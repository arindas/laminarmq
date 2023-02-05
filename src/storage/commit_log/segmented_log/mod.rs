pub mod index;
pub mod segment;
pub mod store;

use self::{
    index::Index,
    segment::{Config as SegmentConfig, SegError, Segment, SegmentCreator, SegmentError},
    store::Store,
};
use super::super::*;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub type Record<M, Idx, T> = super::Record<MetaWithIdx<M, Idx>, T>;

#[derive(Debug, Serialize, Deserialize)]
pub struct MetaWithIdx<M, Idx> {
    pub metadata: M,
    pub index: Idx,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, Copy)]
pub struct Config<Idx> {
    /// Config for every segment in this Log.
    pub segment_config: SegmentConfig,

    /// Index from which the first index of the Log starts.
    pub initial_index: Idx,
}

pub struct SegmentedLog<M, X, I, S, Idx, SegC> {
    write_segment: Option<Segment<M, X, I, S>>,
    read_segments: Vec<Segment<M, X, I, S>>,

    config: Config<Idx>,

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
impl<M, X, I, S, SegC> AsyncIndexedRead for SegmentedLog<M, X, I, S, I::Idx, SegC>
where
    S: Store,
    I: Index<Position = S::Position>,
    I::Idx: Serialize + DeserializeOwned,
    M: Default + Serialize + DeserializeOwned,
    SegC: SegmentCreator,
{
    type ReadError = SegmentedLogError<SegmentError<S::Error, I::Error>, SegC::Error>;

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

impl<M, X, I, S, SegC> SegmentedLog<M, X, I, S, I::Idx, SegC>
where
    S: Store,
    I: Index<Position = S::Position>,
    I::Idx: Serialize + DeserializeOwned,
    M: Default + Serialize + DeserializeOwned,
    SegC: SegmentCreator<Segment = Segment<M, X, I, S>, Idx = I::Idx>,
{
    pub async fn rotate_new_write_segment(&mut self) -> Result<(), SegLogError<S, I, SegC>> {
        self.reopen_write_segment().await?;

        let write_segment = self
            .write_segment
            .take()
            .ok_or(SegmentedLogError::WriteSegmentLost)?;

        let next_index = write_segment.highest_index();

        self.read_segments.push(write_segment);

        self.write_segment = Some(
            self.segment_creator
                .create(&next_index, &self.config.segment_config)
                .await
                .map_err(SegmentedLogError::SegmentCreationError)?,
        );

        Ok(())
    }

    pub async fn reopen_write_segment(&mut self) -> Result<(), SegLogError<S, I, SegC>> {
        let write_segment = self
            .write_segment
            .take()
            .ok_or(SegmentedLogError::WriteSegmentLost)?;

        let write_segment_base_index = write_segment.lowest_index();

        write_segment
            .close()
            .await
            .map_err(SegmentedLogError::SegmentError)?;

        self.write_segment = Some(
            self.segment_creator
                .create(&write_segment_base_index, &self.config.segment_config)
                .await
                .map_err(SegmentedLogError::SegmentCreationError)?,
        );

        Ok(())
    }
}

#[async_trait(?Send)]
impl<M, X, I, S, SegC> AsyncTruncate for SegmentedLog<M, X, I, S, I::Idx, SegC>
where
    S: Store,
    I: Index<Position = S::Position>,
    I::Idx: Serialize + DeserializeOwned,
    M: Default + Serialize + DeserializeOwned,
    SegC: SegmentCreator<Segment = Segment<M, X, I, S>, Idx = I::Idx>,
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
        segments_to_remove.push(
            self.write_segment
                .take()
                .ok_or(SegmentedLogError::WriteSegmentLost)?,
        );

        for segment in segments_to_remove.drain(..) {
            segment
                .remove()
                .await
                .map_err(SegmentedLogError::SegmentError)?;
        }

        self.write_segment = Some(
            self.segment_creator
                .create(&next_index, &self.config.segment_config)
                .await
                .map_err(SegmentedLogError::SegmentCreationError)?,
        );

        Ok(())
    }
}
