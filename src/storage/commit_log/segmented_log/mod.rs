pub mod index;
pub mod segment;
pub mod store;

use self::{
    index::Index,
    segment::{Config as SegmentConfig, Segment, SegmentError},
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

    _segment_creator: SegC,
}

#[derive(Debug)]
pub enum SegmentedLogError<SegmentError> {
    SegmentError(SegmentError),

    IndexOutOfBounds,
}

impl<SegmentError> std::fmt::Display for SegmentedLogError<SegmentError>
where
    SegmentError: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self)
    }
}

impl<SegmentError: std::error::Error> std::error::Error for SegmentedLogError<SegmentError> {}

#[async_trait(?Send)]
impl<M, X, I, S, SegC> AsyncIndexedRead for SegmentedLog<M, X, I, S, I::Idx, SegC>
where
    S: Store,
    I: Index<Position = S::Position>,
    I::Idx: Serialize + DeserializeOwned,
    M: Default + Serialize + DeserializeOwned,
{
    type ReadError = SegmentedLogError<SegmentError<S::Error, I::Error>>;

    type Idx = I::Idx;

    type Value = Record<M, Self::Idx, S::Content>;

    fn highest_index(&self) -> &Self::Idx {
        self.write_segment
            .as_ref()
            .map(|segment| segment.highest_index())
            .unwrap_or(&self.config.initial_index)
    }

    fn lowest_index(&self) -> &Self::Idx {
        self.read_segments
            .first()
            .map(|segment| segment.lowest_index())
            .unwrap_or(&self.config.initial_index)
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
