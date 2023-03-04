pub mod index;
pub mod segment;
pub mod store;

use num::FromPrimitive;
use serde::{Deserialize, Serialize};

use self::segment::Segment;

use super::super::super::{common::serde::SerDe, storage::Storage};

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
}

impl<SE, SDE> std::fmt::Display for SegmentedLogError<SE, SDE>
where
    SE: std::error::Error,
    SDE: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<SE, SDE> std::error::Error for SegmentedLogError<SE, SDE>
where
    SE: std::error::Error,
    SDE: std::error::Error,
{
}

pub struct Config<Idx, Size> {
    pub segment_config: segment::Config<Size>,
    pub initial_index: Idx,
}

pub struct SegmentedLog<S, M, H, Idx, Size, SD, SSP> {
    _write_segment: Option<segment::Segment<S, M, H, Idx, Size, SD>>,
    _read_segments: Vec<segment::Segment<S, M, H, Idx, Size, SD>>,

    _config: Config<Idx, Size>,

    _segment_storage_provider: SSP,
}

pub type LogError<S, SD> = SegmentedLogError<<S as Storage>::Error, <SD as SerDe>::Error>;

impl<S, M, H, Idx, SD, SSP> SegmentedLog<S, M, H, Idx, S::Size, SD, SSP>
where
    S: Storage,
    S::Size: Copy,
    H: Default,
    Idx: FromPrimitive + Copy + Ord,
    SD: SerDe,
    SSP: segment::SegmentStorageProvider<S, Idx>,
{
    pub async fn new(
        config: Config<Idx, S::Size>,
        segment_storage_provider: SSP,
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
                    &segment_storage_provider,
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
