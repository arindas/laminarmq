pub mod index;
pub mod segment;
pub mod store;

use serde::{Deserialize, Serialize};

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
pub enum SegmentedLogError<StorageError, SerDeEror, BuilderError> {
    SegmentError(segment::SegmentError<StorageError, SerDeEror>),
    SegmentBuilderError(BuilderError),
    DummyError,
}

impl<StorageError, SerDeError, BuilderError> std::fmt::Display
    for SegmentedLogError<StorageError, SerDeError, BuilderError>
where
    StorageError: std::error::Error,
    SerDeError: std::error::Error,
    BuilderError: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<StorageError, SerDeError, BuilderError> std::error::Error
    for SegmentedLogError<StorageError, SerDeError, BuilderError>
where
    StorageError: std::error::Error,
    SerDeError: std::error::Error,
    BuilderError: std::error::Error,
{
}

pub struct Config<Idx, Size> {
    pub segment_config: segment::Config<Size>,
    pub initial_index: Idx,
}

pub struct SegmentedLog<S, M, H, Idx, Size, SD, SB> {
    _write_segment: Option<segment::Segment<S, M, H, Idx, Size, SD>>,
    _read_segments: Vec<segment::Segment<S, M, H, Idx, Size, SD>>,

    _config: Config<Idx, Size>,

    _segment_builder: SB,
}

pub type SegmentedLogOpError<S, SD, SB> = SegmentedLogError<
    <S as Storage>::Error,
    <SD as SerDe>::Error,
    <SB as segment::SegmentBuilder>::Error,
>;

impl<S, M, H, Idx, SD, SB> SegmentedLog<S, M, H, Idx, S::Size, SD, SB>
where
    S: Storage,
    SD: SerDe,
    SB: segment::SegmentBuilder<
        Idx = Idx,
        Config = segment::Config<S::Size>,
        Segment = segment::Segment<S, M, H, Idx, S::Size, SD>,
    >,
{
    pub async fn new(
        config: Config<Idx, S::Size>,
        segment_builder: SB,
    ) -> Result<Self, SegmentedLogOpError<S, SD, SB>> {
        let write_segment = segment_builder
            .build(&config.initial_index, &config.segment_config)
            .await
            .map_err(SegmentedLogError::SegmentBuilderError)?;

        Ok(Self {
            _write_segment: Some(write_segment),
            _read_segments: Vec::new(),
            _config: config,
            _segment_builder: segment_builder,
        })
    }
}
