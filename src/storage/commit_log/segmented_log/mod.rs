pub mod index;
pub mod segment;
pub mod store;

use segment::{Config as SegmentConfig, Segment};
use serde::{Deserialize, Serialize};

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
    _write_segment: Option<Segment<M, X, I, S>>,
    _read_segments: Vec<Segment<M, X, I, S>>,

    _config: Config<Idx>,

    _segment_creator: SegC,
}
