use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Configuration pertaining to segment storage and buffer sizes.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Copy)]
pub struct Config {
    /// Segment store's write buffer size. The write buffer is flushed to the disk when it is
    /// filled. Reads of records in a write buffer are only possible when the write buffer is
    /// flushed.
    pub store_buffer_size: Option<usize>,

    /// Maximum segment storage size, after which this segment is demoted from the current
    /// write segment to a read segment.
    pub max_store_bytes: usize,
}

pub struct Segment<Idx, I, S> {
    _index: I,
    _store: S,

    _base_index: Idx,
    _next_index: Idx,

    _created_at: Instant,

    _config: Config,
}

#[derive(Debug)]
pub enum SegmentError<StoreError, IndexError> {
    StoreError(StoreError),

    IndexError(IndexError),

    SegmentMaxed,

    /// Error when ser/deser-ializing a record.
    SerializationError,

    /// Offset used for reading or writing to a segment is beyond that store's allowed
    /// capacity by the [`config::SegmentConfig::max_store_bytes`] limit.
    OffsetBeyondCapacity,

    /// Offset is out of bounds of the written region in this segment.
    OffsetOutOfBounds,
}

impl<StoreError, IndexError> std::fmt::Display for SegmentError<StoreError, IndexError>
where
    StoreError: std::error::Error,
    IndexError: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self)
    }
}

impl<StoreError, IndexError> std::error::Error for SegmentError<StoreError, IndexError>
where
    StoreError: std::error::Error,
    IndexError: std::error::Error,
{
}
