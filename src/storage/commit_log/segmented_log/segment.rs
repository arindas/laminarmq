use super::store::Store;
use super::Record;
use super::{super::super::*, index::Index};
use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, time::Instant};

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

pub struct Segment<M, X, I, S> {
    _index: I,
    _store: S,

    _config: Config,

    _created_at: Instant,

    _phantom_data: PhantomData<(M, X)>,
}

impl<M, X, I, S> Segment<M, X, I, S> {
    pub fn new(_index: I, _store: S, _config: Config) -> Self {
        Self {
            _index,
            _store,
            _created_at: Instant::now(),
            _config,
            _phantom_data: PhantomData,
        }
    }
}

#[derive(Debug)]
pub enum SegmentError<StoreError, IndexError> {
    StoreError(StoreError),

    IndexError(IndexError),

    SegmentMaxed,

    /// Error when ser/deser-ializing a record.
    SerializationError,
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

#[async_trait::async_trait(?Send)]
impl<M, X, I, S> AsyncIndexedRead for Segment<M, X, I, S>
where
    I: Index,
    S: Store,
{
    type ReadError = SegmentError<S::Error, I::Error>;

    type Idx = I::Idx;

    type Value = Record<M, Self::Idx, S::Content>;

    fn highest_index(&self) -> &Self::Idx {
        self._index.highest_index()
    }

    fn lowest_index(&self) -> &Self::Idx {
        self._index.lowest_index()
    }

    async fn read(&self, _idx: &Self::Idx) -> Result<Self::Value, Self::ReadError> {
        Err(SegmentError::SerializationError)
    }
}
