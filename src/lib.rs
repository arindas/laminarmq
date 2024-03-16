#![doc = include_str!("../README.md")]

pub mod common;
pub mod server;
pub mod storage;

pub mod prelude {
    //! Prelude module for [`laminarmq`](super) with common exports for convenience.

    pub use crate::storage::{
        commit_log::{
            segmented_log::{index::Index, segment::Segment, SegmentedLog},
            CommitLog,
        },
        AsyncConsume, AsyncIndexedRead, AsyncTruncate, Storage,
    };
}
