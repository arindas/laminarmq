//! Module providing single-node specific RPC request and response types. Each request
//! has a corresponding response type and vice-versa.
use crate::{
    commit_log::{segmented_log::RecordMetadata, Record},
    server::partition::PartitionId,
};
use std::{borrow::Cow, collections::HashMap, ops::Deref, time::Duration};

/// Single node request schema.
///
/// ## Generic parameters
/// `T`: container for request data bytes
#[derive(Debug)]
pub enum Request<T: Deref<Target = [u8]>> {
    /// Requests a map containing a mapping from topic ids
    /// to lists of partition numbers under them.
    PartitionHierachy,

    /// Remove expired records in the given partition.
    RemoveExpired {
        partition: PartitionId,
        expiry_duration: Duration,
    },

    CreatePartition(PartitionId),
    RemovePartition(PartitionId),

    Read {
        partition: PartitionId,
        offset: u64,
    },
    Append {
        partition: PartitionId,
        record_bytes: T,
    },

    LowestOffset {
        partition: PartitionId,
    },
    HighestOffset {
        partition: PartitionId,
    },
}

/// Single node response schema.
///
/// ## Generic parameters
/// `T`: container for response data bytes
pub enum Response<T: Deref<Target = [u8]>> {
    /// Response containing a mapping from topic ids to lists
    /// of partition numbers under them.
    PartitionHierachy(HashMap<Cow<'static, str>, Vec<u64>>),

    Read {
        record: Record<RecordMetadata<()>, T>,
        next_offset: u64,
    },
    LowestOffset(u64),
    HighestOffset(u64),

    Append {
        write_offset: u64,
        bytes_written: usize,
    },

    /// Response for [`Request::RemoveExpired`]
    ExpiredRemoved,
    PartitionCreated,
    PartitionRemoved,
}

/// Kinds of single node RPC requests.
#[derive(Clone, Copy)]
#[repr(u8)]
pub enum RequestKind {
    Read = 1,
    Append = 2,

    LowestOffset = 3,
    HighestOffset = 4,

    /// Remove expired records in partition.
    RemoveExpired = 5,

    /// Requests a map containing a mapping from topic ids
    /// to lists of partition numbers under them.
    PartitionHierachy = 6,
    CreatePartition = 7,
    RemovePartition = 8,
}

impl TryFrom<u8> for RequestKind {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Read),
            2 => Ok(Self::Append),
            3 => Ok(Self::LowestOffset),
            4 => Ok(Self::HighestOffset),
            5 => Ok(Self::RemoveExpired),
            6 => Ok(Self::PartitionHierachy),
            7 => Ok(Self::CreatePartition),
            8 => Ok(Self::RemovePartition),
            _ => Err(()),
        }
    }
}

pub mod response_io;
