//! Module providing abstractions necessary for a "partition" implementation backed by a "commit_log".
use crate::commit_log::segmented_log::RecordMetadata;

use super::super::{
    super::{super::commit_log::CommitLog, single_node::Response},
    single_node::PartitionRequest,
};
use async_trait::async_trait;
use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
    ops::Deref,
};

/// Error type for [`Partition`]
pub enum PartitionError<M, T, CL: CommitLog<M, T>>
where
    M: serde::Serialize + serde::de::DeserializeOwned,
    T: Deref<Target = [u8]>,
{
    CommitLog(CL::Error),
    NotSupported,
}

impl<M, T, CL> Display for PartitionError<M, T, CL>
where
    M: serde::Serialize + serde::de::DeserializeOwned,
    T: Deref<Target = [u8]>,
    CL::Error: std::error::Error,
    CL: CommitLog<M, T>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            PartitionError::CommitLog(error) => write!(f, "CommitLog error: {:?}", error),
            PartitionError::NotSupported => write!(f, "Operation not supported."),
        }
    }
}

impl<M, T, CL> Debug for PartitionError<M, T, CL>
where
    M: serde::Serialize + serde::de::DeserializeOwned,
    T: Deref<Target = [u8]>,
    CL: CommitLog<M, T>,
    CL::Error: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CommitLog(error) => f.debug_tuple("CommitLog").field(error).finish(),
            Self::NotSupported => write!(f, "NotSupported"),
        }
    }
}

impl<M, T, CL> std::error::Error for PartitionError<M, T, CL>
where
    M: serde::Serialize + serde::de::DeserializeOwned,
    T: Deref<Target = [u8]>,
    CL: CommitLog<M, T>,
    CL::Error: std::error::Error,
{
}

/// [`Partition`] backed by a [`CommitLog`] instance for storage.
///
/// ## Generic parameters:
/// - `M`: [`crate::commit_log::Record`] metadata
/// - `X`: partition request bytes container
/// - `T`: [`crate::commit_log::Record`] value and partition response bytes container
/// - `CL`: concrete [`CommitLog`] implementation
pub struct Partition<M, X, T, CL>(pub CL, PhantomData<(M, X, T)>)
where
    M: serde::Serialize + serde::de::DeserializeOwned,
    X: Deref<Target = [u8]>,
    T: Deref<Target = [u8]>,
    CL: CommitLog<M, T>;

impl<M, X, T, CL> Partition<M, X, T, CL>
where
    M: serde::Serialize + serde::de::DeserializeOwned,
    X: Deref<Target = [u8]>,
    T: Deref<Target = [u8]>,
    CL: CommitLog<M, T>,
{
    /// Creates a new [`Partition`] from the given [`CommitLog`] instance.
    pub fn new(commit_log: CL) -> Self {
        Self(commit_log, PhantomData)
    }
}

#[async_trait(?Send)]
impl<X, T, CL> super::super::Partition for Partition<RecordMetadata<()>, X, T, CL>
where
    X: Deref<Target = [u8]>,
    T: Deref<Target = [u8]>,
    CL: CommitLog<RecordMetadata<()>, T>,
    CL::Error: std::error::Error,
{
    type Error = PartitionError<RecordMetadata<()>, T, CL>;
    type Request = PartitionRequest<X>;
    type Response = Response<T>;

    async fn serve_idempotent(
        &self,
        request: Self::Request,
    ) -> Result<Self::Response, Self::Error> {
        match request {
            PartitionRequest::LowestOffset => Ok(Response::LowestOffset(self.0.lowest_offset())),
            PartitionRequest::HighestOffset => Ok(Response::HighestOffset(self.0.highest_offset())),
            PartitionRequest::Read { offset } => self
                .0
                .read(offset)
                .await
                .map(|(record, next_offset)| Response::Read {
                    record,
                    next_offset,
                })
                .map_err(PartitionError::CommitLog),
            _ => Err(PartitionError::NotSupported),
        }
    }

    async fn serve(&mut self, request: Self::Request) -> Result<Self::Response, Self::Error> {
        match request {
            PartitionRequest::RemoveExpired { expiry_duration } => self
                .0
                .remove_expired(expiry_duration)
                .await
                .map(|_| Response::ExpiredRemoved)
                .map_err(PartitionError::CommitLog),
            PartitionRequest::Append { record_bytes } => self
                .0
                .append(
                    &record_bytes,
                    RecordMetadata {
                        offset: self.0.highest_offset(),
                        additional_metadata: (),
                    },
                )
                .await
                .map(|(write_offset, bytes_written)| Response::Append {
                    write_offset,
                    bytes_written,
                })
                .map_err(PartitionError::CommitLog),
            _ => self.serve_idempotent(request).await,
        }
    }

    async fn close(self) -> Result<(), Self::Error> {
        self.0.close().await.map_err(PartitionError::CommitLog)
    }

    async fn remove(self) -> Result<(), Self::Error> {
        self.0.remove().await.map_err(PartitionError::CommitLog)
    }
}

pub mod segmented_log {
    //! Module providing entities and utilities exclusive to `segmented_log`.
    use crate::{commit_log::prelude::SegmentedLogConfig, server::partition::PartitionId};
    use std::{
        borrow::Cow,
        path::{Path, PathBuf},
    };

    /// Configuration for a [`Partition`](super::Partition) using a `segmented_log`.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct PartitionConfig {
        pub base_storage_directory: Cow<'static, str>,
        pub segmented_log_config: SegmentedLogConfig,
    }

    /// Obtain partition base storage directory for a specific partition from a given
    /// base directory and it's partition id.
    #[inline]
    pub fn partition_storage_path<P: AsRef<Path>>(
        base_directory: P,
        partition_id: &PartitionId,
    ) -> PathBuf {
        base_directory.as_ref().join(format!(
            "{}/{}",
            partition_id.topic, partition_id.partition_number
        ))
    }
}
