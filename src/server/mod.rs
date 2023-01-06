//! Module providing abstractions for commit-log based message queue RPC server.

pub mod channel {
    //! Module providing traits for representing channels. These traits have to be implemented
    //! for each async runtime channel implementation.

    use async_trait::async_trait;
    use std::error::Error;

    /// Trait representing the sending end of a channel.
    pub trait Sender<T> {
        type Error: Error;

        /// Sends the given value over this channel. This method is expected not to block and
        /// return immediately.
        ///
        /// ## Errors
        /// Possible error situations could include:
        /// - unable to send item
        /// - receiving end dropped
        fn try_send(&self, item: T) -> Result<(), Self::Error>;
    }

    /// Trait representing the receiving end of a channel.
    #[async_trait(?Send)]
    pub trait Receiver<T> {
        /// Asynchronously receives the next value in the channel. A None value indicates that
        /// no items are left to be received.
        async fn recv(&self) -> Option<T>;
    }
}

pub mod single_node {
    //! Module providing single-node specific RPC request and response types. Each request
    //! has a corresponding response type and vice-versa.
    use crate::commit_log::{segmented_log::RecordMetadata, Record};

    use super::partition::PartitionId;
    use std::{borrow::Cow, collections::HashMap, io, ops::Deref, time::Duration};

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
        Read,
        Append,

        LowestOffset,
        HighestOffset,

        /// Remove expired records in partition.
        RemoveExpired,

        /// Requests a map containing a mapping from topic ids
        /// to lists of partition numbers under them.
        PartitionHierachy,
        CreatePartition,
        RemovePartition,
    }

    pub struct WireResponse<T: Deref<Target = [u8]>> {
        request_kind: RequestKind,
        serialized_bytes: Option<Vec<u8>>,
        record_bytes: Option<T>,
    }

    impl<T: Deref<Target = [u8]>> From<Response<T>> for Result<WireResponse<T>, ()> {
        fn from(value: Response<T>) -> Self {
            use bincode::serialize;

            Ok(match value {
                Response::PartitionHierachy(hierarchy) => WireResponse {
                    request_kind: RequestKind::PartitionHierachy,
                    serialized_bytes: Some(serialize(&hierarchy).map_err(|_| ())?),
                    record_bytes: None,
                },
                Response::Read {
                    record,
                    next_offset,
                } => WireResponse {
                    request_kind: RequestKind::Read,
                    serialized_bytes: Some(
                        serialize(&[record.metadata.offset, next_offset]).map_err(|_| ())?,
                    ),
                    record_bytes: Some(record.value),
                },
                Response::LowestOffset(lowest_offset) => WireResponse {
                    request_kind: RequestKind::LowestOffset,
                    serialized_bytes: Some(serialize(&lowest_offset).map_err(|_| ())?),
                    record_bytes: None,
                },
                Response::HighestOffset(highest_offset) => WireResponse {
                    request_kind: RequestKind::HighestOffset,
                    serialized_bytes: Some(serialize(&highest_offset).map_err(|_| ())?),
                    record_bytes: None,
                },
                Response::Append {
                    write_offset,
                    bytes_written,
                } => WireResponse {
                    request_kind: RequestKind::Append,
                    serialized_bytes: Some(
                        serialize(&[write_offset, bytes_written as u64]).map_err(|_| ())?,
                    ),
                    record_bytes: None,
                },
                Response::ExpiredRemoved => WireResponse {
                    request_kind: RequestKind::RemoveExpired,
                    serialized_bytes: None,
                    record_bytes: None,
                },
                Response::PartitionCreated => WireResponse {
                    request_kind: RequestKind::CreatePartition,
                    serialized_bytes: None,
                    record_bytes: None,
                },
                Response::PartitionRemoved => WireResponse {
                    request_kind: RequestKind::RemovePartition,
                    serialized_bytes: None,
                    record_bytes: None,
                },
            })
        }
    }

    impl<T: Deref<Target = [u8]>> WireResponse<T> {
        pub async fn write<W: tokio::io::AsyncWrite + Unpin>(
            &self,
            mut writer: W,
        ) -> io::Result<()> {
            use tokio::io::AsyncWriteExt;

            writer.write_u8(self.request_kind as u8).await?;

            if let Some(serialized_bytes) = self.serialized_bytes.as_ref() {
                writer.write_u64(serialized_bytes.len() as u64).await?;
                writer.write(serialized_bytes).await?;
            } else {
                writer.write_u64(0 as u64).await?;
            }

            if let Some(record_bytes) = self.record_bytes.as_ref() {
                writer.write(record_bytes.deref()).await?;
            }

            Ok(())
        }
    }
}

pub mod partition;
pub mod router;
pub mod worker;

#[cfg(not(tarpaulin_include))]
pub mod tokio_compat;

#[cfg(target_os = "linux")]
pub mod glommio_impl;
