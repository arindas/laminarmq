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

    pub mod hyper_impl {
        //! Module providing utilities for serializing [`Response`](super::Response)
        //! into a [`hyper::Response`] for responding back to the client.

        use super::Response;
        use hyper::{Body, Response as HyperResponse};
        use std::ops::Deref;

        impl<T> From<Response<T>> for HyperResponse<Body>
        where
            T: Deref<Target = [u8]>,
        {
            fn from(value: Response<T>) -> Self {
                match value {
                    Response::PartitionHierachy(_) => todo!(),
                    Response::Read {
                        record: _,
                        next_offset: _,
                    } => todo!(),
                    Response::LowestOffset(_) => todo!(),
                    Response::HighestOffset(_) => todo!(),
                    Response::Append {
                        write_offset: _,
                        bytes_written: _,
                    } => todo!(),
                    Response::ExpiredRemoved => todo!(),
                    Response::PartitionCreated => todo!(),
                    Response::PartitionRemoved => todo!(),
                }
            }
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
