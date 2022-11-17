use super::super::{
    super::{super::commit_log::CommitLog, single_node::Response},
    single_node::PartitionRequest,
};
use async_trait::async_trait;
use std::fmt::{Debug, Display};

#[derive(Debug)]
pub enum PartitionError<CL: CommitLog> {
    CommitLog(CL::Error),
    NotSupported,
}

impl<CL> Display for PartitionError<CL>
where
    CL::Error: std::error::Error,
    CL: CommitLog,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            PartitionError::CommitLog(error) => write!(f, "CommitLog error: {:?}", error),
            PartitionError::NotSupported => write!(f, "Operation not supported."),
        }
    }
}

impl<CL> std::error::Error for PartitionError<CL>
where
    CL: CommitLog + Debug,
    CL::Error: std::error::Error,
{
}

pub struct Partition<CL: CommitLog>(CL);

#[async_trait(?Send)]
impl<CL> super::super::Partition for Partition<CL>
where
    CL: CommitLog + Debug,
    CL::Error: std::error::Error,
{
    type Error = PartitionError<CL>;
    type Request = PartitionRequest;
    type Response = Response;

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
                .append(&record_bytes)
                .await
                .map(|write_offset| Response::Append { write_offset })
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
