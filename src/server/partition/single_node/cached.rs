use crate::{
    common::{binalt::BinAlt, borrow::BytesCow, cache::Cache},
    server::{single_node::Response, Record},
};

use super::{
    super::cached::{Partition as CachedPartition, PartitionError as CachedPartitionError},
    super::Partition,
    PartitionRequest,
};

#[async_trait::async_trait(?Send)]
impl<C, P> Partition for CachedPartition<C, P>
where
    C: Cache<u64, (BytesCow<'static>, u64)>,
    C::Error: std::error::Error,
    P: Partition<Request = PartitionRequest, Response = Response>,
    P::Error: std::error::Error,
{
    type Error = CachedPartitionError<C, P>;
    type Request = PartitionRequest;
    type Response = Response;

    async fn serve_idempotent(
        &self,
        request: Self::Request,
    ) -> Result<Self::Response, Self::Error> {
        match match request {
            PartitionRequest::Read { offset } => match self.cache.get(&offset) {
                Ok((record_bytes, next_offset)) => BinAlt::A(Response::Read {
                    record: Record {
                        value: record_bytes.clone().into(),
                        offset,
                    },
                    next_offset: *next_offset,
                }),
                _ => BinAlt::B(request),
            },
            _ => BinAlt::B(request),
        } {
            BinAlt::A(response) => Ok(response),
            BinAlt::B(request) => self
                .partition
                .serve_idempotent(request)
                .await
                .map_err(CachedPartitionError::PartitionError),
        }
    }

    async fn serve(&mut self, request: Self::Request) -> Result<Self::Response, Self::Error> {
        match match request {
            PartitionRequest::Read { offset } => match self.cache.query(&offset) {
                Ok((record_bytes, next_offset)) => BinAlt::A(Response::Read {
                    record: Record {
                        value: record_bytes.clone().into(),
                        offset,
                    },
                    next_offset: *next_offset,
                }),
                _ => BinAlt::B(request),
            },
            PartitionRequest::Append { record_bytes } => {
                let stored_record_bytes = BytesCow::from(record_bytes);

                match self
                    .partition
                    .serve(PartitionRequest::Append {
                        record_bytes: stored_record_bytes.clone().into(),
                    })
                    .await
                    .map_err(CachedPartitionError::PartitionError)?
                {
                    Response::Append {
                        write_offset,
                        bytes_written,
                    } => {
                        let entry = (stored_record_bytes, write_offset + bytes_written as u64);
                        self.cache
                            .insert(write_offset, entry)
                            .map_err(CachedPartitionError::CacheError)?;

                        BinAlt::A(Response::Append {
                            write_offset,
                            bytes_written,
                        })
                    }
                    _ => Err(CachedPartitionError::WrongResponseFromPartition)?,
                }
            }
            _ => BinAlt::B(request),
        } {
            BinAlt::A(response) => Ok(response),
            BinAlt::B(request) => self
                .partition
                .serve(request)
                .await
                .map_err(CachedPartitionError::PartitionError),
        }
    }

    async fn close(self) -> Result<(), Self::Error> {
        self.partition
            .close()
            .await
            .map_err(CachedPartitionError::PartitionError)
    }

    async fn remove(self) -> Result<(), Self::Error> {
        self.partition
            .remove()
            .await
            .map_err(CachedPartitionError::PartitionError)
    }
}
