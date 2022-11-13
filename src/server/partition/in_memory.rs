use std::{collections::HashMap, error::Error, fmt::Display};

use async_trait::async_trait;

use super::{
    super::{Record, Response},
    PartitionId, PartitionRequest,
};

#[derive(Debug)]
pub struct Partition {
    records: HashMap<u64, Record>,
    size: usize,
}

impl Partition {
    pub fn new() -> Self {
        Self {
            records: HashMap::new(),
            size: 0,
        }
    }
}

impl Default for Partition {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum PartitionError {
    NotSupported,
    NotFound,
}

impl Display for PartitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartitionError::NotSupported => write!(f, "Operation not supported."),
            PartitionError::NotFound => write!(f, "Not found"),
        }
    }
}

impl Error for PartitionError {}

#[async_trait(?Send)]
impl super::Partition for Partition {
    type Error = PartitionError;

    async fn serve_idempotent(&self, request: PartitionRequest) -> Result<Response, Self::Error> {
        match request {
            PartitionRequest::LowestOffset => Ok(Response::LowestOffset(0)),
            PartitionRequest::HighestOffset => Ok(Response::HighestOffset(self.size as u64)),
            PartitionRequest::Read { offset } => self
                .records
                .get(&offset)
                .map(|x| {
                    let next_offset = x.offset + x.value.len() as u64;
                    Response::Read {
                        record: x.clone(),
                        next_offset,
                    }
                })
                .ok_or(PartitionError::NotFound),
            _ => Err(PartitionError::NotSupported),
        }
    }

    async fn serve(&mut self, request: PartitionRequest) -> Result<Response, Self::Error> {
        match request {
            PartitionRequest::Append { record_bytes } => {
                let current_offset = self.size as u64;
                let record_size = record_bytes.len();
                let record = Record {
                    value: record_bytes.into(),
                    offset: current_offset,
                };

                self.records.insert(current_offset, record);

                self.size += record_size;

                Ok(Response::Append {
                    write_offset: current_offset,
                })
            }
            _ => self.serve_idempotent(request).await,
        }
    }

    async fn remove(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct PartitionCreator;

#[async_trait(?Send)]
impl super::PartitionCreator<Partition> for PartitionCreator {
    async fn new_partition(
        &self,
        _partition_id: &PartitionId,
    ) -> Result<Partition, PartitionError> {
        Ok(Partition::default())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::super::partition::{Partition as _, PartitionCreator as _, PartitionId, Response},
        PartitionCreator, PartitionError, PartitionRequest as Request,
    };
    use std::time::Duration;

    #[test]
    fn test_partition() {
        futures_lite::future::block_on(async {
            let mut partition = PartitionCreator
                .new_partition(&PartitionId {
                    topic: "some_topic".into(),
                    partition_number: 0,
                })
                .await
                .unwrap();

            assert!(matches!(
                partition
                    .serve_idempotent(Request::RemoveExpired {
                        expiry_duration: Duration::from_secs(1)
                    })
                    .await,
                Err(PartitionError::NotSupported)
            ));

            assert!(matches!(
                partition
                    .serve(Request::RemoveExpired {
                        expiry_duration: Duration::from_secs(1)
                    })
                    .await,
                Err(PartitionError::NotSupported)
            ));

            let records = [
                "Lorem",
                "ipsum",
                "dolor",
                "sit",
                "amet,",
                "consectetur",
                "adipiscing",
                "elit.",
                "In",
                "sagittis",
                "orci",
                "a",
                "neque",
                "aliquet,",
                "a",
                "rutrum",
                "nisi",
                "maximus.",
            ];

            let mut total_size = 0;

            assert!(matches!(
                partition
                    .serve_idempotent(Request::Read { offset: 0 })
                    .await,
                Err(PartitionError::NotFound)
            ));

            for record in records {
                if let Response::Append { write_offset } = partition
                    .serve(Request::Append {
                        record_bytes: record.as_bytes().into(),
                    })
                    .await
                    .unwrap()
                {
                    assert_eq!(write_offset, total_size);
                } else {
                    assert!(false, "Wrong response type!");
                }

                total_size += record.len() as u64;
            }

            if let Response::LowestOffset(lowest_offset) =
                partition.serve(Request::LowestOffset).await.unwrap()
            {
                assert_eq!(lowest_offset, 0);
            } else {
                assert!(false, "Wrong response type!");
            }

            if let Response::HighestOffset(highest_offset) =
                partition.serve(Request::HighestOffset).await.unwrap()
            {
                assert_eq!(highest_offset, total_size);
            } else {
                assert!(false, "Wrong response type!");
            }

            let mut offset = 0;

            for record_str in records {
                if let Response::Read {
                    record,
                    next_offset,
                } = partition
                    .serve_idempotent(Request::Read { offset })
                    .await
                    .unwrap()
                {
                    assert_eq!(record.value, record_str.as_bytes());
                    offset = next_offset;
                } else {
                    assert!(false, "Wrong response type!");
                }
            }
        });
    }
}
