#[async_trait::async_trait(?Send)]
pub trait Partition<'partition> {
    type Error: std::error::Error;

    async fn process<'req, 'resp>(
        &mut self,
        request: Request<'req>,
    ) -> Result<Response<'resp>, Self::Error>
    where
        'partition: 'resp;
}

#[doc(hidden)]
mod sample_partition_impl {
    use super::{Partition, Request, Response};
    use crate::commit_log::Record;

    struct MockPart<'a>(&'a [u8]);

    #[async_trait::async_trait(?Send)]
    impl<'partition> Partition<'partition> for MockPart<'partition> {
        type Error = std::fmt::Error;

        async fn process<'req, 'resp>(
            &mut self,
            _request: Request<'req>,
        ) -> Result<Response<'resp>, Self::Error>
        where
            'partition: 'resp,
        {
            async {}.await; // do some async stuff
            Ok(Response::Read {
                record: Record {
                    value: self.0.into(),
                    offset: 0,
                },
                next_record_offset: 0,
            })
        }
    }
}

pub struct PartitionId {
    pub topic: String,
    pub partition_number: u64,
}

pub enum Request<'a> {
    Append { record_bytes: &'a [u8] },
    Read { offset: u64 },
}

pub enum Response<'a> {
    Append {
        write_offset: u64,
    },
    Read {
        record: crate::commit_log::Record<'a>,
        next_record_offset: u64,
    },
}
