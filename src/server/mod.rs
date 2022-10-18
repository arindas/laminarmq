pub mod channel {
    pub trait Sender<T> {
        type Error: std::error::Error;

        fn try_send(&self, item: T) -> Result<(), Self::Error>;
    }
}

pub mod partition {
    use std::{error::Error, marker::PhantomData};

    use async_trait::async_trait;

    use super::channel::Sender;
    use crate::commit_log::Record;

    #[async_trait(?Send)]
    pub trait Partition<'a, S: Sender<Result<Response<'a>, Self::Error>>> {
        type Error: Error;

        async fn process(&mut self, task: Task<'a, Self::Error, S>);
    }

    pub struct Task<'a, E, S>
    where
        S: Sender<Result<Response<'a>, E>>,
        E: Error,
    {
        pub partition_id: PartitionId,

        pub request: Request<'a>,
        pub response_sender: S,

        _phantom_data: PhantomData<E>,
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
            record: Record<'a>,
            next_record_offset: u64,
        },
    }
}

pub mod tokio_compat;

#[cfg(target_os = "linux")]
pub mod glommio_impl;
