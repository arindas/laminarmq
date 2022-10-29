#[async_trait::async_trait(?Send)]
pub trait Partition {
    type Error: std::error::Error;

    async fn serve_idempotent(&self, request: Request) -> Result<Response, Self::Error>;

    async fn serve(&mut self, request: Request) -> Result<Response, Self::Error>;
}

#[derive(Eq, Hash, PartialEq, Debug)]
pub struct PartitionId {
    pub topic: String,
    pub partition_number: u64,
}

pub enum Request {
    Append { record_bytes: Vec<u8> },
    Read { offset: u64 },
}

impl Request {
    pub fn idempotent(&self) -> bool {
        match self {
            Request::Append { record_bytes: _ } => false,
            Request::Read { offset: _ } => true,
        }
    }
}

pub enum Response {
    Append {
        write_offset: u64,
    },
    Read {
        record: crate::commit_log::Record<'static>,
        next_record_offset: u64,
    },
}
