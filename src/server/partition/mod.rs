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
    RemoveExpired {
        expiry_duration: std::time::Duration,
    },
    LowestOffset,
    HighestOffset,
    Append {
        record_bytes: Vec<u8>,
    },
    Read {
        offset: u64,
    },
}

impl Request {
    pub fn idempotent(&self) -> bool {
        match self {
            Request::RemoveExpired { expiry_duration: _ } => false,
            Request::LowestOffset => true,
            Request::HighestOffset => true,
            Request::Append { record_bytes: _ } => false,
            Request::Read { offset: _ } => true,
        }
    }
}

pub type Record = crate::commit_log::Record<'static>;

pub enum Response {
    ExpiredRemoved,
    LowestOffset(u64),
    HighestOffset(u64),
    Append { write_offset: u64 },
    Read { record: Record, next_offset: u64 },
}

pub mod in_memory;
