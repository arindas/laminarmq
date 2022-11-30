use super::{super::super::server::single_node::Response, base};
use std::{borrow::Cow, time::Duration};

pub const DEFAULT_EXPIRY_DURATION: Duration = Duration::from_secs(86400 * 7);

pub enum PartitionRequest {
    RemoveExpired { expiry_duration: Duration },

    Read { offset: u64 },
    Append { record_bytes: Cow<'static, [u8]> },

    LowestOffset,
    HighestOffset,
}

pub mod commit_log;
pub mod in_memory;

impl From<PartitionRequest> for base::Request {
    fn from(request: PartitionRequest) -> Self {
        match request {
            PartitionRequest::Read { offset } => Self::Read { offset },
            PartitionRequest::Append { record_bytes } => Self::Append { record_bytes },
            _ => Self::Unmapped,
        }
    }
}

impl From<Response> for base::Response {
    fn from(response: Response) -> Self {
        match response {
            Response::Read {
                record,
                next_offset,
            } => Self::Read {
                record,
                next_offset,
            },
            Response::Append { write_offset } => Self::Append { write_offset },
            _ => Self::Unmapped,
        }
    }
}
