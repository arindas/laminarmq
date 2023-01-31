use super::{
    super::{AsyncConsume, AsyncIndexedRead, AsyncTruncate},
    store::common::RecordHeader,
};
use num::Unsigned;

#[async_trait::async_trait(?Send)]
pub trait Index:
    AsyncIndexedRead<Value = (Self::Position, RecordHeader), ReadError = Self::Error>
    + AsyncTruncate<Mark = Self::Idx, TruncError = Self::Error>
    + AsyncConsume<ConsumeError = Self::Error>
{
    type Error: std::error::Error;

    type Position: Unsigned;

    async fn append(
        &mut self,
        index_record: (Self::Position, RecordHeader),
    ) -> Result<(), Self::Error>;
}
