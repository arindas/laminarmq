use super::{AsyncConsume, AsyncIndexedRead, AsyncTruncate, SizedStorage};

pub struct Record<M, T> {
    pub metadata: M,
    pub value: T,
}

#[async_trait::async_trait(?Send)]
pub trait CommitLog<M, X, T>:
    AsyncIndexedRead<Value = Record<M, T>, ReadError = Self::Error>
    + AsyncTruncate<Mark = Self::Idx, TruncError = Self::Error>
    + AsyncConsume<ConsumeError = Self::Error>
    + SizedStorage
{
    type Error: std::error::Error;

    async fn append(&mut self, record: &mut Record<M, X>) -> Result<Self::Size, Self::Error>;

    async fn remove_expired(
        &mut self,
        _expiry_duration: std::time::Duration,
    ) -> Result<Self::Idx, Self::Error> {
        async { Ok(<Self::Idx as num::Zero>::zero()) }.await
    }
}

pub mod segmented_log;
