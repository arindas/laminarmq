use super::{AsyncConsume, AsyncIndexedRead, AsyncTruncate};

pub struct Record<M, T> {
    pub metadata: M,
    pub value: T,
}

#[async_trait::async_trait(?Send)]
pub trait CommitLog<M, X, T>:
    AsyncIndexedRead<Value = Record<M, T>, ReadError = Self::Error>
    + AsyncTruncate<Mark = Self::Idx, TruncError = Self::Error>
    + AsyncConsume<ConsumeError = Self::Error>
{
    type Error: std::error::Error;

    async fn append(&mut self, record: &Record<M, X>) -> Result<(Self::Idx, usize), Self::Error>;

    async fn remove_expired(
        &mut self,
        _expiry_duration: std::time::Duration,
    ) -> Result<usize, Self::Error> {
        async { Ok(0) }.await
    }
}

pub mod segmented_log;
