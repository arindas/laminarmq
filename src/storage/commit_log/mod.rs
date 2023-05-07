use super::{AsyncConsume, AsyncIndexedRead, AsyncTruncate, Sizable};

pub struct Record<M, T> {
    pub metadata: M,
    pub value: T,
}

#[async_trait::async_trait(?Send)]
pub trait CommitLog<M, X, T>:
    AsyncIndexedRead<Value = Record<M, T>, ReadError = Self::Error>
    + AsyncTruncate<Mark = Self::Idx, TruncError = Self::Error>
    + AsyncConsume<ConsumeError = Self::Error>
    + Sizable
{
    type Error: std::error::Error;

    async fn append(&mut self, record: Record<M, X>) -> Result<Self::Idx, Self::Error>
    where
        X: 'async_trait;

    async fn remove_expired(
        &mut self,
        _expiry_duration: std::time::Duration,
    ) -> Result<Self::Idx, Self::Error> {
        async { Ok(<Self::Idx as num::Zero>::zero()) }.await
    }
}

pub mod segmented_log;

pub(crate) mod test {
    use super::{super::common::indexed_read_stream, *};
    use futures_lite::StreamExt;
    use std::ops::Deref;

    pub(crate) async fn _test_indexed_read_contains_expected_records<R, M, X, I, Y>(
        indexed_read: &R,
        expected_records: I,
        expected_record_count: usize,
    ) where
        R: AsyncIndexedRead<Value = Record<M, X>>,
        X: Deref<Target = [u8]>,
        I: Iterator<Item = Y>,
        Y: Deref<Target = [u8]>,
    {
        let count = futures_lite::stream::iter(expected_records)
            .zip(indexed_read_stream(indexed_read, ..).await)
            .map(|(y, record)| {
                assert_eq!(y.deref(), record.value.deref());
                Some(())
            })
            .count()
            .await;

        assert_eq!(count, expected_record_count);
    }
}
