use super::AsyncIndexedRead;
use bytes::Buf;
use futures_core::Stream;
use futures_lite::{AsyncWrite, AsyncWriteExt, StreamExt};
use num::CheckedSub;
use std::{cmp, ops::RangeBounds};

/// Writes all buffers from the given stream to the provided [`AsyncWrite`] instance. Returns the
/// number of bytes written.
pub async fn write_stream<B, S, W>(stream: &mut S, write: &mut W) -> std::io::Result<usize>
where
    B: Buf,
    S: Stream<Item = B> + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut length = 0_usize;
    while let Some(mut buf) = stream.next().await {
        while buf.has_remaining() {
            let chunk = buf.chunk();

            write.write_all(chunk).await?;

            let chunk_len = chunk.len();
            buf.advance(chunk_len);
            length += chunk_len;
        }
    }

    Ok(length)
}

/// Returns a stream of items spanning the given index bounds from the provided
/// [`AsyncIndexedRead`] instance.
pub async fn indexed_read_stream<'a, R, RB>(
    indexed_read: &'a R,
    index_bounds: RB,
) -> impl Stream<Item = R::Value> + 'a
where
    RB: RangeBounds<R::Idx>,
    R: AsyncIndexedRead,
    R::Value: 'a,
{
    let (lo_min, hi_max) = (indexed_read.lowest_index(), indexed_read.highest_index());

    let one = <R::Idx as num::One>::one();

    let hi_max = hi_max - one;

    let lo = match index_bounds.start_bound() {
        std::ops::Bound::Included(x) => *x,
        std::ops::Bound::Excluded(x) => *x + one,
        std::ops::Bound::Unbounded => lo_min,
    };

    let hi = match index_bounds.end_bound() {
        std::ops::Bound::Included(x) => *x,
        std::ops::Bound::Excluded(x) => x.checked_sub(&one).unwrap_or(lo),
        std::ops::Bound::Unbounded => hi_max,
    };

    let (lo, hi) = (cmp::max(lo, lo_min), cmp::min(hi, hi_max));

    async_stream::stream! {
        for index in num::range_inclusive(lo, hi) {
            if let Ok(record) = indexed_read.read(&index).await {
                yield record;
            }
        }
    }
}

pub(crate) mod test {
    use super::{super::Storage, write_stream};
    use futures_lite::stream;
    use num::{FromPrimitive, One, Zero};
    use std::{fmt::Debug, future::Future, ops::Deref};

    pub(crate) async fn _test_storage_read_append_truncate_consistency<SP, F, S>(
        storage_provider: SP,
    ) where
        F: Future<Output = S>,
        SP: Fn() -> F,
        S: Storage,
        S::Position: Zero,
        S::Size: One + Zero + Debug,
        S::Position: From<S::Size> + Debug,
    {
        const REQ_BYTES: &[u8] = b"Hello World!";
        let mut req_body = stream::iter(std::iter::once(REQ_BYTES));

        let mut storage = storage_provider().await;

        // 0 bytes read on 0 size storage should succeed
        storage
            .read(&S::Position::zero(), &S::Size::zero())
            .await
            .unwrap();

        // reading 1 unit of Size type from zero-th position
        // of empty storage shpuld return an error
        assert!(storage
            .read(&S::Position::zero(), &S::Size::one())
            .await
            .is_err());

        let write_position: S::Position = storage.size().into();

        let (position_0, bytes_written_0) = storage
            .append(&mut req_body, &mut write_stream)
            .await
            .unwrap();

        assert_eq!(position_0, write_position);
        assert_eq!(bytes_written_0, REQ_BYTES.len());

        const REPEAT: usize = 5;
        let mut repeated_req_body = stream::iter([REQ_BYTES; REPEAT]);

        let write_position: S::Position = storage.size().into();

        let (position_1, bytes_written_1) = storage
            .append(&mut repeated_req_body, &mut write_stream)
            .await
            .unwrap();

        assert_eq!(position_1, write_position);
        assert_eq!(bytes_written_1, REQ_BYTES.len() * REPEAT);

        assert_eq!(
            storage.size(),
            S::Size::from_usize(REQ_BYTES.len() * (1 + REPEAT)).unwrap()
        );

        let mut storage = if S::is_persistent() {
            storage.close().await.unwrap();
            storage_provider().await
        } else {
            storage
        };

        let read_bytes = storage
            .read(&position_0, &S::Size::from_usize(bytes_written_0).unwrap())
            .await
            .unwrap();
        assert_eq!(read_bytes.deref(), REQ_BYTES);

        let read_bytes = storage
            .read(&position_1, &S::Size::from_usize(bytes_written_1).unwrap())
            .await
            .unwrap();
        for i in 0..REPEAT {
            let (lo, hi) = (i * REQ_BYTES.len(), (i + 1) * REQ_BYTES.len());
            assert_eq!(REQ_BYTES, &read_bytes[lo..hi]);
        }

        storage.truncate(&position_1).await.unwrap();

        assert!(storage.read(&position_1, &S::Size::one()).await.is_err());

        let read_bytes = storage
            .read(&position_0, &S::Size::from_usize(bytes_written_0).unwrap())
            .await
            .unwrap();
        assert_eq!(read_bytes.deref(), REQ_BYTES);

        assert_eq!(
            storage.size(),
            S::Size::from_usize(REQ_BYTES.len()).unwrap()
        );

        storage.remove().await.unwrap();
    }
}
