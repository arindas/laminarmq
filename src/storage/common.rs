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
    let mut length = 0 as usize;
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
