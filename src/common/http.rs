//! Module providing utilities for with HTTP library components.

use super::ref_ops::DerefToAsRef;
use bytes::Buf;
use hyper::{
    body::{HttpBody, SizeHint},
    HeaderMap,
};
use std::{
    convert::Infallible,
    io::Cursor,
    ops::Deref,
    pin::Pin,
    task::{Context, Poll},
};

/// Wraps a [`butes::Buf`] implementation to provide an [`HttpBody`] implementation.
pub struct BufToHttpBody<T>(Option<T>);

impl<T> BufToHttpBody<T> {
    /// Creates a new [`BufToHttpBody`] instance from a [`Buf`] instance.
    pub fn new(buf: T) -> Self {
        Self(Some(buf))
    }
}

impl<T> HttpBody for BufToHttpBody<T>
where
    T: Buf + Unpin,
{
    type Data = T;

    type Error = Infallible;

    fn poll_data(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        Poll::Ready(self.0.take().map(|x| Ok(x)))
    }

    fn poll_trailers(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<Option<HeaderMap>, Self::Error>> {
        Poll::Ready(Ok(None))
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(
            self.0
                .as_ref()
                .map(|body_buf| body_buf.remaining())
                .unwrap_or(0) as u64,
        )
    }
}

impl<T> BufToHttpBody<Cursor<DerefToAsRef<T>>>
where
    T: Deref<Target = [u8]>,
{
    /// Creates a new [`BufToHttpBody`] instance from a [`Deref`] instance.
    /// This function wraps `deref_value` with [`DerefToAsRef`], [`Cursor`] and finally
    /// [`BufToHttpBody`] to obtain the [`HttpBody`] implementation.
    pub fn with_deref_value(deref_value: T) -> Self {
        BufToHttpBody::new(Cursor::new(DerefToAsRef::new(deref_value)))
    }
}

#[cfg(test)]
mod test {
    use super::BufToHttpBody;
    use hyper::body::to_bytes;
    use std::ops::Deref;

    struct A;

    const VALUE: &[u8] = b"Hello World from common::http::test!";

    impl Deref for A {
        type Target = [u8];

        fn deref(&self) -> &Self::Target {
            VALUE
        }
    }

    #[test]
    fn test_buf_to_http_body() {
        futures_lite::future::block_on(async {
            assert_eq!(
                to_bytes(BufToHttpBody::with_deref_value(A))
                    .await
                    .unwrap()
                    .deref(),
                VALUE
            )
        });
    }
}
