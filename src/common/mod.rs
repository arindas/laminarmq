//! Module containing common utilities used throughout `laminarmq`.

#[doc(hidden)]
pub mod cache;

pub mod stream {
    #[derive(Debug)]
    pub struct StreamBroken;

    impl std::fmt::Display for StreamBroken {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{self:?}")
        }
    }

    impl std::error::Error for StreamBroken {}
}

pub mod ref_ops {

    pub struct DerefToAsRef<T>(T);

    impl<T> DerefToAsRef<T> {
        pub fn new(deref_value: T) -> Self {
            Self(deref_value)
        }
    }

    impl<T, X> AsRef<[X]> for DerefToAsRef<T>
    where
        T: std::ops::Deref<Target = [X]>,
    {
        fn as_ref(&self) -> &[X] {
            self.0.deref().as_ref()
        }
    }
}

pub mod http {
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

    pub struct BufToHttpBody<T>(Option<T>);

    impl<T> BufToHttpBody<T> {
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
}

pub mod split {
    //! Module providing a splittable slice abstraction.
    use std::ops::Deref;

    /// Trait respresenting a slice that can be split at a given position.
    pub trait SplitAt<T>: Deref<Target = [T]> + Sized {
        /// Splits this slice at the given position. The left half contains elements in the `[0, at)`
        /// index range while the right half contains elements in the `[at, self.len())` index range.
        fn split_at(self, at: usize) -> Option<(Self, Self)>;
    }

    #[cfg(not(tarpaulin_include))]
    impl<T> SplitAt<T> for Vec<T> {
        fn split_at(mut self, at: usize) -> Option<(Self, Self)> {
            if at > self.len() {
                None
            } else {
                let other = self.split_off(at);
                Some((self, other))
            }
        }
    }

    #[cfg(target_os = "linux")]
    pub mod glommio_impl {
        //! Module containing [`super::SplitAt`] implementations for [`glommio`] specific types.
        use glommio::io::ReadResult;

        use super::SplitAt;

        impl SplitAt<u8> for ReadResult {
            fn split_at(self, at: usize) -> Option<(Self, Self)> {
                Some((
                    ReadResult::slice(&self, 0, at)?,
                    ReadResult::slice(&self, at, self.len() - at)?,
                ))
            }
        }
    }
}

pub mod serde {
    use std::ops::Deref;

    use serde::{Deserialize, Serialize};

    pub trait SerDe {
        type Error: std::error::Error;

        type SerBytes: Deref<Target = [u8]>;

        fn serialize<T>(value: &T) -> Result<Self::SerBytes, Self::Error>
        where
            T: Serialize;

        fn serialized_size<T>(value: &T) -> Result<usize, Self::Error>
        where
            T: Serialize;

        fn deserialize<'a, T>(bytes: &'a [u8]) -> Result<T, Self::Error>
        where
            T: Deserialize<'a>;
    }

    pub mod bincode {
        use super::SerDe;

        pub struct BinCode;

        impl SerDe for BinCode {
            type Error = bincode::Error;

            type SerBytes = Vec<u8>;

            fn serialize<T>(value: &T) -> Result<Self::SerBytes, Self::Error>
            where
                T: serde::Serialize,
            {
                bincode::serialize(value)
            }

            fn serialized_size<T>(value: &T) -> Result<usize, Self::Error>
            where
                T: serde::Serialize,
            {
                bincode::serialized_size(value).map(|x| x as usize)
            }

            fn deserialize<'a, T>(bytes: &'a [u8]) -> Result<T, Self::Error>
            where
                T: serde::Deserialize<'a>,
            {
                bincode::deserialize(bytes)
            }
        }
    }
}
