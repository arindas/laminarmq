//! Module containing common utilities used throughout `laminarmq`.

#[doc(hidden)]
pub mod cache;

pub mod split {
    //! Module providing a splittable slice abstraction.
    use std::ops::Deref;

    /// Trait respresenting a slice that can be split at a given position.
    pub trait SplitAt<T>: Deref<Target = [T]> + Sized {
        /// Splits this slice at the given position. The left half contains elements in the `[0, at)`
        /// index range while the right half contains elements in the `[at, self.len())` index range.
        fn split_at(self, at: usize) -> Option<(Self, Self)>;
    }

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

pub mod borrow {
    //! Module providing types making borrowed values easier to work with.
    use bytes::Bytes;
    use std::{borrow::Cow, ops::Deref};

    /// Enumeration to generalize over [`bytes::Bytes`] and [`Cow<[u8]>`]
    #[derive(Debug, Clone)]
    pub enum BytesCow<'a> {
        Owned(Bytes),
        Borrowed(Cow<'a, [u8]>),
    }

    impl<'a> From<Cow<'a, [u8]>> for BytesCow<'a> {
        fn from(cow: Cow<'a, [u8]>) -> Self {
            match cow {
                Cow::Borrowed(_) => BytesCow::Borrowed(cow),
                Cow::Owned(owned_bytes) => BytesCow::Owned(Bytes::from(owned_bytes)),
            }
        }
    }

    impl<'a> From<BytesCow<'a>> for Cow<'a, [u8]> {
        fn from(bytes_cow: BytesCow<'a>) -> Self {
            match bytes_cow {
                BytesCow::Owned(x) => Into::<Vec<u8>>::into(x).into(),
                BytesCow::Borrowed(cow) => cow,
            }
        }
    }

    impl<'a> Deref for BytesCow<'a> {
        type Target = [u8];

        fn deref(&self) -> &Self::Target {
            match self {
                BytesCow::Owned(x) => x.deref(),
                BytesCow::Borrowed(x) => x.deref(),
            }
        }
    }
}
