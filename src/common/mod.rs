//! Module containing common utilities used throughout `laminarmq`.

pub mod stream {
    //! Module containing common utilities for managing [`futures_lite::Stream`](s).

    /// Error to represent undexpect stream termination or overflow, i.e a stream of unexpected
    /// length.
    #[derive(Debug)]
    pub struct StreamUnexpectedLength;

    impl std::fmt::Display for StreamUnexpectedLength {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{self:?}")
        }
    }

    impl std::error::Error for StreamUnexpectedLength {}
}

pub mod ref_ops {
    //! Module providing utilities for [`Deref`] and [`AsRef`] interop.

    /// Wraps a slice to provide an [`AsRef`] implementation.
    pub struct DerefToAsRef<T>(T);

    impl<T> DerefToAsRef<T> {
        /// Creates a new [`DerefToAsRef`] instance from a [`Deref`] instance.
        pub fn new(deref_value: T) -> Self {
            Self(deref_value)
        }
    }

    impl<T, X> AsRef<[X]> for DerefToAsRef<T>
    where
        T: std::ops::Deref<Target = [X]>,
    {
        fn as_ref(&self) -> &[X] {
            self.0.deref()
        }
    }
}

pub mod http;
pub mod serde_compat;
pub mod split;
pub mod tokio_compat;
