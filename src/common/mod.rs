//! Module containing common utilities used throughout `laminarmq`.

pub mod stream {
    //! Module providing some common utilities for managing streams.

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

    use std::ops::Deref;

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
        T: Deref<Target = [X]>,
    {
        fn as_ref(&self) -> &[X] {
            self.0.deref()
        }
    }
}

pub mod cache {
    pub use generational_cache::{
        cache::lru_cache::LRUCacheBlockArenaEntry,
        prelude::{
            AllocBTreeMap, AllocVec, Cache, Eviction, LRUCache, LRUCacheError, Link, Lookup, Map,
        },
    };
    use std::{fmt::Display, marker::PhantomData};

    pub type AllocLRUCache<K, T> =
        LRUCache<AllocVec<LRUCacheBlockArenaEntry<K, T>>, K, T, AllocBTreeMap<K, Link>>;

    #[derive(Debug, Default)]
    pub struct NoOpCache<K, V> {
        _phantom_data: PhantomData<(K, V)>,
    }

    #[derive(Debug, Default)]
    pub struct UnsupportedOp;

    impl Display for UnsupportedOp {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{self:?}")
        }
    }

    impl std::error::Error for UnsupportedOp {}

    impl<K, V> Cache<K, V> for NoOpCache<K, V> {
        type Error = UnsupportedOp;

        fn insert(&mut self, _: K, _: V) -> Result<Eviction<K, V>, Self::Error> {
            Err(UnsupportedOp)
        }

        fn remove(&mut self, _: &K) -> Result<Lookup<V>, Self::Error> {
            Err(UnsupportedOp)
        }

        fn shrink(&mut self, _: usize) -> Result<(), Self::Error> {
            Err(UnsupportedOp)
        }

        fn reserve(&mut self, _: usize) -> Result<(), Self::Error> {
            Err(UnsupportedOp)
        }

        fn query(&mut self, _: &K) -> Result<Lookup<&V>, Self::Error> {
            Err(UnsupportedOp)
        }

        fn capacity(&self) -> usize {
            0
        }

        fn len(&self) -> usize {
            0
        }

        fn clear(&mut self) -> Result<(), Self::Error> {
            Err(UnsupportedOp)
        }
    }
}

pub mod http;
pub mod serde_compat;
pub mod split;
pub mod tokio_compat;
