//! Module providing abstractions on top of [`serde`] components to make them easier to use.

use std::ops::Deref;

use serde::{Deserialize, Serialize};

/// Trait to represent a serialization provider.
pub trait SerializationProvider {
    /// Serialized bytes container.
    type SerializedBytes: Deref<Target = [u8]>;

    /// Error type used by the fallible functions of this trait.
    type Error: std::error::Error;

    /// Serializes the given value.
    fn serialize<T>(value: &T) -> Result<Self::SerializedBytes, Self::Error>
    where
        T: Serialize;

    /// Returns the number of bytes used by the serialized representation of `value`.
    fn serialized_size<T>(value: &T) -> Result<usize, Self::Error>
    where
        T: Serialize;

    /// Deserializes the given serialized bytes into a `T` instance.
    fn deserialize<'a, T>(bytes: &'a [u8]) -> Result<T, Self::Error>
    where
        T: Deserialize<'a>;
}

pub mod bincode {
    //! Module providing a binary encoding serialization provider.

    /// Implements [`SerializationProvider`](super::SerializationProvider) for [`bincode`].
    pub struct BinCode;

    impl super::SerializationProvider for BinCode {
        type Error = bincode::Error;

        type SerializedBytes = Vec<u8>;

        fn serialize<T>(value: &T) -> Result<Self::SerializedBytes, Self::Error>
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
