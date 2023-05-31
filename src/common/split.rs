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
