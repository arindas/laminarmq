//! [`SegmentedLog`](super::segmented_log::SegmentedLog) specialization for the [`glommio`] runtime.

pub mod segment;
pub mod segmented_log;
pub mod store;

pub mod prelude {
    //! Prelude module for [`glommio_impl`](super) with common exports for convenience.

    pub use super::store::{Store as GlommioStore, StoreError as GlommioStoreError};

    pub use super::segmented_log::{GlommioLog, GlommioLogError};
    pub use glommio::io::ReadResult;
}
