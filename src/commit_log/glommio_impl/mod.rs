//! Module providing specializations of different generic entities and trait impls for the [`glommio`] runtime.
//!
//! ### Sample usage:
//! ```
//! use glommio::{LocalExecutorBuilder, Placement};
//! use std::path::PathBuf;
//!
//! use laminarmq::commit_log::prelude::*;
//!
//! let local_ex = LocalExecutorBuilder::new(Placement::Unbound)
//!     .spawn(move || async move {
//!         const LOG_CONFIG: SegmentedLogConfig = SegmentedLogConfig {
//!             initial_offset: 0,
//!             segment_config: SegmentConfig {
//!                 store_buffer_size: None,
//!                 max_store_bytes: 512,
//!             },
//!         };
//!
//!         let storage_dir_path = "/tmp/laminarmq_commit_log_glommio_impl_segmented_log";
//!
//!         let log = glommio_segmented_log(storage_dir_path, LOG_CONFIG)
//!             .await
//!             .unwrap();
//!
//!         log.close().await.unwrap();
//!
//!         assert!(PathBuf::from(store_file_path(
//!             &storage_dir_path,
//!             LOG_CONFIG.initial_offset
//!         ))
//!         .exists());
//!
//!         let log = glommio_segmented_log(storage_dir_path, LOG_CONFIG)
//!             .await
//!             .unwrap();
//!
//!         log.remove().await.unwrap();
//!         assert!(!PathBuf::from(&storage_dir_path).exists());
//!     })
//!     .unwrap();
//! local_ex.join().unwrap();
//! ```
//!
//! ### Note:
//! All the modules nested in this module have their own tests. When in confusion regarding the
//! API of a particular component, read how it is tested in order to understand them better.

pub mod segmented_log;

pub mod prelude {
    //! Prelude module for [`glommio_impl`](super) with common exports for convenience.

    pub use super::segmented_log::glommio_segmented_log;
    pub use glommio::io::ReadResult;
}
