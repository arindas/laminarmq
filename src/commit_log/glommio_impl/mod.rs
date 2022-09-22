//! [`SegmentedLog`](super::segmented_log::SegmentedLog) specialization for the [`glommio`] runtime.
//!
//! ### Sample usage:
//! ```
//! use laminarmq::commit_log::{
//!     segment::config::SegmentConfig,
//!     segmented_log::{common::store_file_path, config::SegmentedLogConfig as LogConfig},
//!     store::common::bincoded_serialized_record_size,
//!     CommitLog, LogScanner, Record, Scanner,
//!     glommio_impl::segmented_log::{GlommioLog as Log, GlommioLogError as LogError, SegmentCreator}
//! };
//! use glommio::{LocalExecutorBuilder, Placement};
//! use std::{
//!     fs,
//!     path::{Path, PathBuf},
//!     time::Duration,
//! };
//!
//! let local_ex = LocalExecutorBuilder::new(Placement::Unbound)
//!     .spawn(move || async move {
//!         const LOG_CONFIG: LogConfig = LogConfig {
//!             initial_offset: 0,
//!             segment_config: SegmentConfig {
//!                 store_buffer_size: 512,
//!                 max_store_bytes: 512,
//!             },
//!         };
//!
//!         let storage_dir_path =
//!             "/tmp/laminarmq_commit_log_glommio_impl_segmented_log".to_string();
//!
//!         let log = Log::new(storage_dir_path.clone(), LOG_CONFIG, SegmentCreator)
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
//!         let log = Log::new(storage_dir_path.clone(), LOG_CONFIG, SegmentCreator)
//!             .await
//!             .unwrap();
//!
//!         log.remove().await.unwrap();
//!         assert!(!PathBuf::from(&storage_dir_path,).exists());
//!     })
//!     .unwrap();
//! local_ex.join().unwrap();
//! ```
//!
//! ### Note:
//! All the modules nested in this module have their own tests. When in confusion regarding the
//! API of a particular component, read how it is tested in order to understand them better.

pub mod segment;
pub mod segmented_log;
pub mod store;

pub mod prelude {
    //! Prelude module for [`glommio_impl`](super) with common exports for convenience.

    pub use super::store::{Store as GlommioStore, StoreError as GlommioStoreError};

    pub use super::segmented_log::{GlommioLog, GlommioLogError};
    pub use glommio::io::ReadResult;
}
