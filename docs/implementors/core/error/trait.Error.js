(function() {var implementors = {
"laminarmq":[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a> for <a class=\"enum\" href=\"laminarmq/storage/impls/tokio/storage/std_random_read/enum.StdRandomReadFileStorageError.html\" title=\"enum laminarmq::storage::impls::tokio::storage::std_random_read::StdRandomReadFileStorageError\">StdRandomReadFileStorageError</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a> for <a class=\"enum\" href=\"laminarmq/storage/impls/glommio/storage/dma/enum.DmaStorageError.html\" title=\"enum laminarmq::storage::impls::glommio::storage::dma::DmaStorageError\">DmaStorageError</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a> for <a class=\"struct\" href=\"laminarmq/common/stream/struct.StreamUnexpectedLength.html\" title=\"struct laminarmq::common::stream::StreamUnexpectedLength\">StreamUnexpectedLength</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a> for <a class=\"struct\" href=\"laminarmq/common/cache/struct.UnsupportedOp.html\" title=\"struct laminarmq::common::cache::UnsupportedOp\">UnsupportedOp</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a> for <a class=\"enum\" href=\"laminarmq/storage/impls/tokio/storage/std_seek_read/enum.StdSeekReadFileStorageError.html\" title=\"enum laminarmq::storage::impls::tokio::storage::std_seek_read::StdSeekReadFileStorageError\">StdSeekReadFileStorageError</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a> for <a class=\"enum\" href=\"laminarmq/storage/impls/in_mem/storage/enum.InMemStorageError.html\" title=\"enum laminarmq::storage::impls::in_mem::storage::InMemStorageError\">InMemStorageError</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a> for <a class=\"enum\" href=\"laminarmq/storage/impls/glommio/storage/buffered/enum.BufferedStorageError.html\" title=\"enum laminarmq::storage::impls::glommio::storage::buffered::BufferedStorageError\">BufferedStorageError</a>"],["impl&lt;SE, SDE, CE&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a> for <a class=\"enum\" href=\"laminarmq/storage/commit_log/segmented_log/enum.SegmentedLogError.html\" title=\"enum laminarmq::storage::commit_log::segmented_log::SegmentedLogError\">SegmentedLogError</a>&lt;SE, SDE, CE&gt;<span class=\"where fmt-newline\">where\n    SE: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a>,\n    SDE: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a>,\n    CE: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a>,</span>"],["impl&lt;SE&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a> for <a class=\"enum\" href=\"laminarmq/storage/commit_log/segmented_log/store/enum.StoreError.html\" title=\"enum laminarmq::storage::commit_log::segmented_log::store::StoreError\">StoreError</a>&lt;SE&gt;<span class=\"where fmt-newline\">where\n    SE: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">StdError</a>,</span>"],["impl&lt;StorageError, SerDeError&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a> for <a class=\"enum\" href=\"laminarmq/storage/commit_log/segmented_log/segment/enum.SegmentError.html\" title=\"enum laminarmq::storage::commit_log::segmented_log::segment::SegmentError\">SegmentError</a>&lt;StorageError, SerDeError&gt;<span class=\"where fmt-newline\">where\n    StorageError: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a>,\n    SerDeError: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a>,</span>"],["impl&lt;StorageError&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a> for <a class=\"enum\" href=\"laminarmq/storage/commit_log/segmented_log/index/enum.IndexError.html\" title=\"enum laminarmq::storage::commit_log::segmented_log::index::IndexError\">IndexError</a>&lt;StorageError&gt;<span class=\"where fmt-newline\">where\n    StorageError: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.72.1/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a>,</span>"]]
};if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()