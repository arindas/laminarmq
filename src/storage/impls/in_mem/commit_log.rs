pub mod segmented_log {
    #[cfg(test)]
    pub mod tests {
        use super::super::super::{
            super::super::{super::common::serde::bincode, commit_log::segmented_log},
            segment::InMemSegmentStorageProvider,
        };
        use futures_time::{future::FutureExt, time::Duration};
        use std::marker::PhantomData;

        #[test]
        fn test_segmented_log_read_append_truncate_consistency() {
            futures_lite::future::block_on(async {
                segmented_log::test::_test_segmented_log_read_append_truncate_consistency(
                    InMemSegmentStorageProvider::<u32>::default(),
                    PhantomData::<((), crc32fast::Hasher, bincode::BinCode)>,
                )
                .await;
            });
        }

        #[test]
        fn test_segmented_log_remove_expired_segments() {
            async_io::block_on(async {
                segmented_log::test::_test_segmented_log_remove_expired_segments(
                    InMemSegmentStorageProvider::<u32>::default(),
                    |duration| async { () }.delay(Duration::from(duration)),
                    PhantomData::<((), crc32fast::Hasher, bincode::BinCode)>,
                )
                .await;
            });
        }
    }
}
