pub mod segmented_log {
    #[cfg(test)]
    pub mod tests {
        use super::super::super::{
            super::super::{super::common::serde::bincode, commit_log::segmented_log},
            segment::InMemSegmentStorageProvider,
        };
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
    }
}
