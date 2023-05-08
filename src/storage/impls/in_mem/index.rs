#[cfg(test)]
mod tests {
    use super::super::{
        super::super::{commit_log::segmented_log::index, common::_TestStorage},
        storage::InMemStorage,
    };
    use std::marker::PhantomData;

    #[test]
    fn test_index_read_append_truncate_consistency() {
        futures_lite::future::block_on(async {
            index::test::_test_index_read_append_truncate_consistency(|| async {
                (
                    _TestStorage {
                        storage: InMemStorage::default(),
                        persistent: false,
                    },
                    PhantomData::<(crc32fast::Hasher, u32)>,
                )
            })
            .await;
        });
    }
}
