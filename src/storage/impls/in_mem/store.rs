#[cfg(test)]
mod tests {
    use super::super::{super::super::commit_log::segmented_log::store, storage::InMemStorage};
    use std::marker::PhantomData;

    #[test]
    fn test_store_read_append_truncate_consistency() {
        futures_lite::future::block_on(async {
            store::test::_test_store_read_append_truncate_consistency(|| async {
                (InMemStorage::default(), PhantomData::<crc32fast::Hasher>)
            })
            .await;
        });
    }
}
