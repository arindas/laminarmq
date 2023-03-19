use super::{
    super::super::commit_log::segmented_log::segment::{SegmentStorage, SegmentStorageProvider},
    storage::{InMemStorage, InMemStorageError},
};
use async_trait::async_trait;
use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

type Mem = Rc<RefCell<Vec<u8>>>;

#[derive(Default)]
pub struct InMemSegmentStorageProvider<Idx> {
    _storage_map: BTreeMap<Idx, (Mem, Mem)>,
}

#[async_trait(?Send)]
impl<Idx> SegmentStorageProvider<InMemStorage, Idx> for InMemSegmentStorageProvider<Idx>
where
    Idx: Clone + Ord,
{
    async fn base_indices_of_stored_segments(&self) -> Result<Vec<Idx>, InMemStorageError> {
        Ok(self._storage_map.keys().cloned().collect())
    }

    async fn obtain(
        &mut self,
        segment_base_idx: &Idx,
    ) -> Result<SegmentStorage<InMemStorage>, InMemStorageError> {
        if !self._storage_map.contains_key(segment_base_idx) {
            let (index_storage, store_storage) = (
                Rc::new(RefCell::new(Vec::<u8>::new())),
                Rc::new(RefCell::new(Vec::<u8>::new())),
            );

            self._storage_map
                .insert(segment_base_idx.clone(), (index_storage, store_storage));
        }

        let (index, store) = self
            ._storage_map
            .get(segment_base_idx)
            .ok_or(InMemStorageError::StorageNotFound)?;

        Ok(SegmentStorage {
            index: InMemStorage::new(index.clone())?,
            store: InMemStorage::new(store.clone())?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::super::super::{super::common::serde, commit_log::segmented_log::segment},
        *,
    };
    use std::marker::PhantomData;

    #[test]
    fn test_segment_read_append_truncate_consistency() {
        futures_lite::future::block_on(async {
            segment::test::_test_segment_read_append_truncate(
                InMemSegmentStorageProvider::<u32>::default(),
                PhantomData::<((), crc32fast::Hasher, serde::bincode::BinCode)>,
            )
            .await;
        });
    }
}
