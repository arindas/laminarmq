use super::{
    super::super::commit_log::segmented_log::segment::{SegmentStorage, SegmentStorageProvider},
    storage::{InMemStorage, InMemStorageError},
};
use async_trait::async_trait;
use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

type Mem = Rc<RefCell<Vec<u8>>>;

type Map<Idx> = BTreeMap<Idx, (Mem, Mem)>;

type StorageMap<Idx> = Rc<RefCell<Map<Idx>>>;

#[derive(Default, Clone)]
pub struct InMemSegmentStorageProvider<Idx> {
    _storage_map: StorageMap<Idx>,
}

#[async_trait(?Send)]
impl<Idx> SegmentStorageProvider<InMemStorage, Idx> for InMemSegmentStorageProvider<Idx>
where
    Idx: Clone + Ord,
{
    async fn obtain_base_indices_of_stored_segments(
        &mut self,
    ) -> Result<Vec<Idx>, InMemStorageError> {
        loop {
            let mut storage_map = self
                ._storage_map
                .try_borrow_mut()
                .map_err(|_| InMemStorageError::BorrowError)?;

            if storage_map.is_empty() {
                break;
            }

            let (_, (index, _)) = storage_map
                .last_key_value()
                .ok_or(InMemStorageError::StorageNotFound)?;

            if !index
                .try_borrow()
                .map_err(|_| InMemStorageError::BorrowError)?
                .is_empty()
            {
                break;
            }

            storage_map
                .pop_last()
                .ok_or(InMemStorageError::StorageNotFound)?;
        }

        Ok(self
            ._storage_map
            .try_borrow()
            .map_err(|_| InMemStorageError::BorrowError)?
            .keys()
            .cloned()
            .collect())
    }

    async fn obtain(
        &mut self,
        segment_base_idx: &Idx,
    ) -> Result<SegmentStorage<InMemStorage>, InMemStorageError> {
        let mut storage_map = self
            ._storage_map
            .try_borrow_mut()
            .map_err(|_| InMemStorageError::BorrowError)?;

        if !storage_map.contains_key(segment_base_idx) {
            let (index_storage, store_storage) = (
                Rc::new(RefCell::new(Vec::<u8>::new())),
                Rc::new(RefCell::new(Vec::<u8>::new())),
            );

            storage_map.insert(segment_base_idx.clone(), (index_storage, store_storage));
        }

        let (index, store) = storage_map
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
        super::super::super::{super::common::serde::bincode, commit_log::segmented_log::segment},
        *,
    };
    use std::marker::PhantomData;

    #[test]
    fn test_segment_read_append_truncate_consistency() {
        futures_lite::future::block_on(async {
            segment::test::_test_segment_read_append_truncate_consistency(
                InMemSegmentStorageProvider::<u32>::default(),
                PhantomData::<((), crc32fast::Hasher, bincode::BinCode)>,
            )
            .await;
        });
    }
}
