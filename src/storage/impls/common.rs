use crate::storage::{
    commit_log::segmented_log::segment::{SegmentStorage, SegmentStorageProvider},
    Storage,
};
use async_trait::async_trait;
use std::{
    collections::BinaryHeap,
    fmt::{Debug, Display},
    marker::PhantomData,
    path::{Path, PathBuf},
    str::FromStr,
};

#[async_trait(?Send)]
pub trait PathAddressedStorageProvider<S>
where
    S: Storage,
{
    async fn obtain_storage<P>(&self, path: P) -> Result<S, S::Error>
    where
        P: AsRef<Path>;
}

pub struct DiskBackedSegmentStorageProvider<S, PASP, Idx> {
    path_addressed_storage_provider: PASP,
    storage_directory_path: PathBuf,

    _phantom_data: PhantomData<(S, Idx)>,
}

/// Marks a Type as safe to move accross thread boundaries.
///
/// # Safety
///
/// DiskBackedSegmentStorageProvider has PASP, PathBuf and PhantomData. PathBuf
/// and PhantomData are Send + Sync. So Send bound depends only on PASP
unsafe impl<S, PASP, Idx> Send for DiskBackedSegmentStorageProvider<S, PASP, Idx> where PASP: Send {}

/// Marks a Type's references as safe to move accross thread boundaries.
///
/// # Safety
///
/// DiskBackedSegmentStorageProvider has PASP, PathBuf and PhantomData. PathBuf
/// and PhantomData are Send + Sync. So Sync bound depends only on PASP
unsafe impl<S, PASP, Idx> Sync for DiskBackedSegmentStorageProvider<S, PASP, Idx> where PASP: Sync {}

impl<S, PASP, Idx> DiskBackedSegmentStorageProvider<S, PASP, Idx>
where
    PASP: PathAddressedStorageProvider<S> + Default,
    S: Storage,
{
    pub fn new<P>(storage_directory_path: P) -> Result<Self, std::io::Error>
    where
        P: AsRef<Path>,
    {
        Self::with_storage_directory_path_and_provider(storage_directory_path, PASP::default())
    }
}

impl<S, PASP, Idx> Clone for DiskBackedSegmentStorageProvider<S, PASP, Idx>
where
    PASP: PathAddressedStorageProvider<S> + Clone,
    S: Storage,
{
    fn clone(&self) -> Self {
        Self {
            path_addressed_storage_provider: self.path_addressed_storage_provider.clone(),
            storage_directory_path: self.storage_directory_path.clone(),
            _phantom_data: PhantomData,
        }
    }
}

impl<S, PASP, Idx> DiskBackedSegmentStorageProvider<S, PASP, Idx>
where
    PASP: PathAddressedStorageProvider<S>,
    S: Storage,
{
    pub fn with_storage_directory_path_and_provider<P>(
        storage_directory_path: P,
        storage_provider: PASP,
    ) -> Result<Self, std::io::Error>
    where
        P: AsRef<Path>,
    {
        let storage_directory_path = storage_directory_path.as_ref().to_path_buf();
        std::fs::create_dir_all(&storage_directory_path)?;

        Ok(Self {
            path_addressed_storage_provider: storage_provider,
            storage_directory_path,
            _phantom_data: PhantomData,
        })
    }
}

pub const STORE_FILE_EXTENSION: &str = "store";
pub const INDEX_FILE_EXTENSION: &str = "index";

#[async_trait(?Send)]
impl<Idx, S, PASP> SegmentStorageProvider<S, Idx> for DiskBackedSegmentStorageProvider<S, PASP, Idx>
where
    PASP: PathAddressedStorageProvider<S>,
    Idx: Clone + Ord + FromStr + Display + Debug,
    S: Storage,
    S::Error: From<std::io::Error>,
{
    async fn obtain_base_indices_of_stored_segments(&mut self) -> Result<Vec<Idx>, S::Error> {
        let read_dir = std::fs::read_dir(&self.storage_directory_path).map_err(Into::into)?;

        let base_indices = read_dir
            .filter_map(|dir_entry_result| dir_entry_result.ok().map(|dir_entry| dir_entry.path()))
            .filter(|path| {
                path.extension()
                    .map(|extension| extension == INDEX_FILE_EXTENSION)
                    .and_then(|extension_matches| extension_matches.then_some(()))
                    .is_some()
            })
            .filter_map(|path| {
                path.file_stem()
                    .and_then(|path| path.to_str())
                    .and_then(|idx_str| idx_str.parse::<Idx>().ok())
            });

        let base_indices: BinaryHeap<_> = base_indices.collect();

        Ok(base_indices.into_sorted_vec())
    }

    async fn obtain(&mut self, idx: &Idx) -> Result<SegmentStorage<S>, S::Error> {
        let store_path = self
            .storage_directory_path
            .join(format!("{idx}.{STORE_FILE_EXTENSION}"));

        let index_path = self
            .storage_directory_path
            .join(format!("{idx}.{INDEX_FILE_EXTENSION}"));

        let store = self
            .path_addressed_storage_provider
            .obtain_storage(store_path)
            .await?;

        let index = self
            .path_addressed_storage_provider
            .obtain_storage(index_path)
            .await?;

        Ok(SegmentStorage { store, index })
    }
}
