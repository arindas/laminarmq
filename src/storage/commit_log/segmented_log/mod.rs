pub mod index;
pub mod segment;
pub mod store;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct MetaWithIdx<M, Idx> {
    pub metadata: M,
    pub index: Idx,
}

pub type Record<M, Idx, T> = super::Record<MetaWithIdx<M, Idx>, T>;
