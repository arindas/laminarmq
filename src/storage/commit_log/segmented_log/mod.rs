pub mod index;
pub mod segment;
pub mod store;

pub type Record<M, Idx, T> = super::Record<MetaWithIdx<M, Idx>, T>;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct MetaWithIdx<M, Idx> {
    pub metadata: M,
    pub index: Idx,
}
