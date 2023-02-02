pub mod index;
pub mod segment;
pub mod store;

pub type Record<M, Idx, T> = super::Record<MetaWithIdx<M, Idx>, T>;

pub struct MetaWithIdx<M, Idx> {
    pub metadata: M,
    pub index: Idx,
}
