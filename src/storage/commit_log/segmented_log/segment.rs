use serde::{Deserialize, Serialize};

use super::{index::Index, store::Store};

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct Config<Size> {
    pub max_segment_size: Size,
}

pub struct Segment<S, H, Idx, Size> {
    _index: Index<S, Idx>,
    _store: Store<S, H>,

    _config: Config<Size>,
}
