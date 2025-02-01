use std::collections::HashMap;

use crate::typedef::FrameId;

use super::replacer::Replacer;

struct LrukNode {
    frame_id: FrameId,
    is_evictable: bool,
    last_accessed_timestamp: u64,
    k: u16,
}

pub(crate) struct LrukReplacer {
    node_store: HashMap<FrameId, LruNode>,
    evictable_size: usize, // Tracks evictable nodes
    timestamp: u64,
}

impl LrukReplacer {}

impl Replacer for LrukReplacer {
    fn unpin(&mut self, frame_id: FrameId) {
        todo!()
    }

    fn pin(&mut self, frame_id: FrameId) {
        todo!()
    }

    fn record_access(&mut self, frame_id: FrameId) {
        todo!()
    }

    fn evict(&mut self) -> Option<FrameId> {
        todo!()
    }

    fn size(&self) -> usize {
        todo!()
    }

    fn remove(&mut self, frame_id: FrameId) {
        todo!()
    }
}
