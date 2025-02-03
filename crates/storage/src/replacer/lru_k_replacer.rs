use std::collections::{HashMap, VecDeque};

use crate::typedef::FrameId;

use super::replacer::Replacer;

struct LrukNode {
    frame_id: FrameId,
    is_evictable: bool,
    history: VecDeque<u64>,
    k: usize,
}

fn max_timestamp() -> u64 {
    u64::max_value()
}

impl LrukNode {
    fn get_backwards_k_distance(&self, current_timestamp: u64) -> u64 {
        if self.has_infinite_backwards_k_distance() {
            return max_timestamp();
        }
        current_timestamp - self.get_kth_most_recent_timestamp()
    }

    fn has_infinite_backwards_k_distance(&self) -> bool {
        self.history.len() < self.k
    }

    fn get_kth_most_recent_timestamp(&self) -> u64 {
        let number_of_accesses = self.history.len();
        if number_of_accesses < self.k {
            panic!("Node has {number_of_accesses} < `k` accesses in its history.");
        }
        *self.history.front().unwrap()
    }
}

pub(crate) struct LrukReplacer {
    node_store: HashMap<FrameId, LrukNode>,
    evictable_size: usize, // Tracks evictable nodes
    current_timestamp: u64,
}

impl LrukReplacer {
    pub(crate) fn new() -> Self {
        LrukReplacer {
            node_store: HashMap::new(),
            evictable_size: 0,
            current_timestamp: 0,
        }
    }

    fn current_timestamp(&mut self) -> u64 {
        let old_timestamp = self.current_timestamp;
        self.current_timestamp += 1;
        return old_timestamp;
    }
}

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
        // self.node_store.remove(frame_id);
        todo!()
    }

    fn evictable_count(&self) -> usize {
        todo!()
    }

    fn remove(&mut self, frame_id: FrameId) {
        todo!()
    }
}
