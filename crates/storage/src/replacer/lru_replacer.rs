use crate::typedef::FrameId;
use std::collections::HashMap;

use super::replacer::Replacer;

struct LruNode {
    frame_id: FrameId,
    is_evictable: bool,
    last_accessed_timestamp: u64,
}

pub(crate) struct LruReplacer {
    node_store: HashMap<FrameId, LruNode>,
    evictable_count: usize, // Tracks evictable nodes
    current_timestamp: u64,
}

impl LruReplacer {
    pub(crate) fn new() -> Self {
        LruReplacer {
            node_store: HashMap::new(),
            evictable_count: 0,
            current_timestamp: 0,
        }
    }

    fn current_timestamp(&mut self) -> u64 {
        let old_timestamp = self.current_timestamp;
        self.current_timestamp += 1;
        return old_timestamp;
    }
}

impl Replacer for LruReplacer {
    /// Evicts the least recently used evictable frame.
    fn evict(&mut self) -> Option<FrameId> {
        let lru_frame = self
            .node_store
            .values()
            .filter(|node| node.is_evictable) // Only consider evictable frames
            .min_by_key(|node| node.last_accessed_timestamp) // Find the smallest timestamp
            .map(|node| node.frame_id);

        if let Some(frame_id) = lru_frame {
            self.node_store.remove(&frame_id);
            self.evictable_count -= 1;
            return Some(frame_id);
        }

        None
    }

    /// Marks a frame as not evictable (i.e., pinned).
    fn pin(&mut self, frame_id: FrameId) {
        if let Some(node) = self.node_store.get_mut(&frame_id) {
            if node.is_evictable {
                node.is_evictable = false;
                self.evictable_count -= 1;
            }
        }
    }

    /// Marks a frame as evictable
    fn unpin(&mut self, frame_id: FrameId) {
        if let Some(node) = self.node_store.get_mut(&frame_id) {
            if !node.is_evictable {
                node.is_evictable = true;
                self.evictable_count += 1;
            }
        }
    }

    /// Records an access and updates the timestamp.
    /// If the frame_id is new, create a new node.
    fn record_access(&mut self, frame_id: FrameId) {
        let new_timestamp = self.current_timestamp();
        match self.node_store.get_mut(&frame_id) {
            Some(node) => {
                node.last_accessed_timestamp = new_timestamp;
            }
            None => {
                let node = LruNode {
                    frame_id,
                    is_evictable: true,
                    last_accessed_timestamp: self.current_timestamp(),
                };

                self.node_store.insert(frame_id, node);
                self.evictable_count += 1;
            }
        }
    }

    /// Removes a frame from LRU entirely.
    fn remove(&mut self, frame_id: FrameId) {
        if let Some(node) = self.node_store.remove(&frame_id) {
            // If the node was evictable, decrement the counter
            if node.is_evictable {
                self.evictable_count -= 1;
            } else {
                panic!("replacer remoev should only be called on evictable frame");
            }
        }
    }

    /// Returns the number of evictable frames.
    fn evictable_count(&self) -> usize {
        self.evictable_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_access() {
        let mut lru = LruReplacer::new();

        lru.record_access(1);
        lru.record_access(2);
        lru.record_access(3);

        assert_eq!(lru.evictable_count(), 3);
    }

    #[test]
    fn test_evict() {
        let mut lru = LruReplacer::new();

        lru.record_access(1);
        lru.record_access(2);
        lru.record_access(3);

        assert_eq!(lru.evictable_count(), 3);

        assert_eq!(lru.evict(), Some(1)); // LRU frame (1) should be evicted
        assert_eq!(lru.evict(), Some(2)); // Next LRU frame (2) should be evicted
        assert_eq!(lru.evict(), Some(3)); // Last frame (3) should be evicted
        assert_eq!(lru.evict(), None); // No more evictable frames
    }

    #[test]
    fn test_pin() {
        let mut lru = LruReplacer::new();

        lru.record_access(1);
        lru.record_access(2);
        lru.record_access(3);

        lru.pin(2);

        assert_eq!(lru.evictable_count(), 2); // Only 1 & 3 should be evictable
        assert_eq!(lru.evict(), Some(1)); // 1 is now LRU
        assert_eq!(lru.evict(), Some(3)); // 3 is now LRU
        assert_eq!(lru.evict(), None); // No evictable frames left
    }

    #[test]
    fn test_unpin() {
        let mut lru = LruReplacer::new();

        lru.record_access(1);
        lru.record_access(2);
        lru.record_access(3);

        lru.pin(2);
        assert_eq!(lru.evictable_count(), 2); // 2 is pinned, only 1 & 3 are evictable

        lru.unpin(2);
        assert_eq!(lru.evictable_count(), 3); // 2 is now evictable again

        assert_eq!(lru.evict(), Some(1));
        assert_eq!(lru.evict(), Some(2)); // 2 should be evictable again
    }

    #[test]
    fn test_remove() {
        let mut lru = LruReplacer::new();

        lru.record_access(1);
        lru.record_access(2);
        lru.record_access(3);

        lru.remove(2); // Remove frame 2 directly

        assert_eq!(lru.evictable_count(), 2); // Only 1 & 3 should remain
        assert_eq!(lru.evict(), Some(1));
        assert_eq!(lru.evict(), Some(3));
        assert_eq!(lru.evict(), None); // All evictable frames are gone
    }

    #[test]
    fn test_record_access_multiple_times() {
        let mut lru = LruReplacer::new();

        lru.record_access(1);
        lru.record_access(2);
        lru.record_access(3);

        assert_eq!(lru.evictable_count(), 3);

        lru.record_access(1);

        assert_eq!(lru.evict(), Some(2));
        assert_eq!(lru.evict(), Some(3));
        assert_eq!(lru.evict(), Some(1));

        assert_eq!(lru.evictable_count(), 0);
    }
}
