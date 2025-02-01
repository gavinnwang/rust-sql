use crate::typedef::FrameId;
use std::{
    cell::RefCell,
    collections::{HashMap, LinkedList},
    rc::Rc,
    u64,
};

use super::replacer::Replacer;

struct LruNode {
    frame_id: FrameId,
    is_evictable: bool,
    timestamp: u64,
}

type LruNodeRef = Rc<RefCell<LruNode>>;

pub(crate) struct LruReplacer {
    node_store: HashMap<FrameId, LruNode>,
    capacity: usize,
    evictable_size: usize, // Tracks evictable nodes
    last_accessed_timestamp: u64,
}

impl LruReplacer {
    pub(crate) fn new(capacity: usize) -> Self {
        LruReplacer {
            node_store: HashMap::new(),
            capacity,
            evictable_size: 0,
            last_accessed_timestamp: 0,
        }
    }

    pub(crate) fn factory() -> impl Fn(usize) -> Box<dyn Replacer> {
        |capacity| Box::new(Self::new(capacity))
    }

    fn get_timestamp(&mut self) -> u64 {
        let old_timestamp = self.last_accessed_timestamp;
        self.last_accessed_timestamp += 1;
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
            .min_by_key(|node| node.timestamp) // Find the smallest timestamp
            .map(|node| node.frame_id);

        if let Some(frame_id) = lru_frame {
            self.node_store.remove(&frame_id);
            self.evictable_size -= 1;
            return Some(frame_id);
        }

        None
    }

    /// Marks a frame as not evictable (i.e., pinned).
    fn pin(&mut self, frame_id: FrameId) {
        if let Some(node) = self.node_store.get_mut(&frame_id) {
            if node.is_evictable {
                node.is_evictable = false;
                self.evictable_size -= 1;
            }
        }
    }

    /// Marks a frame as evictable
    fn unpin(&mut self, frame_id: FrameId) {
        if let Some(node) = self.node_store.get_mut(&frame_id) {
            if !node.is_evictable {
                node.is_evictable = true;
                self.evictable_size += 1;
            }
        }
    }

    /// Records an access and updates the timestamp.
    /// If the frame_id is new, create a new node.
    fn record_access(&mut self, frame_id: FrameId) {
        let new_timestamp = self.get_timestamp();
        match self.node_store.get_mut(&frame_id) {
            Some(node) => {
                node.timestamp = new_timestamp;
            }
            None => {
                let node = LruNode {
                    frame_id,
                    is_evictable: true,
                    timestamp: self.get_timestamp(),
                };

                self.node_store.insert(frame_id, node);
                self.evictable_size += 1;
            }
        }
    }

    /// Removes a frame from LRU entirely.
    fn remove(&mut self, frame_id: FrameId) {
        if let Some(node) = self.node_store.remove(&frame_id) {
            // If the node was evictable, decrement the counter
            if node.is_evictable {
                self.evictable_size -= 1;
            } else {
                panic!("replacer remoev should only be called on evictable frame");
            }
        }
    }

    /// Returns the number of evictable frames.
    fn size(&self) -> usize {
        self.evictable_size
    }
}
