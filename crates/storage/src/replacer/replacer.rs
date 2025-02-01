use crate::typedef::FrameId;

pub trait Replacer {
    /// Marks a page as unpinned, making it eligible for eviction.
    fn unpin(&mut self, frame_id: FrameId);

    /// Marks a page as pinned, preventing it from being evicted.
    fn pin(&mut self, frame_id: FrameId);

    /// Record the event that the given frame id is accessed at current timestamp.
    fn record_access(&mut self, frame_id: FrameId);

    /// Attempts to evict a page based on the replacement policy.
    /// Returns `Some(page_id)` if a page is evicted, otherwise `None`.
    fn evict(&mut self) -> Option<FrameId>;

    /// Returns the number of evictable pages in the replacer.
    fn size(&self) -> usize;
}
