pub trait Replacer {
    /// Marks a page as unpinned, making it eligible for eviction.
    fn unpin(&mut self, page_id: usize);

    /// Marks a page as pinned, preventing it from being evicted.
    fn pin(&mut self, page_id: usize);

    /// Attempts to evict a page based on the replacement policy.
    /// Returns `Some(page_id)` if a page is evicted, otherwise `None`.
    fn evict(&mut self) -> Option<usize>;

    /// Returns the number of evictable pages in the replacer.
    fn size(&self) -> usize;
}
