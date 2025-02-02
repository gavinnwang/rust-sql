use crate::{buffer_pool::BufferPoolManager, frame::PageFrame, typedef::PageId};

/// Immutable page handle for read access.
pub(crate) struct PageFrameRefHandle<'a> {
    bpm: &'a mut BufferPoolManager,
    page_frame: &'a PageFrame,
}

impl<'a> PageFrameRefHandle<'a> {
    /// Creates a new immutable handle to a page.
    pub(crate) fn new(bpm: &'a mut BufferPoolManager, page_frame: &'a PageFrame) -> Self {
        PageFrameRefHandle { bpm, page_frame }
    }
}

impl<'a> Drop for PageFrameRefHandle<'a> {
    /// Calls `unpin_page()` when dropped, assuming `is_dirty = false`.
    fn drop(&mut self) {
        self.bpm.unpin_page(self.page_frame.page_id(), false);
    }
}

/// Mutable page handle for safe write access.
pub(crate) struct PageFrameMutHandle<'a> {
    bpm: &'a mut BufferPoolManager,
    page_id: PageId,
}

impl<'a> PageFrameMutHandle<'a> {
    /// Creates a new mutable handle to a page.
    pub(crate) fn new(bpm: &'a mut BufferPoolManager, page_id: PageId) -> Self {
        PageFrameMutHandle { bpm, page_id }
    }
}

impl<'a> Drop for PageFrameMutHandle<'a> {
    /// Calls `unpin_page()` when dropped, assuming `is_dirty = true`.
    fn drop(&mut self) {
        self.bpm.unpin_page(self.page_id, true);
    }
}
