use crate::{buffer_pool::BufferPoolManager, frame::PageFrame, typedef::PageId};

/// Immutable page handle for read access.
pub(crate) struct PageFrameRefHandle {
    bpm: *mut BufferPoolManager,
    page_frame: *const PageFrame,
}

impl PageFrameRefHandle {
    /// Creates a new immutable handle to a page.
    pub(crate) fn new(bpm: *mut BufferPoolManager, page_frame: *const PageFrame) -> Self {
        PageFrameRefHandle {
            bpm: bpm as *mut BufferPoolManager,
            page_frame: page_frame as *const PageFrame,
        }
    }

    pub(crate) fn page(&self) -> &PageFrame {
        unsafe { &*self.page_frame }
    }
}

impl Drop for PageFrameRefHandle {
    /// Calls `unpin_page()` when dropped, assuming `is_dirty = false`.
    fn drop(&mut self) {
        unsafe {
            (*self.bpm).unpin_page((*self.page_frame).page_id(), false);
        }
    }
}

/// Mutable page handle for safe write access.
pub(crate) struct PageFrameMutHandle {
    bpm: *mut BufferPoolManager,
    page_frame: *mut PageFrame,
}

impl PageFrameMutHandle {
    /// Creates a new mutable handle to a page.
    pub(crate) fn new(bpm: *mut BufferPoolManager, page_frame: *mut PageFrame) -> Self {
        PageFrameMutHandle {
            bpm: bpm as *mut BufferPoolManager,
            page_frame: page_frame as *mut PageFrame,
        }
    }

    pub(crate) fn page_mut(&self) -> &mut PageFrame {
        unsafe { &mut *self.page_frame }
    }
}

impl Drop for PageFrameMutHandle {
    /// Calls `unpin_page()` when dropped, assuming `is_dirty = true`.
    fn drop(&mut self) {
        unsafe {
            (*self.bpm).unpin_page((*self.page_frame).page_id(), true);
        }
    }
}
