use crate::{buffer_pool::BufferPoolManager, frame::PageFrame, typedef::PageId};

/// Immutable page handle for read access.
pub(crate) struct PageFrameRefHandle<'a> {
    bpm: *mut BufferPoolManager, // Use raw pointer to avoid borrow checker issues
    page_frame: &'a PageFrame,
}

impl<'a> PageFrameRefHandle<'a> {
    /// Creates a new immutable handle to a page.
    pub(crate) fn new(bpm: *mut BufferPoolManager, page_frame: &'a PageFrame) -> Self {
        PageFrameRefHandle { bpm, page_frame }
    }

    /// Returns a reference to the page frame.
    pub(crate) fn page_frame(&self) -> &PageFrame {
        self.page_frame
    }

    /// Provides safe access to `bpm` as immutable.
    fn bpm(&self) -> &BufferPoolManager {
        unsafe { &*self.bpm }
    }

    /// Provides safe access to `bpm` as mutable.
    fn bpm_mut(&mut self) -> &mut BufferPoolManager {
        unsafe { &mut *self.bpm }
    }
}

impl<'a> Drop for PageFrameRefHandle<'a> {
    /// Calls `unpin_page()` when dropped, assuming `is_dirty = false`.
    fn drop(&mut self) {
        unsafe {
            (*self.bpm).unpin_page(self.page_frame.page_id(), false);
        }
    }
}

/// Mutable page handle for safe write access.
pub(crate) struct PageFrameMutHandle<'a> {
    bpm: *mut BufferPoolManager, // Use raw pointer to avoid borrow checker issues
    page_frame: &'a mut PageFrame,
}

impl<'a> PageFrameMutHandle<'a> {
    /// Creates a new mutable handle to a page.
    pub(crate) fn new(bpm: *mut BufferPoolManager, page_frame: &'a mut PageFrame) -> Self {
        PageFrameMutHandle { bpm, page_frame }
    }

    /// Returns a mutable reference to the page frame.
    pub(crate) fn page_frame_mut(&mut self) -> &mut PageFrame {
        self.page_frame
    }

    /// Provides safe access to `bpm` as immutable.
    fn bpm(&self) -> &BufferPoolManager {
        unsafe { &*self.bpm }
    }

    /// Provides safe access to `bpm` as mutable.
    fn bpm_mut(&mut self) -> &mut BufferPoolManager {
        unsafe { &mut *self.bpm }
    }
}

impl<'a> Drop for PageFrameMutHandle<'a> {
    /// Calls `unpin_page()` when dropped, assuming `is_dirty = true`.
    fn drop(&mut self) {
        unsafe {
            (*self.bpm).unpin_page(self.page_frame.page_id(), true);
        }
    }
}

impl<'a> AsRef<PageFrame> for PageFrameRefHandle<'a> {
    fn as_ref(&self) -> &PageFrame {
        self.page_frame
    }
}

impl<'a> AsMut<PageFrame> for PageFrameMutHandle<'a> {
    fn as_mut(&mut self) -> &mut PageFrame {
        self.page_frame
    }
}

impl<'a> AsRef<PageFrame> for PageFrameMutHandle<'a> {
    fn as_ref(&self) -> &PageFrame {
        self.page_frame
    }
}

#[cfg(test)]
mod tests {
    use crate::disk::disk_manager::DiskManager;
    use crate::{buffer_pool::BufferPoolManager, replacer::lru_replacer::LruReplacer};
    use std::sync::{Arc, RwLock};

    #[test]
    fn test_mut_handle_unpins_on_drop() {
        let disk = Arc::new(RwLock::new(DiskManager::new("test.db").unwrap()));
        let replacer = Box::new(LruReplacer::new());
        let mut bpm = BufferPoolManager::new(10, disk, replacer);

        {
            let handle = bpm.create_page_handle();
            let cnt = handle.unwrap().page_frame.pin_count();
            assert_eq!(1, cnt);
        }
    }
}
