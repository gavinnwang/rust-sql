use crate::buffer_pool::BufferPoolManager;
use crate::frame::PageFrame;
use std::sync::{Arc, RwLock};

/// Immutable page handle for read access.
pub struct PageFrameRefHandle<'a> {
    bpm: Arc<RwLock<BufferPoolManager>>,
    page_frame: &'a PageFrame,
}

impl<'a> PageFrameRefHandle<'a> {
    pub(crate) fn new(bpm: Arc<RwLock<BufferPoolManager>>, page_frame: &'a PageFrame) -> Self {
        PageFrameRefHandle { bpm, page_frame }
    }

    pub(crate) fn page_frame(&self) -> &PageFrame {
        self.page_frame
    }
}

impl<'a> Drop for PageFrameRefHandle<'a> {
    fn drop(&mut self) {
        self.bpm
            .write()
            .unwrap()
            .unpin_page(&self.page_frame.page_id(), false);
    }
}

/// Mutable page handle for write access.
pub struct PageFrameMutHandle<'a> {
    bpm: Arc<RwLock<BufferPoolManager>>,
    page_frame: &'a mut PageFrame,
}

impl<'a> PageFrameMutHandle<'a> {
    pub(crate) fn new(bpm: Arc<RwLock<BufferPoolManager>>, page_frame: &'a mut PageFrame) -> Self {
        PageFrameMutHandle { bpm, page_frame }
    }

    pub(crate) fn page_frame_mut(&mut self) -> &mut PageFrame {
        self.page_frame
    }
}

impl<'a> Drop for PageFrameMutHandle<'a> {
    fn drop(&mut self) {
        self.bpm
            .write()
            .unwrap()
            .unpin_page(&self.page_frame.page_id(), true);
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
        let bpm = Arc::new(RwLock::new(BufferPoolManager::new(10, disk, replacer)));

        {
            let handle = BufferPoolManager::create_page_handle(&bpm);
            let cnt = handle.unwrap().page_frame.pin_count();
            assert_eq!(1, cnt);
        }
    }
}
