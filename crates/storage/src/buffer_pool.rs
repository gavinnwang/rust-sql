use crate::disk::disk_manager::DiskManager;
use crate::frame::PageFrame;
use crate::frame_handle::{PageFrameMutHandle, PageFrameRefHandle};
use crate::typedef::{FrameId, PageId};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use crate::Result;

use crate::replacer::replacer::Replacer;

pub(crate) struct BufferPoolManager {
    frames: Vec<PageFrame>,
    page_table: HashMap<PageId, FrameId>,
    replacer: Box<dyn Replacer>,
    free_list: VecDeque<FrameId>,
    disk_manager: Arc<RwLock<DiskManager>>,
}

impl BufferPoolManager {
    pub(crate) fn new(
        pool_size: usize,
        disk_manager: Arc<RwLock<DiskManager>>,
        replacer: Box<dyn Replacer>,
    ) -> Self {
        let mut pages = Vec::with_capacity(pool_size);
        pages.resize_with(pool_size, PageFrame::new);

        Self {
            frames: pages,
            page_table: HashMap::new(),
            replacer,
            free_list: (0..pool_size).collect(),
            disk_manager,
        }
    }

    /// try to find a frame in the buffer pool that is free, or pin count of zero
    fn get_free_frame(&mut self) -> Option<FrameId> {
        // use the freelist if it has available frame
        if let Some(frame_id) = self.free_list.pop_front() {
            return Some(frame_id);
        }

        // otherwise evict a frame
        let frame_id = self.replacer.evict().expect("Failed to evict a frame. Either increase bpm capacity or make sure pages are unpinned.");
        let frame = &mut self.frames[frame_id];
        assert!(
            frame.pin_count() == 0,
            "If page is evicted from replacer, it's pin count must be 0."
        );

        // flush the evicted page to disk if it is dirty
        if frame.is_dirty() {
            let mut disk = self.disk_manager.write().unwrap();
            disk.write(frame.page_id(), frame.data()).unwrap();
        }

        // if a frame is evicted to make space, we should remove the stale record in the page table
        self.page_table.remove(&frame.page_id());

        frame.reset();

        Some(frame_id)
    }

    fn create_page(&mut self) -> Option<&mut PageFrame> {
        let new_page_id = {
            let mut disk = self.disk_manager.write().unwrap();
            disk.allocate_page().unwrap()
        };

        let frame_id = self.get_free_frame()?;

        // add the new record to page table
        self.page_table.insert(new_page_id, frame_id);

        let page_frame = &mut self.frames[frame_id];

        page_frame.set_page_id(new_page_id);
        page_frame.set_dirty(false);
        // pin the new page in frame and record access
        page_frame.set_pin_count(1);
        self.replacer.record_access(frame_id);
        self.replacer.pin(frame_id);

        Some(page_frame)
    }

    fn fetch_page_mut(&mut self, page_id: PageId) -> Option<&mut PageFrame> {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let frame = &mut self.frames[frame_id];
            frame.increment_pin_count();
            self.replacer.record_access(frame_id);
            self.replacer.pin(frame_id);
            return Some(frame);
        }

        let frame_id = self.get_free_frame()?;

        self.page_table.insert(page_id, frame_id);

        let page_frame = &mut self.frames[frame_id];
        page_frame.set_page_id(page_id);
        page_frame.set_dirty(false);
        page_frame.set_pin_count(1);

        let page_data = {
            let mut disk = self.disk_manager.write().unwrap();
            disk.read(page_id).unwrap().unwrap()
        };

        page_frame.write(0, page_data.as_ref());

        Some(page_frame)
    }

    fn fetch_page(&mut self, page_id: PageId) -> Option<&PageFrame> {
        self.fetch_page_mut(page_id).map(|page| &*page)
    }

    // pub(crate) fn fetch_page_handle(&mut self, page_id: PageId) -> Option<PageFrameRefHandle> {
    //     // UNSAFE code to bypass borrow checker
    //     let self_ptr = self as *mut Self;
    //     let page_frame = self.fetch_page(page_id)?;
    //
    //     Some(PageFrameRefHandle::new(self_ptr, page_frame))
    // }
    //
    // pub(crate) fn fetch_page_mut_handle(&mut self, page_id: PageId) -> Option<PageFrameMutHandle> {
    //     // UNSAFE code to bypass borrow checker
    //     let self_ptr = self as *mut Self;
    //     let page_frame = self.fetch_page_mut(page_id)?;
    //
    //     Some(PageFrameMutHandle::new(self_ptr, page_frame))
    // }

    pub(crate) fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let page_frame = &mut self.frames[frame_id];
            if is_dirty {
                page_frame.set_dirty(true);
            }
            page_frame.decrement_pin_count();
            if page_frame.pin_count() == 0 {
                self.replacer.unpin(frame_id);
            }
        }
    }

    /// deletes page from both the bpm and disk
    fn delete_page(&mut self, page_id: PageId) -> Result<()> {
        // If the page is not in the buffer pool, return true (nothing to delete)
        if !self.page_table.contains_key(&page_id) {
            return Ok(());
        }

        let frame_id = self.page_table[&page_id];
        let page_frame = &mut self.frames[frame_id];

        // If the page is pinned, deletion is not possible
        if page_frame.pin_count() > 0 {
            // should probably return error here
            panic!("Cannot delete page when page is pinned");
        }

        self.replacer.unpin(frame_id);
        self.replacer.remove(frame_id);

        // Remove page from page_table
        self.page_table.remove(&page_id);

        // Add the frame to the free list
        self.free_list.push_back(frame_id);

        // deallocate the page on disk
        let mut disk = self.disk_manager.write().unwrap();
        disk.deallocate_page(page_id).unwrap();

        // Reset the page's metadata and memory
        page_frame.reset();

        Ok(())
    }

    fn capacity(&self) -> usize {
        self.frames.len()
    }

    fn free_frame_count(&self) -> usize {
        self.free_list.len() + self.replacer.size()
    }
}

pub(crate) fn create_page_handle(
    bpm: Arc<RwLock<BufferPoolManager>>,
) -> Option<PageFrameMutHandle<'static>> {
    let mut bpm_guard = bpm.write().unwrap();

    let bpm_ptr = &mut *bpm_guard as *mut BufferPoolManager;
    let page_frame = unsafe { (*bpm_ptr).create_page()? };

    Some(PageFrameMutHandle::new(bpm.clone(), page_frame))
}

pub(crate) fn fetch_page_handle(
    bpm: Arc<RwLock<BufferPoolManager>>,
    page_id: PageId,
) -> Option<PageFrameRefHandle<'static>> {
    let mut bpm_guard = bpm.write().unwrap();

    let bpm_ptr = &mut *bpm_guard as *mut BufferPoolManager;
    let page_frame = unsafe { (*bpm_ptr).fetch_page(page_id)? };

    Some(PageFrameRefHandle::new(bpm.clone(), page_frame))
}

pub(crate) fn fetch_page_mut_handle(
    bpm: Arc<RwLock<BufferPoolManager>>,
    page_id: PageId,
) -> Option<PageFrameMutHandle<'static>> {
    let mut bpm_guard = bpm.write().unwrap();

    let bpm_ptr = &mut *bpm_guard as *mut BufferPoolManager;
    let page_frame = unsafe { (*bpm_ptr).fetch_page_mut(page_id)? };

    Some(PageFrameMutHandle::new(bpm.clone(), page_frame))
}

#[cfg(test)]
mod tests {
    use crate::buffer_pool::{create_page_handle, BufferPoolManager};
    use crate::disk::disk_manager::DiskManager;
    use crate::replacer::lru_replacer::LruReplacer;
    use std::sync::{Arc, RwLock};

    #[test]
    fn test_create_pages_beyond_capacity() {
        let pool_size = 5;
        let disk = Arc::new(RwLock::new(DiskManager::new("test.db").unwrap()));
        let replacer = Box::new(LruReplacer::new());
        let bpm = Arc::new(RwLock::new(BufferPoolManager::new(
            pool_size, disk, replacer,
        )));

        assert_eq!(pool_size, bpm.read().unwrap().free_frame_count());

        {
            let mut handles = vec![];

            for i in 0..5 {
                let bpm_clone = Arc::clone(&bpm);
                let page_handle = create_page_handle(bpm_clone);
                assert!(page_handle.is_some(), "Failed to allocate within capacity");
                handles.push(page_handle);
                assert_eq!(pool_size - i - 1, bpm.read().unwrap().free_frame_count());
            }

            assert_eq!(0, bpm.read().unwrap().free_frame_count());
        }
        assert_eq!(5, bpm.read().unwrap().free_frame_count());
    }
}
