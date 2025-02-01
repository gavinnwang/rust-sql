use crate::disk_manager::DiskManager;
use crate::frame::PageFrame;
use crate::typedef::{FrameId, PageId};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use crate::replacer::Replacer;

pub(crate) struct BufferPoolManager {
    frames: Vec<PageFrame>,
    page_table: HashMap<PageId, FrameId>,
    pool_size: usize,
    replacer: Box<dyn Replacer>,
    free_list: VecDeque<FrameId>,
    disk_manager: Arc<RwLock<DiskManager>>,
}

impl BufferPoolManager {
    pub(crate) fn new<F>(
        pool_size: usize,
        disk_manager: Arc<RwLock<DiskManager>>,
        replacer_factory: F,
    ) -> Self
    where
        F: Fn(usize) -> Box<dyn Replacer>,
    {
        let mut pages = Vec::with_capacity(pool_size);
        pages.resize_with(pool_size, PageFrame::new);

        Self {
            frames: pages,
            page_table: HashMap::new(),
            pool_size,
            replacer: replacer_factory(pool_size),
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

    pub(crate) fn create_page(&mut self) -> Option<&PageFrame> {
        let new_page_id = {
            let mut disk = self.disk_manager.write().unwrap();
            disk.allocate_page().unwrap()
        };

        let frame_id = self.get_free_frame()?;

        // add the new record to page table
        self.page_table.insert(new_page_id, frame_id);

        let page_frame = &mut self.frames[frame_id];

        // pin the new page in frame and record access
        page_frame.set_pin_count(1);
        self.replacer.pin(frame_id);
        self.replacer.record_access(frame_id);

        Some(page_frame)
    }

    pub(crate) fn fetch_page_mut(&mut self, page_id: PageId) -> Option<&mut PageFrame> {
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
        page_frame.set_pin_count(1);

        let page_data = {
            let mut disk = self.disk_manager.write().unwrap();
            disk.read(page_id).unwrap().unwrap()
        };

        page_frame.write(0, page_data.as_ref());

        Some(page_frame)
    }

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

    pub(crate) fn fetch_page(&mut self, page_id: PageId) -> Option<&PageFrame> {
        self.fetch_page_mut(page_id).map(|page| &*page)
    }
}
