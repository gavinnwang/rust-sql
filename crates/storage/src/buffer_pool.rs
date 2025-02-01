use crate::disk_manager::DiskManager;
use crate::typedef::PageId;
use crate::{typedef::FrameId, Result};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use crate::page::page::Page;
use crate::replacer::Replacer;

pub(crate) struct BufferPoolManager {
    pages: Vec<Page>,
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
        pages.resize_with(pool_size, Page::new);

        Self {
            pages,
            page_table: HashMap::new(),
            pool_size,
            replacer: replacer_factory(pool_size),
            free_list: (0..pool_size).collect(),
            disk_manager,
        }
    }

    pub fn create_page(&mut self) -> Result<&Page> {
        let page_id = {
            let mut disk = self.disk_manager.write()?;
            disk.allocate_page()?
        };
    }

    pub(crate) fn fetch_page(&self, page_id: PageId) -> Option<&Page> {
        self.page_table.get(&page_id).map(|&idx| &self.pages[idx])
    }

    pub(crate) fn fetch_page_mut(&mut self, page_id: PageId) -> Option<&mut Page> {
        self.page_table
            .get(&page_id)
            .map(|&idx| &mut self.pages[idx])
    }

    pub(crate) fn load_page(&mut self, page_id: PageId, page_data: &[u8]) {
        if self.page_table.contains_key(&page_id) {
            return;
        }

        if self.page_table.len() < self.pool_size {
            let idx = self.page_table.len();
            self.pages[idx].write(0, page_data);
            self.page_table.insert(page_id, idx);
        } else {
            let evicted_page = self.page_table.keys().next().copied().unwrap();
            let idx = self.page_table.remove(&evicted_page).unwrap();
            self.pages[idx].write(0, page_data);
            self.page_table.insert(page_id, idx);
        }
    }
}
