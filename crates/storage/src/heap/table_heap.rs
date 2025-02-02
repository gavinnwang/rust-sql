use std::sync::{Arc, RwLock};

use crate::{buffer_pool::BufferPoolManager, typedef::PageId};

pub struct TableHeap {
    page_cnt: u32,
    buffer_pool_manager: Arc<RwLock<BufferPoolManager>>,
    first_page_id: PageId,
    last_page_id: PageId,
}

impl TableHeap {
    pub fn new(bpm: &Arc<RwLock<BufferPoolManager>>) -> TableHeap {
        let bpm = Arc::clone(bpm);
        let first_page_id = {
            let mut bpm_handle = bpm.write().unwrap();
            let root_page = bpm_handle.create_page().unwrap();
            root_page.page_id()
        };

        TableHeap {
            page_cnt: 1,
            buffer_pool_manager: bpm,
            first_page_id,
            last_page_id: first_page_id,
        }
    }
}
