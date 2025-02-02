use std::sync::{Arc, RwLock};

use crate::{buffer_pool::BufferPoolManager, typedef::PageId};

pub struct TableHeap {
    page_cnt: u32,
    buffer_pool_manager: Arc<RwLock<BufferPoolManager>>,
    first_page_id: PageId,
    last_page_id: PageId,
}

impl TableHeap {}
