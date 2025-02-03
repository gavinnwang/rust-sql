use std::sync::{Arc, RwLock};

use rustdb_error::Error;

use crate::{
    buffer_pool::BufferPoolManager,
    page::table_page::{TablePageRef, TupleMetadata},
    record_id::RecordId,
    tuple::Tuple,
    typedef::PageId,
    Result,
};

pub struct TableHeap {
    page_cnt: u32,
    bpm: Arc<RwLock<BufferPoolManager>>,
    first_page_id: PageId,
    last_page_id: PageId,
}

impl TableHeap {
    pub fn new(bpm: Arc<RwLock<BufferPoolManager>>) -> TableHeap {
        let first_page_id = {
            let mut root_page_handle = BufferPoolManager::create_page_handle(bpm.clone())
                .expect("Failed to create root page for table heap");
            root_page_handle.page_frame_mut().page_id()
        };

        TableHeap {
            page_cnt: 1,
            bpm,
            first_page_id,
            last_page_id: first_page_id,
        }
    }

    pub fn get_tuple(&self, rid: &RecordId) -> Result<(TupleMetadata, Tuple)> {
        let page_handle = BufferPoolManager::fetch_page_handle(self.bpm.clone(), rid.page_id())
            .ok_or(Error::IO(rid.to_string()))?;

        let table_page_ref = TablePageRef::from(page_handle);

        table_page_ref.get_tuple(rid)
    }
}
