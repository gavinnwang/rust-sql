use std::sync::{Arc, RwLock};

use rustdb_error::Error;

use crate::page::INVALID_PAGE_ID;
use crate::{
    buffer_pool::BufferPoolManager,
    page::table_page::{TablePageMut, TablePageRef, TupleMetadata},
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
    /// Create a new table heap. A new root page is allocated from the buffer pool.
    pub fn new(bpm: Arc<RwLock<BufferPoolManager>>) -> TableHeap {
        // Create the first (root) page.
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

    /// Retrieve a tuple given its record id.
    pub fn get_tuple(&self, rid: &RecordId) -> Result<(TupleMetadata, Tuple)> {
        // Fetch an immutable handle to the page where the tuple should reside.
        let page_handle = BufferPoolManager::fetch_page_handle(self.bpm.clone(), rid.page_id())
            .ok_or_else(|| Error::IO(rid.to_string()))?;
        let table_page_ref = TablePageRef::from(page_handle);
        table_page_ref.get_tuple(rid)
    }

    /// Insert a tuple into the table heap.
    pub fn insert_tuple(&mut self, tuple: &Tuple) -> Result<RecordId> {
        // For a newly inserted tuple the metadata is by default not deleted
        let metadata = TupleMetadata::new(false);

        // Try to fetch a mutable handle for the current last page.
        let page_handle =
            BufferPoolManager::fetch_page_mut_handle(self.bpm.clone(), self.last_page_id)
                .ok_or_else(|| {
                    Error::IO(format!(
                        "Failed to fetch mutable handle for page {}",
                        self.last_page_id
                    ))
                })?;
        let mut table_page = TablePageMut::from(page_handle);

        // Try inserting the tuple into the current page.
        match table_page.insert_tuple(&metadata, tuple) {
            Ok(rid) => Ok(rid),
            // If there isn’t enough free space
            Err(Error::OutOfBounds) => {
                // Allocate a new page.
                let new_page_handle = BufferPoolManager::create_page_handle(self.bpm.clone())
                    .ok_or_else(|| {
                        Error::IO("Failed to create a new page for table heap".to_string())
                    })?;
                let mut new_table_page = TablePageMut::from(new_page_handle);

                let new_page_id = new_table_page.page_id();

                // Update the current page’s header to point to the new page.
                table_page.set_next_page_id(new_page_id);

                // Initialize the new page (its header’s next_page_id is set to INVALID_PAGE_ID).
                new_table_page.init_header(INVALID_PAGE_ID);

                // Try inserting the tuple into the new page.
                let rid = new_table_page.insert_tuple(&metadata, tuple)?;
                // Update the table heap’s bookkeeping.
                self.last_page_id = new_page_id;
                self.page_cnt += 1;
                Ok(rid)
            }
            Err(e) => Err(e),
        }
    }
}
