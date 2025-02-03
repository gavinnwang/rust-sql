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
            let root_page_handle = BufferPoolManager::create_page_handle(bpm.clone())
                .expect("Failed to create root page for table heap");
            let mut table_page = TablePageMut::from(root_page_handle);
            table_page.init_header(INVALID_PAGE_ID);
            table_page.page_id()
        };

        TableHeap {
            page_cnt: 1,
            bpm,
            first_page_id,
            last_page_id: first_page_id,
        }
    }

    pub(crate) fn first_page_id(&self) -> PageId {
        self.first_page_id
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use crate::disk::disk_manager::DiskManager;
    use crate::heap::table_heap::TableHeap;
    use crate::page::table_page::{TABLE_PAGE_HEADER_SIZE, TUPLE_INFO_SIZE};
    use crate::page::PAGE_SIZE;
    use crate::replacer::lru_replacer::LruReplacer;
    use crate::{buffer_pool::BufferPoolManager, tuple::Tuple, Result};

    /// Test that we can insert a tuple into the table heap and then retrieve it correctly.
    #[test]
    fn test_table_heap_insert_and_get() -> Result<()> {
        let disk = Arc::new(RwLock::new(DiskManager::new("test.db").unwrap()));
        let replacer = Box::new(LruReplacer::new());
        let bpm = Arc::new(RwLock::new(BufferPoolManager::new(10, disk, replacer)));

        let mut table_heap = TableHeap::new(bpm.clone());

        let tuple_data = vec![10, 20, 30, 40, 50];
        let tuple = Tuple::new(tuple_data.clone());

        let rid = table_heap.insert_tuple(&tuple)?;
        let (meta, retrieved_tuple) = table_heap.get_tuple(&rid)?;
        assert_eq!(retrieved_tuple.data(), tuple_data.as_slice());
        assert!(!meta.is_deleted());

        Ok(())
    }

    /// Test that a tuple insertion that would overflow the current page
    /// triggers allocation of a new page and that both tuples are correctly stored.
    #[test]
    fn test_table_heap_new_page_allocation() -> Result<()> {
        let disk = Arc::new(RwLock::new(DiskManager::new("test.db").unwrap()));
        let replacer = Box::new(LruReplacer::new());
        let bpm = Arc::new(RwLock::new(BufferPoolManager::new(2, disk, replacer)));

        // Create a new table heap.
        let mut table_heap = TableHeap::new(bpm.clone());

        // Create and insert a huge tuple that nearly fills the page.
        let huge_tuple_size = PAGE_SIZE - TABLE_PAGE_HEADER_SIZE - TUPLE_INFO_SIZE - 5;
        let huge_tuple_data = vec![1; huge_tuple_size];
        let huge_tuple = Tuple::new(huge_tuple_data.clone());
        let rid1 = table_heap.insert_tuple(&huge_tuple)?;

        // Insert another tuple. This insertion should detect insufficient space in the
        // current page and cause a new page to be allocated.
        let small_tuple_data = vec![2, 3, 4, 5, 2, 3, 4, 5, 2, 3, 4, 5, 2, 3, 4, 5];
        let small_tuple = Tuple::new(small_tuple_data.clone());
        let rid2 = table_heap.insert_tuple(&small_tuple)?;

        // Verify that the two record IDs have different page ids.
        assert_ne!(rid1.page_id(), rid2.page_id());

        // Retrieve both tuples and verify their data.
        let (_meta1, retrieved_huge) = table_heap.get_tuple(&rid1)?;
        let (_meta2, retrieved_small) = table_heap.get_tuple(&rid2)?;
        assert_eq!(retrieved_huge.data(), huge_tuple_data.as_slice());
        assert_eq!(retrieved_small.data(), small_tuple_data.as_slice());

        Ok(())
    }
}
