// table_iterator.rs

use std::sync::{Arc, RwLock};

use crate::page::INVALID_PAGE_ID;
use crate::{
    buffer_pool::BufferPoolManager,
    page::table_page::{TablePageRef, TupleMetadata},
    record_id::RecordId,
    tuple::Tuple,
    typedef::PageId,
    Result,
};
use rustdb_error::Error;

/// An iterator over all non-deleted tuples in a table heap.
///
/// The iterator walks the page chain starting at `first_page_id` and then iterates
/// over the tuple slots in each page. Deleted tuples are skipped.
pub struct TableIterator {
    bpm: Arc<RwLock<BufferPoolManager>>,
    current_page_id: PageId,
    current_slot: u16,
}

impl TableIterator {
    /// Create a new `TableIterator` starting at the given `first_page_id`.
    pub fn new(bpm: Arc<RwLock<BufferPoolManager>>, first_page_id: PageId) -> Self {
        TableIterator {
            bpm,
            current_page_id: first_page_id,
            current_slot: 0,
        }
    }
}

impl Iterator for TableIterator {
    // The iterator yields a result with the tuple’s record id and the tuple.
    type Item = Result<(RecordId, Tuple)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // No more pages.
            if self.current_page_id == INVALID_PAGE_ID {
                return None;
            }

            // Try to fetch an immutable handle for the current page.
            let page_handle_opt =
                BufferPoolManager::fetch_page_handle(self.bpm.clone(), self.current_page_id);
            let page_handle = match page_handle_opt {
                Some(handle) => handle,
                None => {
                    return Some(Err(Error::IO(format!(
                        "Failed to fetch page {}",
                        self.current_page_id
                    ))))
                }
            };

            let table_page = TablePageRef::from(page_handle);
            let tuple_count = table_page.tuple_count();

            // Iterate over the tuple slots in the current page.
            while (self.current_slot as usize) < tuple_count as usize {
                let rid = RecordId::new(self.current_page_id, self.current_slot);
                self.current_slot += 1;
                match table_page.get_tuple(&rid) {
                    Ok((meta, tuple)) => {
                        if !meta.is_deleted() {
                            // Return the tuple if it is not marked as deleted.
                            return Some(Ok((rid, tuple)));
                        }
                        // Otherwise, skip the deleted tuple.
                    }
                    Err(e) => return Some(Err(e)),
                }
            }

            // Finished iterating the current page.
            // Move to the next page and reset the slot counter.
            self.current_page_id = table_page.next_page_id();
            self.current_slot = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use crate::{
        buffer_pool::BufferPoolManager,
        disk::disk_manager::DiskManager,
        heap::table_heap::TableHeap,
        page::table_page::{TABLE_PAGE_HEADER_SIZE, TUPLE_INFO_SIZE},
        page::PAGE_SIZE,
        record_id::RecordId,
        replacer::lru_replacer::LruReplacer,
        tuple::Tuple,
        Result,
    };

    use super::TableIterator;

    /// Test that the iterator correctly visits all non-deleted tuples inserted into the table heap.
    #[test]
    fn test_table_iterator() -> Result<()> {
        // Setup a test disk and buffer pool manager.
        let disk = Arc::new(RwLock::new(DiskManager::new("iter_test.db").unwrap()));
        let replacer = Box::new(LruReplacer::new());
        let bpm = Arc::new(RwLock::new(BufferPoolManager::new(10, disk, replacer)));

        // Create a new table heap.
        let mut table_heap = TableHeap::new(bpm.clone());

        // Insert a few tuples.
        let tuple1 = Tuple::new(vec![1, 2, 3]);
        let tuple2 = Tuple::new(vec![4, 5, 6]);
        let tuple3 = Tuple::new(vec![7, 8, 9]);

        table_heap.insert_tuple(&tuple1)?;
        table_heap.insert_tuple(&tuple2)?;
        table_heap.insert_tuple(&tuple3)?;

        // Create a table iterator starting at the table heap's first page.
        let iter = TableIterator::new(bpm.clone(), table_heap.first_page_id());

        // Collect all tuples from the iterator.
        let tuples: Vec<_> = iter.collect::<Result<Vec<(RecordId, Tuple)>>>()?;
        // We expect three tuples.
        assert_eq!(tuples.len(), 3);
        // Verify the content of each tuple.
        assert_eq!(tuples[0].1.data(), &[1, 2, 3]);
        assert_eq!(tuples[1].1.data(), &[4, 5, 6]);
        assert_eq!(tuples[2].1.data(), &[7, 8, 9]);

        Ok(())
    }

    /// Test that the iterator correctly spans multiple pages.
    #[test]
    fn test_table_iterator_multiple_pages() -> Result<()> {
        let disk = Arc::new(RwLock::new(DiskManager::new("iter_test_multi.db").unwrap()));
        let replacer = Box::new(LruReplacer::new());
        // Use a small pool to force new page allocation.
        let bpm = Arc::new(RwLock::new(BufferPoolManager::new(2, disk, replacer)));

        let mut table_heap = TableHeap::new(bpm.clone());

        // Insert a huge tuple that nearly fills the page.
        let huge_tuple_size = PAGE_SIZE - TABLE_PAGE_HEADER_SIZE - TUPLE_INFO_SIZE - 5;
        let huge_tuple = Tuple::new(vec![1; huge_tuple_size]);
        table_heap.insert_tuple(&huge_tuple)?;

        // Insert a small tuple that will be placed on a new page.
        let small_tuple = Tuple::new(vec![2, 3, 4, 5]);
        table_heap.insert_tuple(&small_tuple)?;

        // Create an iterator.
        let iter = TableIterator::new(bpm.clone(), table_heap.first_page_id());
        let tuples: Vec<_> = iter.collect::<Result<Vec<(RecordId, Tuple)>>>()?;

        // Expecting two tuples from two different pages.
        assert_eq!(tuples.len(), 2);
        // The two record IDs should have different page IDs.
        assert_ne!(tuples[0].0.page_id(), tuples[1].0.page_id());

        // Verify the tuple data.
        assert_eq!(tuples[0].1.data(), vec![1; huge_tuple_size].as_slice());
        assert_eq!(tuples[1].1.data(), &[2, 3, 4, 5]);

        Ok(())
    }
}
