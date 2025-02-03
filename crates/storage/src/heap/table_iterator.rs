// table_iterator.rs

use std::sync::{Arc, RwLock};

use crate::page::INVALID_PAGE_ID;
use crate::{
    buffer_pool::BufferPoolManager, page::table_page::TablePageRef, record_id::RecordId,
    tuple::Tuple, typedef::PageId, Result,
};
use rustdb_error::Error;

// Bring in the TableHeap type.
use super::table_heap::TableHeap;

/// An iterator over all non-deleted tuples in a table heap.
///
/// This iterator borrows a TableHeap (to obtain the starting page ID and BPM)
/// and then walks the page chain (via each page’s header) while iterating over the
/// tuple slots. Deleted tuples are skipped.
pub struct TableIterator<'a> {
    bpm: Arc<RwLock<BufferPoolManager>>,
    table_heap: &'a TableHeap,
    current_page_id: PageId,
    current_slot: u16,
}

impl<'a> TableIterator<'a> {
    /// Creates a new `TableIterator` using the table heap’s starting page.
    pub fn new(bpm: Arc<RwLock<BufferPoolManager>>, table_heap: &'a TableHeap) -> Self {
        Self {
            bpm,
            table_heap,
            current_page_id: table_heap.first_page_id(), // assume a getter exists
            current_slot: 0,
        }
    }
}

impl<'a> Iterator for TableIterator<'a> {
    // Each item is a Result wrapping a (RecordId, Tuple) pair.
    type Item = Result<(RecordId, Tuple)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If there are no more pages, we are done.
            if self.current_page_id == INVALID_PAGE_ID {
                return None;
            }

            // Fetch header info from the current page.
            let (tuple_count, next_page_id) = {
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

                // Create an immutable TablePageRef from the page handle.
                let table_page = TablePageRef::from(page_handle);
                (table_page.tuple_count(), table_page.next_page_id())
            };

            // If we've exhausted the tuple slots of the current page,
            // move to the next page and reset the slot counter.
            if self.current_slot >= tuple_count {
                self.current_page_id = next_page_id;
                self.current_slot = 0;
                continue;
            }

            // Prepare the record id for the current slot.
            let rid = RecordId::new(self.current_page_id, self.current_slot);
            self.current_slot += 1;

            // Fetch the tuple from the current page.
            let tuple_result = {
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
                table_page.get_tuple(&rid)
            };

            match tuple_result {
                Ok((meta, tuple)) => {
                    if !meta.is_deleted() {
                        // Found a live tuple; return it.
                        return Some(Ok((rid, tuple)));
                    }
                    // Otherwise, skip this tuple (and continue the loop).
                }
                // OutOfBounds indicates we have no tuple in this slot.
                Err(Error::OutOfBounds) => {
                    self.current_page_id = next_page_id;
                    self.current_slot = 0;
                    continue;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use crate::{
        buffer_pool::BufferPoolManager, disk::disk_manager::DiskManager,
        heap::table_heap::TableHeap, record_id::RecordId, replacer::lru_replacer::LruReplacer,
        tuple::Tuple, Result,
    };

    use super::TableIterator;

    /// Test that the iterator correctly visits all non-deleted tuples in the table heap.
    #[test]
    fn test_table_iterator() -> Result<()> {
        // Set up a test disk and buffer pool manager.
        let disk = Arc::new(RwLock::new(DiskManager::new("iter_test.db").unwrap()));
        let replacer = Box::new(LruReplacer::new());
        let bpm = Arc::new(RwLock::new(BufferPoolManager::new(10, disk, replacer)));

        // Create a new table heap.
        let mut table_heap = TableHeap::new(bpm.clone());

        // Insert several tuples.
        let tuple1 = Tuple::new(vec![1, 2, 3]);
        let tuple2 = Tuple::new(vec![4, 5, 6]);
        let tuple3 = Tuple::new(vec![7, 8, 9]);

        table_heap.insert_tuple(&tuple1)?;
        table_heap.insert_tuple(&tuple2)?;
        table_heap.insert_tuple(&tuple3)?;

        // Create a table iterator that borrows the table heap.
        let iter = TableIterator::new(bpm.clone(), &table_heap);

        // Collect all tuples from the iterator.
        let tuples: Vec<_> = iter.collect::<Result<Vec<(RecordId, Tuple)>>>()?;
        assert_eq!(tuples.len(), 3);
        assert_eq!(tuples[0].1.data(), &[1, 2, 3]);
        assert_eq!(tuples[1].1.data(), &[4, 5, 6]);
        assert_eq!(tuples[2].1.data(), &[7, 8, 9]);

        Ok(())
    }
}
