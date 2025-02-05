use std::sync::{Arc, RwLock};

use crate::page::INVALID_PAGE_ID;
use crate::{
    buffer_pool::BufferPoolManager, page::table_page::TablePageRef, record_id::RecordId,
    tuple::Tuple, typedef::PageId, Result,
};
use rustdb_error::Error;

use super::table_heap::TableHeap;

/// An iterator over all non-deleted tuples in a table heap.
///
/// This iterator borrows a TableHeap (to obtain the starting page ID and BPM)
/// and then walks the page chain (via each page’s header) while iterating over the
/// tuple slots. Deleted tuples are skipped.
pub struct TableTupleIterator<'a> {
    bpm: Arc<RwLock<BufferPoolManager>>,
    table_heap: &'a TableHeap,
    current_page_id: PageId,
    current_slot: u16,
}

impl<'a> TableTupleIterator<'a> {
    /// Creates a new `TableIterator` using the table heap’s starting page.
    pub fn new(bpm: Arc<RwLock<BufferPoolManager>>, table_heap: &'a TableHeap) -> Self {
        Self {
            bpm,
            table_heap,
            current_page_id: table_heap.first_page_id(),
            current_slot: 0,
        }
    }
}

impl<'a> Iterator for TableTupleIterator<'a> {
    type Item = Result<(RecordId, Tuple)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If there are no more pages, we are done.
            if self.current_page_id == INVALID_PAGE_ID {
                return None;
            }

            // Fetch header info from the current page.
            let (tuple_count, next_page_id) = {
                let page_handle_res =
                    BufferPoolManager::fetch_page_handle(&self.bpm, &self.current_page_id);
                let page_handle = match page_handle_res {
                    Ok(handle) => handle,
                    _ => {
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
                let page_handle_res =
                    BufferPoolManager::fetch_page_handle(&self.bpm, &self.current_page_id);
                let page_handle = match page_handle_res {
                    Ok(handle) => handle,
                    _ => {
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
                        // Found a non-deleted tuple; return it.
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

    use super::TableTupleIterator;

    /// Test that the iterator correctly visits all non-deleted tuples in the table heap.
    #[test]
    fn test_table_iterator() -> Result<()> {
        // Set up a test disk and buffer pool manager.
        let disk = Arc::new(RwLock::new(DiskManager::new("test.db").unwrap()));
        let replacer = Box::new(LruReplacer::new());
        let bpm = Arc::new(RwLock::new(BufferPoolManager::new(10, disk, replacer)));

        let mut table_heap = TableHeap::new(bpm.clone());

        let tuple1 = Tuple::new(vec![1, 2, 3]);
        let tuple2 = Tuple::new(vec![4, 5, 6]);
        let tuple3 = Tuple::new(vec![7, 8, 9]);
        let tuple4 = Tuple::new(vec![10, 11, 12]);
        let tuple5 = Tuple::new(vec![13, 14, 15]);

        table_heap.insert_tuple(&tuple1)?;
        table_heap.insert_tuple(&tuple2)?;
        let rid3 = table_heap.insert_tuple(&tuple3)?;
        table_heap.insert_tuple(&tuple4)?;
        table_heap.insert_tuple(&tuple5)?;

        table_heap.delete_tuple(&rid3).unwrap();

        let iter = TableTupleIterator::new(bpm.clone(), &table_heap);

        // Collect all tuples from the iterator.
        let tuples: Vec<_> = iter.collect::<Result<Vec<(RecordId, Tuple)>>>()?;
        assert_eq!(tuples.len(), 4);
        assert_eq!(tuples[0].1.data(), &[1, 2, 3]);
        assert_eq!(tuples[1].1.data(), &[4, 5, 6]);
        assert_eq!(tuples[2].1.data(), &[10, 11, 12]);
        assert_eq!(tuples[3].1.data(), &[13, 14, 15]);

        Ok(())
    }

    #[test]
    fn test_table_tuple_iterator_multiple_pages() -> Result<()> {
        let disk = Arc::new(RwLock::new(DiskManager::new("test_multiple_pages.db")?));
        let replacer = Box::new(LruReplacer::new());
        let bpm = Arc::new(RwLock::new(BufferPoolManager::new(10, disk, replacer)));
        let mut table_heap = TableHeap::new(bpm.clone());

        let pages_wanted = 10;
        let mut inserted = 0;
        let mut inserted_data: Vec<Vec<u8>> = Vec::new();
        let mut inserted_rid: Vec<RecordId> = Vec::new();
        loop {
            let tuple = Tuple::new(vec![
                (inserted % 256) as u8,
                ((inserted + 1) % 256) as u8,
                ((inserted + 2) % 256) as u8,
            ]);
            let rid = table_heap.insert_tuple(&tuple)?;
            inserted += 1;
            inserted_data.push(tuple.data().to_vec());
            inserted_rid.push(rid.clone());
            if rid.page_id() >= pages_wanted {
                break;
            }
        }

        let iter = TableTupleIterator::new(bpm.clone(), &table_heap);

        let all_tuples: Vec<(RecordId, Tuple)> = iter.collect::<Result<Vec<_>>>()?;

        let num_tuples = inserted_data.len();

        assert_eq!(all_tuples.len(), num_tuples as usize);
        for ((expected_data, expected_rid), (actual_rid, actual_tuple)) in inserted_data
            .into_iter()
            .zip(inserted_rid.into_iter())
            .zip(all_tuples.into_iter())
        {
            assert_eq!(expected_data, actual_tuple.data().to_vec());
            assert_eq!(expected_rid, actual_rid);
        }

        Ok(())
    }
}
