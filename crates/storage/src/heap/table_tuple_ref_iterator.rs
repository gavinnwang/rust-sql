use crate::{record_id::RecordId, tuple::TupleRef, Result};

/// An iterator over the tuples in a table page, returning zero-copy TupleRef values.
pub struct TableTupleIterator<'a> {
    /// A reference to the table page from which we are iterating.
    page: &'a crate::page::table_page::TablePageRef<'a>,
    current_slot: u16,
}

impl<'a> TableTupleIterator<'a> {
    pub fn new(page: &'a crate::page::table_page::TablePageRef<'a>) -> Self {
        Self {
            page,
            current_slot: 0,
        }
    }
}

impl<'a> Iterator for TableTupleIterator<'a> {
    type Item = Result<TupleRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_slot >= self.page.tuple_count() {
            return None;
        }

        let rid = RecordId::new(self.page.page_id(), self.current_slot);
        let tuple_ref = self.page.get_tuple_ref(&rid);

        self.current_slot += 1;

        Some(tuple_ref)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use crate::{
        buffer_pool::BufferPoolManager,
        disk::disk_manager::DiskManager,
        heap::table_heap::TableHeap,
        page::table_page::TablePageRef,
        replacer::lru_replacer::LruReplacer,
        tuple::{Tuple, TupleRef},
        Result,
    };

    // Import the iterator that returns TupleRef values.
    use super::TableTupleIterator;

    #[test]
    fn test_table_tuple_iterator() -> Result<()> {
        // Create a disk manager and a buffer pool manager with a small pool.
        let disk = Arc::new(RwLock::new(DiskManager::new("test.db").unwrap()));
        let replacer = Box::new(LruReplacer::new());
        let bpm = Arc::new(RwLock::new(BufferPoolManager::new(10, disk, replacer)));

        // Create a table heap using the buffer pool manager.
        let mut table_heap = TableHeap::new(bpm.clone());

        // Prepare three tuples with distinct data.
        let t1_data = vec![10, 20, 30];
        let t2_data = vec![40, 50, 60];
        let t3_data = vec![70, 80, 90];
        let tuple1 = Tuple::new(t1_data.clone());
        let tuple2 = Tuple::new(t2_data.clone());
        let tuple3 = Tuple::new(t3_data.clone());

        // Insert the tuples into the table heap.
        let _rid1 = table_heap.insert_tuple(&tuple1)?;
        let _rid2 = table_heap.insert_tuple(&tuple2)?;
        let _rid3 = table_heap.insert_tuple(&tuple3)?;

        // Obtain the first page ID from the table heap.
        let first_page_id = table_heap.first_page_id();

        // Fetch a page handle from the BufferPoolManager and convert it to a TablePageRef.
        let frame_handle = BufferPoolManager::fetch_page_handle(bpm.clone(), first_page_id)?;
        let table_page = TablePageRef::from(frame_handle);

        // Create the tuple iterator on this page.
        let mut iter = TableTupleIterator::new(&table_page);

        // Iterate through the tuples and collect their data.
        let mut collected: Vec<Vec<u8>> = Vec::new();
        while let Some(tuple_result) = iter.next() {
            let tuple_ref: TupleRef = tuple_result?;
            // You can also check properties of the metadata; for example:
            assert!(!tuple_ref.metadata.is_deleted());
            // Copy the zero-copy slice into a Vec for easy comparison.
            collected.push(tuple_ref.data.to_vec());
        }

        // Ensure that we have exactly three tuples.
        assert_eq!(collected.len(), 3);
        // Verify that the collected tuple data matches the inserted tuples.
        assert_eq!(collected[0], t1_data);
        assert_eq!(collected[1], t2_data);
        assert_eq!(collected[2], t3_data);

        Ok(())
    }
}
