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
        heap::{table_heap::TableHeap, table_page_iterator::TablePageIterator},
        page::table_page::TablePageRef,
        replacer::lru_replacer::LruReplacer,
        tuple::{Tuple, TupleRef},
        Result,
    };

    use super::TableTupleIterator;

    #[test]
    fn test_table_tuple_iterator() -> Result<()> {
        let disk = Arc::new(RwLock::new(DiskManager::new("test.db").unwrap()));
        let replacer = Box::new(LruReplacer::new());
        let bpm = Arc::new(RwLock::new(BufferPoolManager::new(10, disk, replacer)));

        let mut table_heap = TableHeap::new(bpm.clone());

        let t1_data = vec![10, 20, 30];
        let t2_data = vec![40, 50, 60];
        let t3_data = vec![70, 80, 90];
        let tuple1 = Tuple::new(t1_data.clone());
        let tuple2 = Tuple::new(t2_data.clone());
        let tuple3 = Tuple::new(t3_data.clone());

        let _rid1 = table_heap.insert_tuple(&tuple1)?;
        let _rid2 = table_heap.insert_tuple(&tuple2)?;
        let _rid3 = table_heap.insert_tuple(&tuple3)?;

        let first_page_id = table_heap.first_page_id();

        let frame_handle = BufferPoolManager::fetch_page_handle(bpm.clone(), first_page_id)?;
        let table_page = TablePageRef::from(frame_handle);

        let mut iter = TableTupleIterator::new(&table_page);

        let mut collected: Vec<Vec<u8>> = Vec::new();
        while let Some(tuple_result) = iter.next() {
            let tuple_ref: TupleRef = tuple_result?;
            assert!(!tuple_ref.metadata().is_deleted());
            collected.push(tuple_ref.data().to_vec());
        }

        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0], t1_data);
        assert_eq!(collected[1], t2_data);
        assert_eq!(collected[2], t3_data);

        Ok(())
    }
    #[test]
    fn test_combined_page_and_tuple_iterators() -> Result<()> {
        let disk = Arc::new(RwLock::new(DiskManager::new("test.db").unwrap()));
        let replacer = Box::new(LruReplacer::new());
        let bpm = Arc::new(RwLock::new(BufferPoolManager::new(10, disk, replacer)));
        let mut table_heap = TableHeap::new(bpm.clone());

        let pages_wanted = 10;
        let mut num_tuples = 0;
        let mut inserted_data: Vec<Vec<u8>> = Vec::new();
        loop {
            let tuple = Tuple::new(vec![1, 2, 3]);
            let rid = table_heap.insert_tuple(&tuple)?;
            num_tuples += 1;
            inserted_data.push(tuple.data().to_vec());
            if rid.page_id() >= pages_wanted {
                break;
            }
        }

        let mut page_iter = TablePageIterator::new(bpm.clone(), table_heap.first_page_id());
        let mut all_tuples: Vec<Vec<u8>> = Vec::new();

        while let Some(page_result) = page_iter.next() {
            let page: TablePageRef = page_result?;
            let mut tuple_iter = TableTupleIterator::new(&page);
            while let Some(tuple_result) = tuple_iter.next() {
                let tuple_ref: TupleRef = tuple_result?;
                assert!(!tuple_ref.metadata().is_deleted());
                all_tuples.push(tuple_ref.data().to_vec());
            }
        }

        assert_eq!(all_tuples.len(), num_tuples as usize);
        for (expected, actual) in inserted_data.into_iter().zip(all_tuples.into_iter()) {
            assert_eq!(expected, actual);
        }

        Ok(())
    }
}
