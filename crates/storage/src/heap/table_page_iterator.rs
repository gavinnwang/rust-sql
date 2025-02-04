use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

use crate::page::INVALID_PAGE_ID;
use crate::{
    buffer_pool::BufferPoolManager, page::table_page::TablePageRef, typedef::PageId, Result,
};
use rustdb_error::Error;

/// An iterator over all pages in a table heap.
///
/// This iterator walks the page chain (via each page’s header) while iterating over the pages.
/// The lifetime `'a` is tied to the lifetime of the data in the buffer pool.
pub struct TablePageIterator<'a> {
    bpm: Arc<RwLock<BufferPoolManager>>,
    current_page_id: PageId,
    _marker: PhantomData<&'a BufferPoolManager>,
}

impl<'a> TablePageIterator<'a> {
    pub fn new(bpm: Arc<RwLock<BufferPoolManager>>, first_page_id: PageId) -> Self {
        Self {
            bpm,
            current_page_id: first_page_id,
            _marker: PhantomData,
        }
    }
}

impl<'a> Iterator for TablePageIterator<'a> {
    type Item = Result<TablePageRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_page_id == INVALID_PAGE_ID {
            return None;
        }

        let (next_page_id, table_page) = {
            let page_handle_res =
                BufferPoolManager::fetch_page_handle(self.bpm.clone(), self.current_page_id);
            let page_handle = match page_handle_res {
                Ok(handle) => handle,
                Err(_) => {
                    return Some(Err(Error::IO(format!(
                        "Failed to fetch page {}",
                        self.current_page_id
                    ))))
                }
            };

            // Create an immutable TablePageRef from the page handle.
            let table_page = TablePageRef::from(page_handle);
            (table_page.next_page_id(), table_page)
        };

        self.current_page_id = next_page_id;
        Some(Ok(table_page))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use crate::{
        buffer_pool::BufferPoolManager, disk::disk_manager::DiskManager,
        heap::table_heap::TableHeap, replacer::lru_replacer::LruReplacer, tuple::Tuple, Result,
    };

    use super::TablePageIterator;

    #[test]
    fn test_table_page_iterator() -> Result<()> {
        let disk = Arc::new(RwLock::new(DiskManager::new("test.db").unwrap()));
        let replacer = Box::new(LruReplacer::new());
        let bpm = Arc::new(RwLock::new(BufferPoolManager::new(10, disk, replacer)));

        let mut table_heap = TableHeap::new(bpm.clone());
        let tuple1 = Tuple::new(vec![1, 2, 3]);
        let tuple2 = Tuple::new(vec![4, 5, 6]);
        table_heap.insert_tuple(&tuple1)?;
        table_heap.insert_tuple(&tuple2)?;

        let mut iter = TablePageIterator::new(bpm.clone(), table_heap.first_page_id());

        let mut page_count = 0;
        while let Some(page_result) = iter.next() {
            page_result?;
            page_count += 1;
        }
        assert!(page_count >= 1);
        Ok(())
    }
}
