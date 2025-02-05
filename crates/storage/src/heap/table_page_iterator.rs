use std::sync::{Arc, RwLock};

use crate::{
    buffer_pool::BufferPoolManager,
    page::{table_page::TablePageRef, INVALID_PAGE_ID},
    typedef::PageId,
    Result,
};
use rustdb_error::Error;

pub struct TablePageIterator<'a> {
    bpm: &'a Arc<RwLock<BufferPoolManager>>,
    current_page_id: PageId,
}

impl<'a> TablePageIterator<'a> {
    pub fn new(bpm: &'a Arc<RwLock<BufferPoolManager>>, first_page_id: PageId) -> Self {
        TablePageIterator {
            bpm,
            current_page_id: first_page_id,
        }
    }
}

impl<'a> Iterator for TablePageIterator<'a> {
    type Item = Result<TablePageRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_page_id == INVALID_PAGE_ID {
            return None;
        }

        let new_handle =
            match BufferPoolManager::fetch_page_handle(&self.bpm, &self.current_page_id) {
                Ok(handle) => handle,
                Err(e) => {
                    return Some(Err(Error::IO(format!(
                        "Failed to fetch page {}: {}",
                        self.current_page_id, e
                    ))));
                }
            };

        let table_page = TablePageRef::from(new_handle);

        self.current_page_id = table_page.next_page_id();

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

    use super::{PageId, TablePageIterator};

    #[test]
    fn test_table_page_iterator() -> Result<()> {
        let disk = Arc::new(RwLock::new(DiskManager::new("test.db").unwrap()));
        let replacer = Box::new(LruReplacer::new());
        let bpm = Arc::new(RwLock::new(BufferPoolManager::new(10, disk, replacer)));

        let mut table_heap = TableHeap::new(bpm.clone());

        let pages_wanted = 10;
        let mut first_page_id: Option<PageId> = None;

        loop {
            let tuple = Tuple::new(vec![1, 2, 3]);
            let rid = table_heap.insert_tuple(&tuple)?;
            if first_page_id.is_none() {
                first_page_id = Some(rid.page_id());
            }
            if rid.page_id() >= pages_wanted {
                break;
            }
        }

        let mut iter = TablePageIterator::new(&bpm, table_heap.first_page_id());

        let mut current_page_id = first_page_id.unwrap();
        while let Some(page) = iter.next() {
            assert_eq!(current_page_id, page?.page_id());
            current_page_id += 1;
        }

        Ok(())
    }
}
