use std::mem;

use super::page::Page;
use crate::typedef::PageId;

use bytemuck::{Pod, Zeroable};

#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub(crate) struct TablePageHeader {
    // Pages are stored as a linked list
    pub(crate) next_page_id: PageId,
    // Number of non-deleted tuples
    pub(crate) tuple_cnt: u16,
    // Number of deleted tuples
    pub(crate) deleted_tuple_cnt: u16,
    // Padding to satisfy bytemuck POD trait
    _padding: [u8; 4],
}

#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub(crate) struct TupleInfo {
    pub(crate) offset: u16,
    pub(crate) size_bytes: u16,
    pub(crate) metadata: TupleMetadata,
}

#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub(crate) struct TupleMetadata {
    is_deleted: u8,
    is_null: u8,
}

impl TupleMetadata {
    pub fn is_deleted(&self) -> bool {
        self.is_deleted != 0
    }

    pub fn set_deleted(&mut self, deleted: bool) {
        self.is_deleted = deleted as u8;
    }

    pub fn is_null(&self) -> bool {
        self.is_null != 0
    }

    pub fn set_null(&mut self, is_null: bool) {
        self.is_null = is_null as u8;
    }
}

pub(crate) struct TablePage {
    page: Page,
}

impl TablePage {
    const PAGE_HEADER_SIZE: usize = mem::size_of::<TablePageHeader>();
    const TUPLE_INFO_SIZE: usize = mem::size_of::<TupleInfo>();

    pub(crate) fn init_header(&mut self, next_page_id: PageId) {
        let header = self.header_mut();
        *header = TablePageHeader {
            next_page_id,
            tuple_cnt: 0,
            deleted_tuple_cnt: 0,
            _padding: [0; 4],
        };
    }

    fn header(&self) -> &TablePageHeader {
        bytemuck::from_bytes(&self.page.data()[..Self::PAGE_HEADER_SIZE])
    }

    fn header_mut(&mut self) -> &mut TablePageHeader {
        bytemuck::from_bytes_mut(&mut self.page.data_mut()[..Self::PAGE_HEADER_SIZE])
    }

    pub(crate) fn slot_array(&self) -> &[TupleInfo] {
        let tuple_cnt = self.header().tuple_cnt as usize;
        let slots_end = Self::PAGE_HEADER_SIZE + (tuple_cnt * Self::TUPLE_INFO_SIZE);

        bytemuck::cast_slice(&self.page.data()[Self::PAGE_HEADER_SIZE..slots_end])
    }

    pub(crate) fn slot_array_mut(&mut self) -> &mut [TupleInfo] {
        let tuple_cnt = self.header().tuple_cnt as usize;
        let slots_end = Self::PAGE_HEADER_SIZE + (tuple_cnt * Self::TUPLE_INFO_SIZE);

        bytemuck::cast_slice_mut(&mut self.page.data_mut()[Self::PAGE_HEADER_SIZE..slots_end])
    }
}

impl From<Page> for TablePage {
    fn from(page: Page) -> Self {
        TablePage { page }
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer_pool::BufferPoolManager;

    use super::*;

    #[test]
    fn test_table_page() {
        let page = Page::new();

        let mut table_page = TablePage::from(page);

        table_page.init_header(2);

        let header = table_page.header();
        assert_eq!(header.next_page_id, 2);
        assert_eq!(header.tuple_cnt, 0);
        assert_eq!(header.deleted_tuple_cnt, 0);

        table_page.header_mut().tuple_cnt = 5;

        let updated_header = table_page.header();
        assert_eq!(updated_header.tuple_cnt, 5);

        let slots = table_page.slot_array();
        assert_eq!(slots.len(), 5);

        let slots_mut = table_page.slot_array_mut();
        slots_mut[0].offset = 55;
        slots_mut[1].offset = 11;
        slots_mut[1].metadata.set_null(true);
        assert_eq!(slots_mut[0].offset, 55);
        assert_eq!(slots_mut[1].offset, 11);
        assert_eq!(slots_mut[1].metadata.is_null(), true);

        table_page.header_mut().tuple_cnt = 3;

        let slots = table_page.slot_array();
        assert_eq!(slots.len(), 3);
        assert_eq!(slots[0].offset, 55);
        assert_eq!(slots[1].offset, 11);
        assert_eq!(slots[1].metadata.is_null(), true);
    }

    #[test]
    fn test_table_page_with_buffer_pool() {
        let bpm = BufferPoolManager::new(10);
    }
}
