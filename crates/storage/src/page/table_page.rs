use crate::{frame::PageFrame, typedef::PageId};
use bytemuck::{Pod, Zeroable};
use std::mem;

#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub(crate) struct TablePageHeader {
    pub(crate) next_page_id: PageId,
    pub(crate) tuple_cnt: u16,
    pub(crate) deleted_tuple_cnt: u16,
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

pub(crate) struct TablePage<'a> {
    page_frame: &'a mut PageFrame,
}

impl<'a> TablePage<'a> {
    const PAGE_HEADER_SIZE: usize = mem::size_of::<TablePageHeader>();
    const TUPLE_INFO_SIZE: usize = mem::size_of::<TupleInfo>();

    pub(crate) fn new(page: &'a mut PageFrame) -> Self {
        TablePage { page_frame: page }
    }

    pub(crate) fn init_header(&mut self, next_page_id: PageId) {
        let header = self.header_mut();
        *header = TablePageHeader {
            next_page_id,
            tuple_cnt: 0,
            deleted_tuple_cnt: 0,
            _padding: [0; 4],
        };
    }

    /// Immutable access to the header
    pub(crate) fn header(&self) -> &TablePageHeader {
        bytemuck::from_bytes(&self.page_frame.data()[..Self::PAGE_HEADER_SIZE])
    }

    /// Mutable access to the header
    pub(crate) fn header_mut(&mut self) -> &mut TablePageHeader {
        bytemuck::from_bytes_mut(&mut self.page_frame.data_mut()[..Self::PAGE_HEADER_SIZE])
    }

    /// Returns the slot array (immutable)
    pub(crate) fn slot_array(&self) -> &[TupleInfo] {
        let tuple_cnt = self.header().tuple_cnt as usize;
        let slots_end = Self::PAGE_HEADER_SIZE + (tuple_cnt * Self::TUPLE_INFO_SIZE);

        bytemuck::cast_slice(&self.page_frame.data()[Self::PAGE_HEADER_SIZE..slots_end])
    }

    /// Returns the slot array (mutable)
    pub(crate) fn slot_array_mut(&mut self) -> &mut [TupleInfo] {
        let tuple_cnt = self.header().tuple_cnt as usize;
        let slots_end = Self::PAGE_HEADER_SIZE + (tuple_cnt * Self::TUPLE_INFO_SIZE);

        bytemuck::cast_slice_mut(&mut self.page_frame.data_mut()[Self::PAGE_HEADER_SIZE..slots_end])
    }
}

impl<'a> From<&'a mut PageFrame> for TablePage<'a> {
    fn from(page_frame: &'a mut PageFrame) -> Self {
        TablePage { page_frame }
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer_pool::BufferPoolManager;

    use super::*;

    #[test]
    fn test_table_page() {
        let frame = &mut PageFrame::new();

        // let mut table_page = TablePage::new(&mut frame);
        let mut table_page = TablePage::from(&mut frame);

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
