use crate::Result;
use crate::{frame::PageFrame, typedef::PageId};
use bytemuck::{Pod, Zeroable};
use rustdb_error::Error;
use std::mem;

#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub(crate) struct TablePageHeader {
    next_page_id: PageId,
    tuple_cnt: u16,
    deleted_tuple_cnt: u16,
    _padding: [u8; 4],
}

#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub(crate) struct TupleInfo {
    offset: u16,
    size_bytes: u16,
    metadata: TupleMetadata,
}

#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub(crate) struct TupleMetadata {
    is_deleted: u8,
    is_null: u8,
}

const PAGE_HEADER_SIZE: usize = mem::size_of::<TablePageHeader>();
const TUPLE_INFO_SIZE: usize = mem::size_of::<TupleInfo>();

impl TupleMetadata {
    pub(crate) fn is_deleted(&self) -> bool {
        self.is_deleted != 0
    }

    pub(crate) fn set_deleted(&mut self, deleted: bool) {
        self.is_deleted = deleted as u8;
    }

    pub(crate) fn is_null(&self) -> bool {
        self.is_null != 0
    }

    pub(crate) fn set_null(&mut self, is_null: bool) {
        self.is_null = is_null as u8;
    }
}

/// Generic struct for both mutable and immutable table pages.
pub(crate) struct TablePage<T> {
    page_frame: T,
}

impl<T: AsRef<PageFrame>> TablePage<T> {
    pub(crate) fn page_id(&self) -> PageId {
        self.page_frame.as_ref().page_id()
    }

    pub(crate) fn next_page_id(&self) -> PageId {
        self.header().next_page_id
    }

    pub(crate) fn tuple_count(&self) -> u16 {
        self.header().tuple_cnt
    }

    pub(crate) fn deleted_tuple_count(&self) -> u16 {
        self.header().deleted_tuple_cnt
    }

    /// Immutable access to the header
    pub(crate) fn header(&self) -> &TablePageHeader {
        bytemuck::from_bytes(&self.page_frame.as_ref().data()[..PAGE_HEADER_SIZE])
    }

    /// Returns the slot array (immutable)
    pub(crate) fn slot_array(&self) -> &[TupleInfo] {
        let tuple_cnt = self.header().tuple_cnt as usize;
        let slots_end = PAGE_HEADER_SIZE + (tuple_cnt * TUPLE_INFO_SIZE);
        bytemuck::cast_slice(&self.page_frame.as_ref().data()[PAGE_HEADER_SIZE..slots_end])
    }

    pub(crate) fn get_tuple(&self, rid: &RecordId) -> Result<(TupleInfo, Tuple)> {
        if rid.page_id() != self.page_id() || rid.slot_id() >= self.tuple_count() {
            return Result::from(Error::InvalidInput(rid.to_string()));
        }

        let slot_array = self.slot_array();
        let tuple_info = slot_array[rid.slot_id() as usize];

        todo!()
    }
}

impl<T: AsMut<PageFrame> + AsRef<PageFrame>> TablePage<T> {
    /// Mutable access to the header
    pub(crate) fn header_mut(&mut self) -> &mut TablePageHeader {
        bytemuck::from_bytes_mut(&mut self.page_frame.as_mut().data_mut()[..PAGE_HEADER_SIZE])
    }

    /// Returns the slot array (mutable)
    pub(crate) fn slot_array_mut(&mut self) -> &mut [TupleInfo] {
        let tuple_cnt = self.header().tuple_cnt as usize;
        let slots_end = PAGE_HEADER_SIZE + (tuple_cnt * TUPLE_INFO_SIZE);
        bytemuck::cast_slice_mut(
            &mut self.page_frame.as_mut().data_mut()[PAGE_HEADER_SIZE..slots_end],
        )
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
}

/// Type alias for immutable TablePage
pub(crate) type TablePageRef<'a> = TablePage<&'a PageFrame>;

/// Type alias for mutable TablePage
pub(crate) type TablePageMut<'a> = TablePage<&'a mut PageFrame>;

impl<'a> From<&'a PageFrame> for TablePageRef<'a> {
    fn from(page_frame: &'a PageFrame) -> Self {
        TablePage { page_frame }
    }
}

impl<'a> From<&'a mut PageFrame> for TablePageMut<'a> {
    fn from(page_frame: &'a mut PageFrame) -> Self {
        TablePage { page_frame }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use crate::{
        buffer_pool::BufferPoolManager, disk::disk_manager::DiskManager,
        replacer::lru_replacer::LruReplacer,
    };

    use super::*;

    #[test]
    fn test_table_page() {
        let frame = &mut PageFrame::new();

        let mut table_page = TablePageMut::from(frame);

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
        let disk = Arc::new(RwLock::new(DiskManager::new("test.db").unwrap()));
        let replacer = Box::new(LruReplacer::new());
        let mut bpm = BufferPoolManager::new(10, disk, replacer);

        let frame = bpm.create_page().unwrap();
        let mut table_page = TablePageMut::from(frame);

        table_page.init_header(2);
        table_page.header_mut().tuple_cnt = 5;

        // bpm.unpin_page_frame(frame, true);

        assert_eq!(1, table_page.page_id());

        let frame1 = bpm.fetch_page(1).unwrap();

        let table_page1 = TablePageRef::from(frame1);

        assert_eq!(1, table_page1.page_id());
        assert_eq!(2, table_page1.next_page_id());
        assert_eq!(5, table_page1.tuple_count());
    }
}
