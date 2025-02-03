use crate::frame_handle::{PageFrameMutHandle, PageFrameRefHandle};
use crate::page::PAGE_SIZE;
use crate::record_id::RecordId;
use crate::tuple::Tuple;
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

pub(crate) const TABLE_PAGE_HEADER_SIZE: usize = mem::size_of::<TablePageHeader>();
pub(crate) const TUPLE_INFO_SIZE: usize = mem::size_of::<TupleInfo>();

#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub(crate) struct TupleMetadata {
    is_deleted: u8,
    _padding: [u8; 1],
}

impl TupleMetadata {
    pub fn new(is_deleted: bool) -> Self {
        Self {
            is_deleted: is_deleted as u8,
            _padding: [0; 1],
        }
    }

    pub(crate) fn is_deleted(&self) -> bool {
        self.is_deleted != 0
    }

    pub(crate) fn set_deleted(&mut self, deleted: bool) {
        self.is_deleted = deleted as u8;
    }
}

/// Generic struct for both mutable and immutable table pages.
pub(crate) struct TablePage<T> {
    page_frame_handle: T,
}

impl<T: AsRef<PageFrame>> TablePage<T> {
    pub(crate) fn page_id(&self) -> PageId {
        self.page_frame_handle.as_ref().page_id()
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
        bytemuck::from_bytes(&self.page_frame_handle.as_ref().data()[..TABLE_PAGE_HEADER_SIZE])
    }

    /// Returns the slot array (immutable)
    pub(crate) fn slot_array(&self) -> &[TupleInfo] {
        let tuple_cnt = self.header().tuple_cnt as usize;
        let slots_end = TABLE_PAGE_HEADER_SIZE + (tuple_cnt * TUPLE_INFO_SIZE);
        bytemuck::cast_slice(
            &self.page_frame_handle.as_ref().data()[TABLE_PAGE_HEADER_SIZE..slots_end],
        )
    }

    pub(crate) fn get_tuple(&self, rid: &RecordId) -> Result<(TupleMetadata, Tuple)> {
        self.validate_record_id(rid)?;

        let slot_array = self.slot_array();
        let tuple_info = slot_array[rid.slot_id() as usize];

        // // If the tuple is deleted, return an error
        // if tuple_info.metadata.is_deleted() {
        //     return Result::from(Error::InvalidInput(rid.to_string()));
        // }

        let data_offset = tuple_info.offset as usize;
        let data_size = tuple_info.size_bytes as usize;
        let page_data = self.page_frame_handle.as_ref().data();

        if data_offset + data_size > page_data.len() {
            return Result::from(Err(Error::OutOfBounds));
        }

        // copy the data in page frame to the tuple
        let tuple_data = page_data[data_offset..data_offset + data_size].to_vec();
        let tuple = Tuple::new(tuple_data);

        Ok((tuple_info.metadata, tuple))
    }

    fn get_next_tuple_offset(&mut self, tuple: &Tuple) -> Result<u16> {
        let slot_end_offset = match self.tuple_count() {
            0 => PAGE_SIZE,
            _ => {
                let slot_array = self.slot_array();
                let last_tuple_info = slot_array.last().unwrap();
                last_tuple_info.offset as usize
            }
        };

        let tuple_offset = slot_end_offset - tuple.tuple_size();

        if TABLE_PAGE_HEADER_SIZE + TUPLE_INFO_SIZE * (self.tuple_count() + 1) as usize
            > tuple_offset
        {
            return Result::from(Err(Error::OutOfBounds));
        }

        Ok(tuple_offset as u16)
    }

    fn validate_record_id(&self, rid: &RecordId) -> Result<()> {
        if rid.page_id() != self.page_id() || rid.slot_id() >= self.tuple_count() {
            Err(Error::InvalidInput(rid.to_string()))
        } else {
            Ok(())
        }
    }
}

impl<T: AsMut<PageFrame> + AsRef<PageFrame>> TablePage<T> {
    /// Mutable access to the header
    pub(crate) fn header_mut(&mut self) -> &mut TablePageHeader {
        bytemuck::from_bytes_mut(
            &mut self.page_frame_handle.as_mut().data_mut()[..TABLE_PAGE_HEADER_SIZE],
        )
    }

    /// Returns the slot array (mutable)
    pub(crate) fn slot_array_mut(&mut self) -> &mut [TupleInfo] {
        let tuple_cnt = self.header().tuple_cnt as usize;
        let slots_end = TABLE_PAGE_HEADER_SIZE + (tuple_cnt * TUPLE_INFO_SIZE);
        bytemuck::cast_slice_mut(
            &mut self.page_frame_handle.as_mut().data_mut()[TABLE_PAGE_HEADER_SIZE..slots_end],
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

    pub(crate) fn set_next_page_id(&mut self, next_page_id: PageId) {
        let header = self.header_mut();
        header.next_page_id = next_page_id;
    }

    pub(crate) fn set_tuple_count(&mut self, tuple_count: u16) {
        let header = self.header_mut();
        header.tuple_cnt = tuple_count;
    }

    pub(crate) fn set_deleted_tuple_count(&mut self, deleted_tuple_count: u16) {
        let header = self.header_mut();
        header.deleted_tuple_cnt = deleted_tuple_count;
    }

    pub(crate) fn insert_tuple(&mut self, meta: &TupleMetadata, tuple: &Tuple) -> Result<RecordId> {
        let tuple_size = tuple.tuple_size();
        let tuple_offset = self.get_next_tuple_offset(tuple)?;

        // Ensure there's enough space
        if tuple_offset as usize + tuple_size > PAGE_SIZE {
            return Result::from(Err(Error::OutOfBounds));
        }

        let tuple_count = self.header().tuple_cnt as usize;

        // Write tuple data into the page
        let page_data = self.page_frame_handle.as_mut().data_mut();
        page_data[tuple_offset as usize..tuple_offset as usize + tuple_size]
            .copy_from_slice(&tuple.data());

        // Update the slot array
        let new_slot = TupleInfo {
            offset: tuple_offset,
            size_bytes: tuple_size as u16,
            metadata: *meta,
        };

        // Extend the slot array
        let slot_start = TABLE_PAGE_HEADER_SIZE + tuple_count * TUPLE_INFO_SIZE;
        let slot_end = slot_start + TUPLE_INFO_SIZE;
        page_data[slot_start..slot_end].copy_from_slice(bytemuck::bytes_of(&new_slot));

        let header = self.header_mut();
        header.tuple_cnt += 1;

        Ok(RecordId::new(self.page_id(), tuple_count as u16))
    }

    pub(crate) fn update_tuple_metadata(
        &mut self,
        rid: &RecordId,
        metadata: TupleMetadata,
    ) -> Result<()> {
        self.validate_record_id(rid)?;

        let slot_array = self.slot_array_mut();
        let slot = &mut slot_array[rid.slot_id() as usize];

        slot.metadata = metadata;

        Ok(())
    }
}

/// Type alias for immutable TablePage
pub(crate) type TablePageRef<'a> = TablePage<PageFrameRefHandle<'a>>;
/// Type alias for mutable TablePage
pub(crate) type TablePageMut<'a> = TablePage<PageFrameMutHandle<'a>>;

impl<'a> From<PageFrameRefHandle<'a>> for TablePageRef<'a> {
    fn from(page_frame_handle: PageFrameRefHandle<'a>) -> Self {
        TablePage { page_frame_handle }
    }
}

impl<'a> From<PageFrameMutHandle<'a>> for TablePageMut<'a> {
    fn from(page_frame_handle: PageFrameMutHandle<'a>) -> Self {
        TablePage { page_frame_handle }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use crate::{
        buffer_pool::BufferPoolManager, disk::disk_manager::DiskManager, page::INVALID_PAGE_ID,
        record_id::INVALID_RECORD_ID, replacer::lru_replacer::LruReplacer,
    };

    use super::*;

    #[test]
    fn test_table_page_with_buffer_pool() {
        let disk = Arc::new(RwLock::new(DiskManager::new("test.db").unwrap()));
        let replacer = Box::new(LruReplacer::new());
        let bpm = Arc::new(RwLock::new(BufferPoolManager::new(10, disk, replacer)));

        let mut page_id = INVALID_PAGE_ID;
        {
            let frame_handle = BufferPoolManager::create_page_handle(bpm.clone()).unwrap();
            let mut table_page = TablePageMut::from(frame_handle);

            table_page.init_header(2);

            page_id = table_page.page_id();

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
            slots_mut[1].metadata.set_deleted(true);
            assert_eq!(slots_mut[0].offset, 55);
            assert_eq!(slots_mut[1].offset, 11);
            assert_eq!(slots_mut[1].metadata.is_deleted(), true);

            table_page.header_mut().tuple_cnt = 3;

            let slots = table_page.slot_array();
            assert_eq!(slots.len(), 3);
            assert_eq!(slots[0].offset, 55);
            assert_eq!(slots[1].offset, 11);
            assert_eq!(slots[1].metadata.is_deleted(), true);
        }

        let frame_handle_1 = BufferPoolManager::fetch_page_handle(bpm.clone(), page_id).unwrap();

        let table_page1 = TablePageRef::from(frame_handle_1);

        assert_eq!(1, table_page1.page_id());
        assert_eq!(2, table_page1.next_page_id());
        assert_eq!(3, table_page1.tuple_count());

        let slots = table_page1.slot_array();
        assert_eq!(slots.len(), 3);
        assert_eq!(slots[0].offset, 55);
        assert_eq!(slots[1].offset, 11);
        assert_eq!(slots[1].metadata.is_deleted(), true);
    }

    #[test]
    fn test_insert_and_get_tuple() {
        let disk = Arc::new(RwLock::new(DiskManager::new("test.db").unwrap()));
        let replacer = Box::new(LruReplacer::new());
        let mut bpm = Arc::new(RwLock::new(BufferPoolManager::new(10, disk, replacer)));

        let mut page_id = INVALID_PAGE_ID;
        let mut insert_record_id = INVALID_RECORD_ID;

        // tuple metadata
        let metadata = TupleMetadata::new(true);

        let tuple_data = vec![1, 2, 3, 1, 2, 3, 4, 5, 6, 7, 8];
        {
            let frame_handle = BufferPoolManager::create_page_handle(bpm.clone()).unwrap();
            let mut table_page = TablePageMut::from(frame_handle);

            page_id = table_page.page_id();

            // Initialize page header
            table_page.init_header(2);
            assert_eq!(table_page.header().tuple_cnt, 0);

            let tuple = Tuple::new(tuple_data.clone());

            // Insert the tuple
            let record_id = table_page.insert_tuple(&metadata, &tuple).unwrap();
            assert_eq!(table_page.tuple_count(), 1);

            insert_record_id = record_id.clone();

            // Retrieve the tuple
            let (retrieved_meta, retrieved_tuple) = table_page.get_tuple(&record_id).unwrap();

            // Ensure retrieved tuple matches inserted tuple
            assert_eq!(retrieved_meta.is_deleted(), metadata.is_deleted());
            assert_eq!(retrieved_tuple.data(), &tuple_data);
        }
        let frame_handle_1 = BufferPoolManager::fetch_page_handle(bpm.clone(), page_id).unwrap();

        let table_page1 = TablePageRef::from(frame_handle_1);
        // Retrieve the tuple
        let (retrieved_meta, retrieved_tuple) = table_page1.get_tuple(&insert_record_id).unwrap();

        // Ensure retrieved tuple matches inserted tuple
        assert_eq!(retrieved_meta.is_deleted(), metadata.is_deleted());
        assert_eq!(retrieved_tuple.data(), &tuple_data);
    }
}
