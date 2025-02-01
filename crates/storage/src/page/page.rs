use crate::typedef::PageId;

const PAGE_SIZE: usize = 4096;
const INVALID_PAGE_ID: PageId = PageId::MAX;

pub(crate) struct Page {
    pub(crate) page_id: PageId,
    pub(crate) is_dirty: bool,
    pub(crate) pin_cnt: u16,
    data: [u8; PAGE_SIZE],
}

impl Page {
    pub(crate) fn new() -> Self {
        Self {
            page_id: INVALID_PAGE_ID,
            is_dirty: false,
            pin_cnt: 0,
            data: [0; PAGE_SIZE],
        }
    }

    /// Reads data from the page.
    pub(crate) fn read(&self, offset: usize, size: usize) -> &[u8] {
        if offset + size > PAGE_SIZE {
            panic!("Read out of bounds");
        }
        &self.data[offset..offset + size]
    }

    /// Writes data to the page.
    pub(crate) fn write(&mut self, offset: usize, data: &[u8]) {
        if offset + data.len() > PAGE_SIZE {
            panic!("Write out of bounds");
        }
        self.data[offset..offset + data.len()].copy_from_slice(data);
    }

    /// Immutable access to data
    pub(crate) fn data(&self) -> &[u8] {
        &self.data
    }

    /// Mutable access to data
    pub(crate) fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}
