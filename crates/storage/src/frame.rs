use crate::typedef::PageId;

const PAGE_SIZE: usize = 4096;
const INVALID_PAGE_ID: PageId = PageId::MAX;

pub(crate) struct PageFrame {
    page_id: PageId,
    is_dirty: bool,
    pin_cnt: u16,
    data: [u8; PAGE_SIZE],
}

impl PageFrame {
    /// Creates a new page with default values.
    pub(crate) fn new() -> Self {
        Self {
            page_id: INVALID_PAGE_ID,
            is_dirty: false,
            pin_cnt: 0,
            data: [0; PAGE_SIZE],
        }
    }

    pub(crate) fn page_id(&self) -> PageId {
        self.page_id
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    pub(crate) fn pin_count(&self) -> u16 {
        self.pin_cnt
    }

    pub(crate) fn data(&self) -> &[u8] {
        &self.data
    }

    pub(crate) fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub(crate) fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id;
    }

    pub(crate) fn set_dirty(&mut self, dirty: bool) {
        self.is_dirty = dirty;
    }

    pub(crate) fn set_pin_count(&mut self, pin_cnt: u16) {
        self.pin_cnt = pin_cnt;
    }

    pub(crate) fn increment_pin_count(&mut self) {
        self.pin_cnt += 1;
    }

    pub(crate) fn decrement_pin_count(&mut self) {
        assert!(self.pin_cnt != 0);
        self.pin_cnt -= 1;
    }

    pub(crate) fn reset(&mut self) {
        self.page_id = INVALID_PAGE_ID;
        self.pin_cnt = 0;
        self.is_dirty = false;
        self.data.fill(0);
    }

    /// Writes data to the page.
    pub(crate) fn write(&mut self, offset: usize, data: &[u8]) {
        if offset + data.len() > PAGE_SIZE {
            panic!("Write out of bounds");
        }
        self.data[offset..offset + data.len()].copy_from_slice(data);
    }
}

impl AsRef<PageFrame> for PageFrame {
    fn as_ref(&self) -> &PageFrame {
        self
    }
}

impl AsMut<PageFrame> for PageFrame {
    fn as_mut(&mut self) -> &mut PageFrame {
        self
    }
}
