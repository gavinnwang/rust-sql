use crate::typedef::PageId;

pub(crate) mod table_page;

pub(crate) const INVALID_PAGE_ID: PageId = PageId::MAX;
pub(crate) const PAGE_SIZE: usize = 4096;
