use crate::typedef::PageId;

pub(crate) mod record_id;
pub(crate) mod table_page;

pub(crate) const INVALID_PAGE_ID: PageId = PageId::MAX;
