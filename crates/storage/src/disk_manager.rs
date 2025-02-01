use crate::page::PageId;
use crate::Result;
use bytes::{Bytes, BytesMut};
use rustdb_error::{errdata, Error};
use std::cell::{RefCell, RefMut};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(crate) const DATA_DIR: &str = "src/disk/data/";
const PAGE_SIZE_BYTES: usize = 4096;

const DELETED_FLAG: &'static [u8] = &[1];
const EMPTY_BUFFER: &'static [u8] = &[0; PAGE_SIZE_BYTES];

/// Handles read and write accesses to pages stored on disk. File I/O operations are synchronous.
/// Asynchronous row operations, on the other hand, should occur on the pages buffered in memory,
/// with the disk manager being protected behind a [tokio::sync::RwLock] synchronization primitive.
#[derive(Debug)]
pub struct DiskManager {
    last_allocated_pid: PageId,
    file: RefCell<std::fs::File>,
}

impl DiskManager {
    /// Creates a new disk manager for the given database file `filename`, e.g. `example.db`.
    pub(crate) fn new(filename: &str) -> Result<Self> {
        let path = Path::new(DATA_DIR).join(filename);
        let file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect(format!("Unable to create or open file {}.", path.display()).as_str());

        let mut disk_manager = Self {
            last_allocated_pid: 0,
            file: RefCell::new(file),
        };

        // Initialize the first page, potentially clearing out any garbage data.
        disk_manager.write(&0, EMPTY_BUFFER)?;

        Ok(disk_manager)
    }

    pub fn allocate_page(&mut self) -> Result<PageId> {
        self.last_allocated_pid += 1;
        let page_id = self.last_allocated_pid;

        self.write(&page_id, EMPTY_BUFFER)?;
        Ok(page_id)
    }

    #[allow(dead_code)]
    pub fn deallocate_page(&mut self, page_id: &PageId) -> Result<()> {
        let mut file = self.file.borrow_mut();
        file.seek(SeekFrom::Start(Self::calculate_offset(page_id)?))?;
        file.write_all(DELETED_FLAG)?;
        Ok(())
    }

    fn is_deleted(reader: &mut RefMut<std::fs::File>) -> Result<bool> {
        let mut buf = [0; 1];
        let current_offset = reader.stream_position()?;
        reader.read_exact(&mut buf)?;
        reader.seek(SeekFrom::Start(current_offset))?;
        Ok(buf == DELETED_FLAG)
    }

    pub(crate) fn read(&mut self, page_id: &PageId) -> Result<Option<Bytes>> {
        let mut file = self.file.borrow_mut();
        file.seek(SeekFrom::Start(Self::calculate_offset(page_id)?))?;

        if Self::is_deleted(&mut file)? {
            return Ok(None);
        }

        let bytes = {
            let mut bytes = BytesMut::zeroed(PAGE_SIZE_BYTES);
            file.read_exact(&mut bytes)?;
            bytes.freeze()
        };

        Ok(Some(bytes))
    }

    pub(crate) fn write(&mut self, page_id: &PageId, data: &[u8]) -> Result<()> {
        if data.len() > PAGE_SIZE_BYTES {
            return errdata!("Page data must fit in a page.");
        }

        let mut file = self.file.borrow_mut();
        file.seek(SeekFrom::Start(Self::calculate_offset(page_id)?))?;
        file.write_all(data)?;
        file.sync_all()?;

        Ok(())
    }

    fn calculate_offset(page_id: &PageId) -> Result<u64> {
        match (*page_id).checked_mul(PAGE_SIZE_BYTES as u64) {
            Some(value) => Ok(value as u64),
            None => Err(Error::ArithmeticOverflow),
        }
    }
}
