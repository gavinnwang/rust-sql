#![allow(dead_code)]
pub(crate) mod buffer_pool;
pub(crate) mod disk;
pub(crate) mod frame;
pub(crate) mod frame_handle;
pub(crate) mod heap;
pub(crate) mod page;
pub(crate) mod record_id;
pub(crate) mod replacer;
pub(crate) mod tuple;
pub(crate) mod typedef;
pub type Result<T> = std::result::Result<T, rustdb_error::Error>;
