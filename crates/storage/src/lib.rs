mod buffer_pool;
mod disk;
mod frame;
mod page;
mod replacer;
mod typedef;
pub type Result<T> = std::result::Result<T, rustdb_error::Error>;
