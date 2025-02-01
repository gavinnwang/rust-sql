mod buffer_pool;
mod disk_manager;
mod frame;
mod page;
mod replacer;
mod typedef;

pub type Result<T> = std::result::Result<T, rustdb_error::Error>;
