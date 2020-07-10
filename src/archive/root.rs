use std::path::{Path, PathBuf};

use super::Archive;

use crate::errors::BufkitDataErr;

impl Archive {
    const DATA_DIR: &'static str = "data";
    const DB_FILE: &'static str = "index.db";

    /// Initialize a new archive.
    pub fn create(root: &dyn AsRef<Path>) -> Result<Self, BufkitDataErr> {
        let data_root = root.as_ref().join(Archive::DATA_DIR);
        let db_file = root.as_ref().join(Archive::DB_FILE);
        let root = root.as_ref().to_path_buf();

        std::fs::create_dir_all(&data_root)?; // The folder to store the sounding files.

        // Create and set up the archive
        let db_conn = rusqlite::Connection::open_with_flags(
            db_file,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        )?;

        db_conn.execute_batch(include_str!("root/create_index.sql"))?;

        Ok(Archive { root, db_conn })
    }

    /// Open an existing archive.
    pub fn connect(root: &dyn AsRef<Path>) -> Result<Self, BufkitDataErr> {
        let db_file = root.as_ref().join(Archive::DB_FILE);
        let root = root.as_ref().to_path_buf();

        // Create and set up the archive
        let db_conn = rusqlite::Connection::open_with_flags(
            db_file,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;

        Ok(Archive { root, db_conn })
    }

    /// Retrieve a path to the root. Allows caller to store files in the archive.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Get the directory the data files are stored in.
    pub(crate) fn data_root(&self) -> PathBuf {
        self.root.join(Archive::DATA_DIR)
    }
}
