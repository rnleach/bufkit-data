use super::Archive;
use crate::{errors::BufkitDataErr, models::Model, site::StationNumber};
use chrono::NaiveDateTime;
use rusqlite::ToSql;
use std::path::{Path, PathBuf};

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

    /// Export part of the archive.
    pub fn export(
        &self,
        stations: &[StationNumber],
        models: &[Model],
        start: NaiveDateTime,
        end: NaiveDateTime,
        dest: &Path,
    ) -> Result<(), BufkitDataErr> {
        let new_db = Self::create(&dest)?;
        let db_file = new_db.root.join(Archive::DB_FILE);

        let statement = &format!(
            "ATTACH '{}' AS ex;",
            db_file.to_str().ok_or(BufkitDataErr::GeneralError(
                "Unable to convert path to string".to_owned()
            ))?
        );
        self.db_conn.execute(statement, rusqlite::NO_PARAMS)?;

        let mut sites_stmt = self.db_conn.prepare(
            "
                INSERT INTO ex.sites 
                SELECT * FROM main.sites 
                WHERE main.sites.station_num = ?1
            ",
        )?;

        let mut files_stmt = self.db_conn.prepare(
            "
                INSERT INTO ex.files
                SELECT * FROM main.files 
                WHERE main.files.station_num = ?1 AND main.files.model = ?2 
                    AND main.files.init_time >= ?3 AND main.files.init_time <= ?4
            ",
        )?;

        let source_dir = self.root.join(Archive::DATA_DIR);
        let dest_dir = dest.join(Archive::DATA_DIR);
        let mut file_names_stmt = self.db_conn.prepare(
            "
                SELECT ex.files.file_name FROM ex.files
                WHERE ex.files.station_num = ?1 AND ex.files.model = ?2
                    AND ex.files.init_time >= ?3 AND ex.files.init_time <= ?4
            ",
        )?;

        for &stn in stations {
            let stn_num: u32 = stn.into();
            sites_stmt.execute(&[stn_num])?;

            for &model in models {
                files_stmt.execute(&[
                    &stn_num as &dyn ToSql,
                    &model.as_static_str(),
                    &start,
                    &end,
                ])?;

                let fnames = file_names_stmt.query_and_then(
                    &[&stn_num as &dyn ToSql, &model.as_static_str(), &start, &end],
                    |row| -> Result<String, _> { row.get(0) },
                )?;

                for fname in fnames {
                    let fname = fname?;
                    let src = source_dir.join(&fname);
                    let dest = dest_dir.join(fname);
                    std::fs::copy(src, dest)?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod unit {
    use super::*;
    use crate::archive::unit::*; // Test setup and tear down.

    #[test]
    fn test_archive_create_new() {
        assert!(create_test_archive().is_ok());
    }

    #[test]
    fn test_archive_connect() {
        let TestArchive { tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");
        drop(arch);

        assert!(Archive::connect(&tmp.path()).is_ok());
        assert!(Archive::connect(&"unlikely_directory_in_my_project").is_err());
    }

    #[test]
    fn test_get_root() {
        let TestArchive { tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let root = arch.root();
        assert_eq!(root, tmp.path());
    }
}
