//! An archive of bufkit soundings.

use std::fs::{create_dir, create_dir_all};
use std::io;
use std::path::{PathBuf, Path};

use chrono::NaiveDateTime;
use rusqlite::{Connection, OpenFlags};

use errors::BufkitDataErr;
use models::Model;

/// Description of a site with a sounding
#[allow(missing_docs)]
pub struct Site {
    pub lat: f64,
    pub lon: f64,
    pub elev_m: f64,
    pub id: String,
    pub name: String,
    pub notes: String,
}

/// Inventory lists first & last initialization times of the models in the database for a site &
/// model. It also contains a list of model initialization times that are missing between the first
/// and last.
#[allow(missing_docs)]
pub struct Inventory {
    pub first: NaiveDateTime,
    pub last: NaiveDateTime,
    pub missing: Vec<NaiveDateTime>,
}

/// The archive.
pub struct Archive {
    data_root: PathBuf,  // the directory containing the downloaded files.
    db_conn: Connection, // An sqlite connection.
}

impl Archive {
    const DATA_DIR: &'static str = "data";
    const DB_FILE: &'static str = "index";

    /// Initialize a new archive.
    pub fn create_new<T>(root: T) -> Result<Self, BufkitDataErr> where T: AsRef<Path>{

        let data_root = root.as_ref().join(Archive::DATA_DIR);
        let db_file = root.as_ref().join(Archive::DB_FILE);

        create_dir_all(root.as_ref())?;
        create_dir(&data_root)?; // The folder to store the sounding files.

        // Create and set up the database
        let db_conn = Connection::open_with_flags(
            db_file,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;

        db_conn.execute(
            "CREATE TABLE files (
                site      TEXT NOT NULL,
                model     TEXT NOT NULL,
                init_time TEXT NOT NULL,
                file_name TEXT NOT NULL,
                PRIMARY KEY (site, model, init_time)
            )",
            &[],
        )?;

        db_conn.execute(
            "CREATE TABLE sites (
                site        TEXT PRIMARY KEY,
                latitude    REAL DEFAULT NULL,
                longitude   REAL DEFAULT NULL,
                elevation_m REAL DEFAULT NULL,
                state       TEXT DEFAULT NULL,
                name        TEXT DEFAULT NULL,
                comments    TEXT DEFAULT NULL
            )",
            &[],
        )?;

        Ok(Archive { data_root, db_conn })
    }

    /// Open an existing archive.
    pub fn connect(root: PathBuf) -> Self {
        unimplemented!()
    }

    /// Retrieve a list of sites in the archive.
    pub fn get_sites(&self, only_with_missing: bool) -> Vec<Site> {
        unimplemented!()
    }

    /// Add a site to the list of sites.
    pub fn add_site(&mut self, site: &Site) -> Result<(), BufkitDataErr> {
        unimplemented!()
    }

    /// Add a bufkit file to the archive.
    pub fn add_file(
        &mut self,
        site_id: &str,
        model: Model,
        init_time: &NaiveDateTime,
    ) -> Result<(), BufkitDataErr> {
        unimplemented!()
    }

    /// Load a file from the archive and return its contents in a `String`.
    pub fn get_file(
        &self,
        site: &str,
        model: Model,
        init_time: &NaiveDateTime,
    ) -> Result<String, BufkitDataErr> {
        unimplemented!()
    }

    /// Check to see if a file is present in the archive.
    pub fn exists(
        &self,
        site: &str,
        model: Model,
        init_time: &NaiveDateTime,
    ) -> Result<bool, BufkitDataErr> {
        unimplemented!()
    }

    /// Get an inventory of soundings for a site & model.
    pub fn get_inventory(&self, site_id: &str, model: Model) -> Result<Inventory, BufkitDataErr> {
        unimplemented!()
    }

    //
    // TODO
    //

    // Add climate summary file and climate data cache files.
}

/// Find the default archive root. This can be passed into the `create` and `connect` methods of
/// `Archive`.
pub fn default_root() -> Result<PathBuf, BufkitDataErr> {
    let default_root = ::std::env::home_dir()
        .ok_or(io::Error::new(
            io::ErrorKind::NotFound,
            "could not find home directory",
        ))?
        .join("bufkit");

    Ok(default_root)
}

#[cfg(test)]
mod unit {
    use super::*;
    use tempdir::TempDir;

    // Function to create a new archive to test.
    fn create_test_archive() -> Result<Archive, BufkitDataErr> {
        let test_root = TempDir::new("bufkit-data-test-archive")?;

        Archive::create_new(test_root.path())
    }

    #[test]
    fn test_archive_create_new() {
        create_test_archive().unwrap();
    }
}
