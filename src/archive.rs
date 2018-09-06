//! An archive of bufkit soundings.

use std::fs::{create_dir, create_dir_all};
use std::io;
use std::path::{Path, PathBuf};

use chrono::NaiveDateTime;
use rusqlite::{Connection, OpenFlags};

use errors::BufkitDataErr;
use models::Model;
use site::{Site, STATES};

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
    pub fn create_new<T>(root: T) -> Result<Self, BufkitDataErr>
    where
        T: AsRef<Path>,
    {
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
                notes       TEXT DEFAULT NULL
            )",
            &[],
        )?;

        Ok(Archive { data_root, db_conn })
    }

    /// Open an existing archive.
    pub fn connect<T>(root: T) -> Result<Self, BufkitDataErr>
    where
        T: AsRef<Path>,
    {
        let data_root = root.as_ref().join(Archive::DATA_DIR);
        let db_file = root.as_ref().join(Archive::DB_FILE);

        // Create and set up the database
        let db_conn = Connection::open_with_flags(db_file, OpenFlags::SQLITE_OPEN_READ_WRITE)?;

        Ok(Archive { data_root, db_conn })
    }

    /// Retrieve a list of sites in the archive.
    pub fn get_sites(&self) -> Result<Vec<Site>, BufkitDataErr> {
        let mut stmt = self
            .db_conn
            .prepare("SELECT site,name,state,notes,latitude,longitude, elevation_m FROM sites")?;

        let vals: Result<Vec<Site>, BufkitDataErr> = stmt
            .query_map(&[], |row| Site {
                id: row.get(0),
                name: row.get(1),
                lat: row.get(4),
                lon: row.get(5),
                elev_m: row.get(6),
                notes: row.get(3),
                state: STATES
                    .iter()
                    .find(|&&st| st == row.get::<_, String>(2))
                    .map(|st| *st),
            })?
            .map(|res| res.map_err(|err| BufkitDataErr::Database(err)))
            .collect();

        vals
    }

    /// Add a site to the list of sites.
    pub fn add_site(&mut self, site: &Site) -> Result<(), BufkitDataErr> {
        self.db_conn.execute(
            "INSERT INTO sites (site, latitude, longitude, elevation_m, state, name, notes)
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            &[
                &site.id,
                &site.lat,
                &site.lon,
                &site.elev_m,
                &site.state,
                &site.name,
                &site.notes,
            ],
        )?;

        Ok(())
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

/*--------------------------------------------------------------------------------------------------
                                          Unit Tests
--------------------------------------------------------------------------------------------------*/
#[cfg(test)]
mod unit {
    use super::*;
    use tempdir::TempDir;

    // struct to hold temporary data for tests.
    struct TestArchive {
        tmp: TempDir,
        arch: Archive,
    }

    // Function to create a new archive to test.
    fn create_test_archive() -> Result<TestArchive, BufkitDataErr> {
        let tmp = TempDir::new("bufkit-data-test-archive")?;
        let arch = Archive::create_new(tmp.path())?;

        Ok(TestArchive { tmp, arch })
    }

    #[test]
    fn test_archive_create_new() {
        assert!(create_test_archive().is_ok());
    }

    #[test]
    fn test_archive_connect() {
        let TestArchive { tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");
        drop(arch);

        assert!(Archive::connect(tmp.path()).is_ok());
        assert!(Archive::connect("unlikely_directory_in_my_project").is_err());
    }

    #[test]
    fn test_sites_round_trip() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        let test_sites = &[
            Site {
                id: "kord".to_owned(),
                name: Some("Chicago/O'Hare".to_owned()),
                lat: None,
                lon: None,
                elev_m: None,
                notes: Some("Major air travel hub.".to_owned()),
                state: Some("IL"),
            },
            Site {
                id: "ksea".to_owned(),
                name: Some("Seattle".to_owned()),
                lat: None,
                lon: None,
                elev_m: None,
                notes: Some("A coastal city with coffe and rain".to_owned()),
                state: Some("WA"),
            },
        ];

        for site in test_sites {
            arch.add_site(site).expect("Error adding site.");
        }

        let retrieved_sites = arch.get_sites().expect("Error retrieving sites.");

        for site in retrieved_sites {
            assert!(test_sites.iter().find(|st| **st == site).is_some());
        }
    }
}
