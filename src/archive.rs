//! An archive of bufkit soundings.

use chrono::NaiveDateTime;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use rusqlite::{Connection, OpenFlags};
use std::fs::{create_dir, create_dir_all, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use strum::AsStaticRef;

use errors::BufkitDataErr;
use inventory::Inventory;
use models::Model;
use site::{Site, StateProv};

/// The archive.
#[derive(Debug)]
pub struct Archive {
    data_root: PathBuf,  // the directory containing the downloaded files.
    db_conn: Connection, // An sqlite connection.
}

impl Archive {
    const DATA_DIR: &'static str = "data";
    const DB_FILE: &'static str = "index.db";

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
                site          TEXT PRIMARY KEY,
                state         TEXT DEFAULT NULL,
                name          TEXT DEFAULT NULL,
                notes         TEXT DEFAULT NULL,
                auto_download INT DEFAULT 0
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
            .prepare("SELECT site,name,state,notes,auto_download FROM sites")?;

        let vals: Result<Vec<Site>, BufkitDataErr> = stmt
            .query_map(&[], |row| {
                let id = row.get(0);
                let name = row.get(1);
                let notes = row.get(3);
                let auto_download = row.get(4);
                let state: Option<StateProv> = row
                    .get_checked::<_, String>(2)
                    .ok()
                    .and_then(|a_string| StateProv::from_str(&a_string).ok());

                Site {
                    id,
                    name,
                    notes,
                    state,
                    auto_download,
                }
            })?.map(|res| res.map_err(BufkitDataErr::Database))
            .collect();

        vals
    }

    /// Retrieve the information about a single site.
    pub fn get_site_info(&self, site_id: &str) -> Result<Site, BufkitDataErr> {
        self.db_conn.query_row_and_then(
            "
                SELECT site,name,state,notes,auto_download
                FROM sites
                WHERE site = ?1
            ",
            &[&site_id.to_uppercase()],
            |row| {
                let id = row.get(0);
                let name = row.get(1);
                let notes = row.get(3);
                let auto_download = row.get(4);
                let state: Option<StateProv> = row
                    .get_checked::<_, String>(2)
                    .ok()
                    .and_then(|a_string| StateProv::from_str(&a_string).ok());

                Ok(Site {
                    id,
                    name,
                    notes,
                    state,
                    auto_download,
                })
            },
        )
    }

    /// Modify a sites values.
    pub fn set_site_info(&self, site: &Site) -> Result<(), BufkitDataErr> {
        self.db_conn.execute(
            "
                UPDATE sites 
                SET (state, name, notes, auto_download)
                = (?2, ?3, ?4, ?5)
                WHERE site = ?1
            ",
            &[
                &site.id.to_uppercase(),
                &site.state.map(|state_prov| state_prov.as_static()),
                &site.name,
                &site.notes,
                &site.auto_download,
            ],
        )?;

        Ok(())
    }

    /// Add a site to the list of sites.
    pub fn add_site(&self, site: &Site) -> Result<(), BufkitDataErr> {
        self.db_conn.execute(
            "INSERT INTO sites (site, state, name, notes, auto_download)
                  VALUES (?1, ?2, ?3, ?4, ?5)",
            &[
                &site.id.to_uppercase(),
                &site.state.map(|state_prov| state_prov.as_static()),
                &site.name,
                &site.notes,
                &site.auto_download,
            ],
        )?;

        Ok(())
    }

    /// Check if a site already exists
    pub fn site_exists(&self, site_id: &str) -> Result<bool, BufkitDataErr> {
        let number: i32 = self.db_conn.query_row(
            "SELECT COUNT(*) FROM sites WHERE site = ?1",
            &[&site_id.to_uppercase()],
            |row| row.get(0),
        )?;

        Ok(number == 1)
    }

    /// Get a list of models in the database for this site.
    pub fn models_for_site(&self, site_id: &str) -> Result<Vec<Model>, BufkitDataErr> {
        let mut stmt = self
            .db_conn
            .prepare("SELECT DISTINCT model FROM files WHERE site = ?1")?;

        let vals: Result<Vec<Model>, BufkitDataErr> = stmt
            .query_map(&[&site_id.to_uppercase()], |row| {
                let model: String = row.get(0);
                Model::from_str(&model).map_err(|_err| BufkitDataErr::InvalidModelName(model))
            })?.flat_map(|res| res.map_err(BufkitDataErr::Database).into_iter())
            .collect();

        vals
    }

    /// Add a bufkit file to the archive.
    pub fn add_file(
        &self,
        site_id: &str,
        model: Model,
        init_time: &NaiveDateTime,
        text_data: &str,
    ) -> Result<(), BufkitDataErr> {
        if !self.site_exists(site_id)? {
            self.add_site(&Site {
                id: site_id.to_owned(),
                name: None,
                notes: None,
                state: None,
                auto_download: false,
            })?;
        }

        let file_name = self.compressed_file_name(site_id, model, init_time);
        let file = File::create(self.data_root.join(&file_name))?;
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(text_data.as_bytes())?;

        self.db_conn.execute(
            "INSERT OR REPLACE INTO files (site, model, init_time, file_name)
                  VALUES (?1, ?2, ?3, ?4)",
            &[
                &site_id.to_uppercase(),
                &model.as_static(),
                init_time,
                &file_name,
            ],
        )?;

        Ok(())
    }

    /// Retrieve a file from the archive.
    pub fn get_file(
        &self,
        site_id: &str,
        model: Model,
        init_time: &NaiveDateTime,
    ) -> Result<String, BufkitDataErr> {
        let file_name: String = self.db_conn.query_row(
            "SELECT file_name FROM files WHERE site = ?1 AND model = ?2 AND init_time = ?3",
            &[&site_id.to_uppercase(), &model.as_static(), init_time],
            |row| row.get_checked(0),
        )??;

        let file = File::open(self.data_root.join(file_name))?;
        let mut decoder = GzDecoder::new(file);
        let mut s = String::new();
        decoder.read_to_string(&mut s)?;
        Ok(s)
    }

    /// Retrieve the model initialization time of the most recent model in the archive.
    pub fn get_most_recent_valid_time(
        &self,
        site_id: &str,
        model: Model,
    ) -> Result<NaiveDateTime, BufkitDataErr> {
        let init_time: NaiveDateTime = self.db_conn.query_row(
            "
                SELECT init_time FROM files 
                WHERE site = ?1 AND model = ?2
                ORDER BY init_time DESC
                LIMIT 1
            ",
            &[&site_id.to_uppercase(), &model.as_static()],
            |row| row.get_checked(0),
        )??;

        Ok(init_time)
    }

    /// Retrieve the  most recent file
    pub fn get_most_recent_file(
        &self,
        site_id: &str,
        model: Model,
    ) -> Result<String, BufkitDataErr> {
        let init_time = self.get_most_recent_valid_time(site_id, model)?;
        self.get_file(site_id, model, &init_time)
    }

    fn compressed_file_name(
        &self,
        site_id: &str,
        model: Model,
        init_time: &NaiveDateTime,
    ) -> String {
        let file_string = init_time.format("%Y%m%d%HZ").to_string();

        format!(
            "{}_{}_{}.buf.gz",
            file_string,
            model.as_static(),
            site_id.to_uppercase()
        )
    }

    /// Get the file name this would have if uncompressed.
    pub fn file_name(&self, site_id: &str, model: Model, init_time: &NaiveDateTime) -> String {
        let file_string = init_time.format("%Y%m%d%HZ").to_string();

        format!(
            "{}_{}_{}.buf",
            file_string,
            model.as_static(),
            site_id.to_uppercase()
        )
    }

    /// Check to see if a file is present in the archive and it is retrieveable.
    pub fn exists(
        &self,
        site_id: &str,
        model: Model,
        init_time: &NaiveDateTime,
    ) -> Result<bool, BufkitDataErr> {
        let num_records: i32 = self.db_conn.query_row(
            "SELECT COUNT(*) FROM files WHERE site = ?1 AND model = ?2 AND init_time = ?3",
            &[&site_id.to_uppercase(), &model.as_static(), init_time],
            |row| row.get_checked(0),
        )??;

        Ok(num_records == 1)
    }

    /// Get an inventory of soundings for a site & model.
    pub fn get_inventory(&self, site_id: &str, model: Model) -> Result<Inventory, BufkitDataErr> {
        let mut stmt = self.db_conn.prepare(
            "
                SELECT init_time FROM files 
                WHERE site = ?1 AND model = ?2
                ORDER BY init_time ASC
            ",
        )?;

        let init_times: Result<Vec<Result<NaiveDateTime, _>>, BufkitDataErr> = stmt
            .query_map(&[&site_id.to_uppercase(), &model.as_static()], |row| {
                row.get_checked(0)
            })?.map(|res| res.map_err(BufkitDataErr::Database))
            .collect();

        let init_times: Vec<NaiveDateTime> =
            init_times?.into_iter().filter_map(|res| res.ok()).collect();

        let site = &self.get_site_info(site_id)?;

        Inventory::new(init_times, model, site)
    }
}

/*--------------------------------------------------------------------------------------------------
                                          Unit Tests
--------------------------------------------------------------------------------------------------*/
#[cfg(test)]
mod unit {
    use super::*;

    use std::fs::read_dir;

    use chrono::NaiveDate;
    use tempdir::TempDir;

    use sounding_bufkit::BufkitFile;

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

    // Function to fetch a list of test files.
    fn get_test_data() -> Result<Vec<(String, Model, NaiveDateTime, String)>, BufkitDataErr> {
        let path = PathBuf::new().join("example_data");

        let files = read_dir(path)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                entry.file_type().ok().and_then(|ft| {
                    if ft.is_file() {
                        Some(entry.path())
                    } else {
                        None
                    }
                })
            });

        let mut to_return = vec![];

        for path in files {
            let bufkit_file = BufkitFile::load(&path)?;
            let anal = bufkit_file
                .data()?
                .into_iter()
                .nth(0)
                .ok_or(BufkitDataErr::NotEnoughData)?;
            let snd = anal.sounding();

            let model = if path.to_string_lossy().to_string().contains("gfs") {
                Model::GFS
            } else {
                Model::NAM
            };
            let site = if path.to_string_lossy().to_string().contains("kmso") {
                "kmso"
            } else {
                panic!("Unprepared for this test data!");
            };

            let init_time = snd.get_valid_time().expect("NO VALID TIME?!");
            let raw_string = bufkit_file.raw_text();

            to_return.push((site.to_owned(), model, init_time, raw_string.to_owned()))
        }

        Ok(to_return)
    }

    // Function to fill the archive with some example data.
    fn fill_test_archive(arch: &mut Archive) -> Result<(), BufkitDataErr> {
        let test_data = get_test_data().expect("Error loading test data.");

        for (site, model, init_time, raw_data) in test_data {
            arch.add_file(&site, model, &init_time, &raw_data)?;
        }
        Ok(())
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
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = &[
            Site {
                id: "kord".to_uppercase(),
                name: Some("Chicago/O'Hare".to_owned()),
                notes: Some("Major air travel hub.".to_owned()),
                state: Some(StateProv::IL),
                auto_download: false,
            },
            Site {
                id: "ksea".to_uppercase(),
                name: Some("Seattle".to_owned()),
                notes: Some("A coastal city with coffe and rain".to_owned()),
                state: Some(StateProv::WA),
                auto_download: true,
            },
            Site {
                id: "kmso".to_uppercase(),
                name: Some("Missoula".to_owned()),
                notes: Some("In a valley.".to_owned()),
                state: None,
                auto_download: true,
            },
        ];

        for site in test_sites {
            arch.add_site(site).expect("Error adding site.");
        }

        assert!(arch.site_exists("ksea").expect("Error checking existence"));
        assert!(arch.site_exists("kord").expect("Error checking existence"));
        assert!(!arch.site_exists("xyz").expect("Error checking existence"));

        let retrieved_sites = arch.get_sites().expect("Error retrieving sites.");

        for site in retrieved_sites {
            println!("{:#?}", site);
            assert!(test_sites.iter().find(|st| **st == site).is_some());
        }
    }

    #[test]
    fn test_get_site_info() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = &[
            Site {
                id: "kord".to_uppercase(),
                name: Some("Chicago/O'Hare".to_owned()),
                notes: Some("Major air travel hub.".to_owned()),
                state: Some(StateProv::IL),
                auto_download: false,
            },
            Site {
                id: "ksea".to_uppercase(),
                name: Some("Seattle".to_owned()),
                notes: Some("A coastal city with coffe and rain".to_owned()),
                state: Some(StateProv::WA),
                auto_download: true,
            },
            Site {
                id: "kmso".to_uppercase(),
                name: Some("Missoula".to_owned()),
                notes: Some("In a valley.".to_owned()),
                state: None,
                auto_download: true,
            },
        ];

        for site in test_sites {
            arch.add_site(site).expect("Error adding site.");
        }

        assert_eq!(arch.get_site_info("ksea").unwrap(), test_sites[1]);
    }

    #[test]
    fn test_set_site_info() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = &[
            Site {
                id: "kord".to_uppercase(),
                name: Some("Chicago/O'Hare".to_owned()),
                notes: Some("Major air travel hub.".to_owned()),
                state: Some(StateProv::IL),
                auto_download: false,
            },
            Site {
                id: "ksea".to_uppercase(),
                name: Some("Seattle".to_owned()),
                notes: Some("A coastal city with coffe and rain".to_owned()),
                state: Some(StateProv::WA),
                auto_download: true,
            },
            Site {
                id: "kmso".to_uppercase(),
                name: Some("Missoula".to_owned()),
                notes: Some("A coastal city with coffe and rain".to_owned()),
                state: None,
                auto_download: false,
            },
        ];

        for site in test_sites {
            arch.add_site(site).expect("Error adding site.");
        }

        let zootown = Site {
            id: "kmso".to_uppercase(),
            name: Some("Zootown".to_owned()),
            notes: Some("Mountains, not coast.".to_owned()),
            state: None,
            auto_download: true,
        };

        arch.set_site_info(&zootown).expect("Error updating site.");

        assert_eq!(arch.get_site_info("kmso").unwrap(), zootown);
        assert_ne!(arch.get_site_info("kmso").unwrap(), test_sites[2]);
    }

    #[test]
    fn test_files_round_trip() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_data = get_test_data().expect("Error loading test data.");

        for (site, model, init_time, raw_data) in test_data {
            arch.add_file(&site, model, &init_time, &raw_data)
                .expect("Failure to add.");
            let recovered_str = arch
                .get_file(&site, model, &init_time)
                .expect("Failure to load.");

            assert!(raw_data == recovered_str);
        }
    }

    #[test]
    fn test_get_most_recent_file() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let init_time = arch
            .get_most_recent_valid_time("kmso", Model::GFS)
            .expect("Error getting valid time.");

        assert_eq!(init_time, NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0));

        arch.get_most_recent_file("kmso", Model::GFS)
            .expect("Failed to retrieve sounding.");
    }

    #[test]
    fn test_exists() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        println!("Checking for files that should exist.");
        assert!(
            arch.exists(
                "kmso",
                Model::GFS,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0)
            ).expect("Error checking for existence")
        );
        assert!(
            arch.exists(
                "kmso",
                Model::GFS,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(6, 0, 0)
            ).expect("Error checking for existence")
        );
        assert!(
            arch.exists(
                "kmso",
                Model::GFS,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(12, 0, 0)
            ).expect("Error checking for existence")
        );
        assert!(
            arch.exists(
                "kmso",
                Model::GFS,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0)
            ).expect("Error checking for existence")
        );

        println!("Checking for files that should NOT exist.");
        assert!(
            !arch
                .exists(
                    "kmso",
                    Model::GFS,
                    &NaiveDate::from_ymd(2018, 4, 1).and_hms(0, 0, 0)
                ).expect("Error checking for existence")
        );
        assert!(
            !arch
                .exists(
                    "kmso",
                    Model::GFS,
                    &NaiveDate::from_ymd(2018, 4, 1).and_hms(6, 0, 0)
                ).expect("Error checking for existence")
        );
        assert!(
            !arch
                .exists(
                    "kmso",
                    Model::GFS,
                    &NaiveDate::from_ymd(2018, 4, 1).and_hms(12, 0, 0)
                ).expect("Error checking for existence")
        );
        assert!(
            !arch
                .exists(
                    "kmso",
                    Model::GFS,
                    &NaiveDate::from_ymd(2018, 4, 1).and_hms(18, 0, 0)
                ).expect("Error checking for existence")
        );
    }

    #[test]
    fn test_models_for_site() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let models = arch
            .models_for_site("kmso")
            .expect("Error querying archive.");

        assert!(models.contains(&Model::GFS));
        assert!(models.contains(&Model::NAM));
        assert!(!models.contains(&Model::NAM4KM));
        assert!(!models.contains(&Model::LocalWrf));
        assert!(!models.contains(&Model::Other));
    }

    #[test]
    fn test_inventory() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let first = NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0);
        let last = NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0);
        let missing = vec![(
            NaiveDate::from_ymd(2017, 4, 1).and_hms(6, 0, 0),
            NaiveDate::from_ymd(2017, 4, 1).and_hms(6, 0, 0),
        )];

        let expected = Inventory {
            first,
            last,
            missing,
            auto_download: false, // this is the default value
        };
        assert_eq!(arch.get_inventory("kmso", Model::NAM).unwrap(), expected);
    }
}
