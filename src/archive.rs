//! An archive of bufkit soundings.

use chrono::{FixedOffset, NaiveDate, NaiveDateTime};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use rusqlite::{types::ToSql, Connection, OpenFlags, Row, NO_PARAMS};
use sounding_bufkit::BufkitData;
use std::{
    collections::HashSet,
    fs::{create_dir_all, read_dir, remove_file, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::mpsc::{channel, Receiver},
    thread::{self, JoinHandle},
};
use strum::AsStaticRef;

use crate::errors::BufkitDataErr;
use crate::inventory::Inventory;
use crate::models::Model;
use crate::site::{Site, StateProv};

/// The archive.
#[derive(Debug)]
pub struct Archive {
    root: PathBuf,       // The root directory.
    data_root: PathBuf,  // the directory containing the downloaded files.
    db_conn: Connection, // An sqlite connection.
}

impl Archive {
    // ---------------------------------------------------------------------------------------------
    // Connecting, creating, and maintaining the archive.
    // ---------------------------------------------------------------------------------------------

    /// Initialize a new archive.
    pub fn create<T>(root: T) -> Result<Self, BufkitDataErr>
    where
        T: AsRef<Path>,
    {
        let data_root = root.as_ref().join(Archive::DATA_DIR);
        let db_file = root.as_ref().join(Archive::DB_FILE);
        let root = root.as_ref().to_path_buf();

        create_dir_all(&data_root)?; // The folder to store the sounding files.

        // Create and set up the archive
        let db_conn = Connection::open_with_flags(
            db_file,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;

        db_conn.execute_batch(include_str!("create_index.sql"))?;

        Ok(Archive {
            root,
            data_root,
            db_conn,
        })
    }

    /// Open an existing archive.
    pub fn connect<T>(root: T) -> Result<Self, BufkitDataErr>
    where
        T: AsRef<Path>,
    {
        let data_root = root.as_ref().join(Archive::DATA_DIR);
        let db_file = root.as_ref().join(Archive::DB_FILE);
        let root = root.as_ref().to_path_buf();

        // Create and set up the archive
        let db_conn = Connection::open_with_flags(db_file, OpenFlags::SQLITE_OPEN_READ_WRITE)?;

        Ok(Archive {
            root,
            data_root,
            db_conn,
        })
    }

    /// Validate files listed in the index are in the archive too, if not remove them from the
    /// index.
    pub fn clean(
        &self,
    ) -> Result<(JoinHandle<Result<(), BufkitDataErr>>, Receiver<String>), BufkitDataErr> {
        let (sender, receiver) = channel::<String>();
        let root = self.root.clone();

        let jh = thread::spawn(move || -> Result<(), BufkitDataErr> {
            let arch = Archive::connect(root)?;

            arch.db_conn.execute("PRAGMA cache_size=10000", NO_PARAMS)?;

            let mut del_stmt = arch
                .db_conn
                .prepare("DELETE FROM files WHERE file_name = ?1")?;
            let mut insert_stmt = arch.db_conn.prepare(
                "
                    INSERT INTO files (site, model, init_time, end_time, file_name)
                    VALUES (?1, ?2, ?3, ?4, ?5)
                ",
            )?;
            let mut all_files_stmt = arch.db_conn.prepare("SELECT file_name FROM files")?;

            sender
                .send("Building set of files from the index.".to_string())
                .map_err(BufkitDataErr::SenderError)?;

            let index_vals: Result<HashSet<String>, BufkitDataErr> = all_files_stmt
                .query_map(NO_PARAMS, |row| row.get::<_, String>(0))?
                .map(|res| res.map_err(BufkitDataErr::Database))
                .collect();
            let index_vals = index_vals?;

            sender
                .send("Building set of files from the file system.".to_string())
                .map_err(BufkitDataErr::SenderError)?;

            let file_system_vals: HashSet<String> = read_dir(&arch.data_root)?
                .filter_map(Result::ok)
                .map(|de| de.path())
                .filter(|p| p.is_file())
                .filter_map(|p| p.file_name().map(ToOwned::to_owned))
                .map(|p| p.to_string_lossy().to_string())
                .collect();

            sender
                .send("Comparing sets for files in index but not in the archive.".to_string())
                .map_err(BufkitDataErr::SenderError)?;
            let files_in_index_but_not_on_file_system = index_vals.difference(&file_system_vals);

            arch.db_conn.execute("BEGIN TRANSACTION", NO_PARAMS)?;
            for missing_file in files_in_index_but_not_on_file_system {
                del_stmt.execute(&[missing_file])?;
                sender
                    .send(format!("Removing {} from index.", missing_file))
                    .map_err(BufkitDataErr::SenderError)?;
            }
            arch.db_conn.execute("COMMIT TRANSACTION", NO_PARAMS)?;

            sender
                .send("Comparing sets for files in archive but not in the index.".to_string())
                .map_err(BufkitDataErr::SenderError)?;
            let files_not_in_index = file_system_vals.difference(&index_vals);

            arch.db_conn.execute("BEGIN TRANSACTION", NO_PARAMS)?;
            for extra_file in files_not_in_index {
                let message = if let Some((init_time, end_time, model, site)) =
                    arch.parse_compressed_file(&extra_file)
                {
                    if !arch.site_exists(&site)? {
                        arch.add_site(&Site {
                            id: site.clone(),
                            state: None,
                            name: None,
                            notes: None,
                            auto_download: false,
                            time_zone: None,
                        })?;
                    }
                    match insert_stmt.execute(&[
                        &site.to_uppercase() as &dyn ToSql,
                        &model.as_static() as &dyn ToSql,
                        &init_time as &dyn ToSql,
                        &end_time as &dyn ToSql,
                        &extra_file,
                    ]) {
                        Ok(_) => format!("Added {}", extra_file),
                        Err(_) => {
                            remove_file(arch.data_root.join(extra_file))?;
                            format!("Duplicate file removed: {}", extra_file)
                        }
                    }
                } else {
                    // Remove non-bufkit file
                    remove_file(arch.data_root.join(extra_file))?;
                    format!("Removed non-bufkit file: {}", extra_file)
                };

                sender.send(message).map_err(BufkitDataErr::SenderError)?;
            }
            arch.db_conn.execute("COMMIT TRANSACTION", NO_PARAMS)?;

            sender
                .send("Compressing index.".to_string())
                .map_err(BufkitDataErr::SenderError)?;
            arch.db_conn.execute("VACUUM", NO_PARAMS)?;

            Ok(())
        });

        Ok((jh, receiver))
    }

    // ---------------------------------------------------------------------------------------------
    // The file system aspects of the archive, e.g. the root directory of the archive
    // ---------------------------------------------------------------------------------------------
    const DATA_DIR: &'static str = "data";
    const DB_FILE: &'static str = "index.sqlite";

    /// Retrieve a path to the root. Allows caller to store files in the archive.
    pub fn root(&self) -> &Path {
        &self.root
    }

    // ---------------------------------------------------------------------------------------------
    // Query or modify site metadata
    // ---------------------------------------------------------------------------------------------

    fn parse_row_to_site(row: &Row) -> Result<Site, rusqlite::Error> {
        let id = row.get(0)?;
        let name = row.get(1)?;
        let notes = row.get(3)?;
        let auto_download = row.get(4)?;
        let state: Option<StateProv> = row
            .get::<_, String>(2)
            .ok()
            .and_then(|a_string| StateProv::from_str(&a_string).ok());

        let time_zone: Option<FixedOffset> = row.get::<_, i32>(5).ok().map(|offset: i32| {
            if offset < 0 {
                FixedOffset::west(offset.abs())
            } else {
                FixedOffset::east(offset)
            }
        });

        Ok(Site {
            id,
            name,
            notes,
            state,
            auto_download,
            time_zone,
        })
    }

    /// Retrieve a list of sites in the archive.
    pub fn sites(&self) -> Result<Vec<Site>, BufkitDataErr> {
        let mut stmt = self
            .db_conn
            .prepare("SELECT site,name,state,notes,auto_download,tz_offset_sec FROM sites")?;

        let vals: Result<Vec<Site>, BufkitDataErr> = stmt
            .query_and_then(NO_PARAMS, Self::parse_row_to_site)?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .collect();

        vals
    }

    /// Retrieve the information about a single site.
    pub fn site_info(&self, site_id: &str) -> Result<Site, BufkitDataErr> {
        self.db_conn
            .query_row_and_then(
                "
                SELECT site,name,state,notes,auto_download,tz_offset_sec
                FROM sites
                WHERE site = ?1
            ",
                &[&site_id.to_uppercase()],
                Self::parse_row_to_site,
            )
            .map_err(BufkitDataErr::Database)
    }

    /// Modify a sites values.
    pub fn set_site_info(&self, site: &Site) -> Result<(), BufkitDataErr> {
        self.db_conn.execute(
            "
                UPDATE sites
                SET (state,name,notes,auto_download,tz_offset_sec)
                = (?2, ?3, ?4, ?5, ?6)
                WHERE site = ?1
            ",
            &[
                &site.id.to_uppercase(),
                &site.state.map(|state_prov| state_prov.as_static()) as &dyn ToSql,
                &site.name,
                &site.notes,
                &site.auto_download,
                &site.time_zone.map(|tz| tz.local_minus_utc()),
            ],
        )?;

        Ok(())
    }

    /// Add a site to the list of sites.
    pub fn add_site(&self, site: &Site) -> Result<(), BufkitDataErr> {
        self.db_conn.execute(
            "INSERT INTO sites (site, state, name, notes, auto_download, tz_offset_sec)
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            &[
                &site.id.to_uppercase(),
                &site.state.map(|state_prov| state_prov.as_static()) as &dyn ToSql,
                &site.name,
                &site.notes,
                &site.auto_download,
                &site.time_zone.map(|tz| tz.local_minus_utc()),
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

    // ---------------------------------------------------------------------------------------------
    // Query archive inventory
    // ---------------------------------------------------------------------------------------------

    /// Get a list of all the available model initialization times for a given site and model.
    pub fn init_times(
        &self,
        site_id: &str,
        model: Model,
    ) -> Result<Vec<NaiveDateTime>, BufkitDataErr> {
        let mut stmt = self.db_conn.prepare(
            "
                SELECT init_time FROM files
                WHERE site = ?1 AND model = ?2
                ORDER BY init_time ASC
            ",
        )?;

        let init_times: Vec<Result<NaiveDateTime, _>> = stmt
            .query_map(&[&site_id.to_uppercase(), model.as_static()], |row| {
                row.get::<_, NaiveDateTime>(0)
            })?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .collect();

        let init_times: Vec<NaiveDateTime> =
            init_times.into_iter().filter_map(Result::ok).collect();

        Ok(init_times)
    }

    /// Get the number of values files in the archive for the model and intitialization time.
    pub fn count_init_times(&self, site_id: &str, model: Model) -> Result<i64, BufkitDataErr> {
        let num_records: i64 = self.db_conn.query_row(
            "
                SELECT COUNT(init_time) FROM files
                WHERE site = ?1 AND model = ?2
            ",
            &[&site_id.to_uppercase(), model.as_static()],
            |row| row.get(0),
        )?;

        Ok(num_records)
    }

    /// Get an inventory of soundings for a site & model.
    pub fn inventory(&self, site_id: &str, model: Model) -> Result<Inventory, BufkitDataErr> {
        let init_times = self.init_times(site_id, model)?;

        let site = &self.site_info(site_id)?;

        Inventory::new(init_times, model, site)
    }

    /// Get a list of models in the archive for this site.
    pub fn models(&self, site_id: &str) -> Result<Vec<Model>, BufkitDataErr> {
        let mut stmt = self
            .db_conn
            .prepare("SELECT DISTINCT model FROM files WHERE site = ?1")?;

        let vals: Result<Vec<Model>, BufkitDataErr> = stmt
            .query_map(&[&site_id.to_uppercase()], |row| row.get::<_, String>(0))?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .map(|res| {
                res.and_then(|name| Model::from_str(&name).map_err(BufkitDataErr::StrumError))
            })
            .collect();

        vals
    }

    /// Retrieve the model initialization time of the most recent model in the archive.
    pub fn most_recent_init_time(
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
            &[&site_id.to_uppercase(), model.as_static()],
            |row| row.get(0),
        )?;

        Ok(init_time)
    }

    /// Retrieve all the initialization times of all sounding files that have a sounding with a
    /// valid time in the specified range (inclusive).
    pub fn init_times_for_soundings_valid_between(
        &self,
        start: NaiveDateTime,
        end: NaiveDateTime,
        site_id: &str,
        model: Model,
    ) -> Result<Vec<NaiveDateTime>, BufkitDataErr> {
        let mut stmt = self.db_conn.prepare(
            "
                SELECT init_time
                FROM files
                WHERE site = ?1 AND model = ?2 AND init_time <= ?4 AND end_time >= ?3
                ORDER BY init_time ASC
            ",
        )?;

        let init_times: Result<Vec<NaiveDateTime>, _> = stmt
            .query_map(
                &[
                    &site_id.to_uppercase() as &dyn ToSql,
                    &model.as_static() as &dyn ToSql,
                    &start as &dyn ToSql,
                    &end as &dyn ToSql,
                ],
                |row| row.get::<_, NaiveDateTime>(0),
            )?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .collect();

        init_times
    }

    /// Check to see if a file is present in the archive and it is retrieveable.
    pub fn file_exists(
        &self,
        site_id: &str,
        model: Model,
        init_time: &NaiveDateTime,
    ) -> Result<bool, BufkitDataErr> {
        let num_records: i32 = self.db_conn.query_row(
            "SELECT COUNT(*) FROM files WHERE site = ?1 AND model = ?2 AND init_time = ?3",
            &[
                &site_id.to_uppercase() as &dyn ToSql,
                &model.as_static() as &dyn ToSql,
                init_time as &dyn ToSql,
            ],
            |row| row.get(0),
        )?;

        Ok(num_records == 1)
    }

    /// Get the number of files stored in the archive.
    pub fn count(&self) -> Result<i64, BufkitDataErr> {
        let num_records: i64 =
            self.db_conn
                .query_row("SELECT COUNT(*) FROM files", NO_PARAMS, |row| row.get(0))?;

        Ok(num_records)
    }

    // ---------------------------------------------------------------------------------------------
    // Add, remove, and retrieve files from the archive
    // ---------------------------------------------------------------------------------------------

    /// Add a bufkit file to the archive.
    pub fn add(
        &self,
        site_id: &str,
        model: Model,
        init_time: NaiveDateTime,
        end_time: NaiveDateTime,
        text_data: &str,
    ) -> Result<(), BufkitDataErr> {
        if !self.site_exists(site_id)? {
            self.add_site(&Site {
                id: site_id.to_owned(),
                name: None,
                notes: None,
                state: None,
                auto_download: false,
                time_zone: None,
            })?;
        }

        let file_name = self.compressed_file_name(site_id, model, init_time);
        let file = File::create(self.data_root.join(&file_name))?;
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(text_data.as_bytes())?;

        self.db_conn.execute(
            "INSERT OR REPLACE INTO files (site, model, init_time, end_time, file_name)
                  VALUES (?1, ?2, ?3, ?4, ?5)",
            &[
                &site_id.to_uppercase() as &dyn ToSql,
                &model.as_static() as &dyn ToSql,
                &init_time as &dyn ToSql,
                &end_time,
                &file_name,
            ],
        )?;

        Ok(())
    }

    /// Retrieve a file from the archive.
    pub fn retrieve(
        &self,
        site_id: &str,
        model: Model,
        init_time: NaiveDateTime,
    ) -> Result<String, BufkitDataErr> {
        let file_name: String = self.db_conn.query_row(
            "SELECT file_name FROM files WHERE site = ?1 AND model = ?2 AND init_time = ?3",
            &[
                &site_id.to_uppercase() as &dyn ToSql,
                &model.as_static() as &dyn ToSql,
                &init_time as &dyn ToSql,
            ],
            |row| row.get(0),
        )?;

        let file = File::open(self.data_root.join(file_name))?;
        let mut decoder = GzDecoder::new(file);
        let mut s = String::new();
        decoder.read_to_string(&mut s)?;
        Ok(s)
    }

    /// Retrieve the  most recent file
    pub fn most_recent_file(&self, site_id: &str, model: Model) -> Result<String, BufkitDataErr> {
        let init_time = self.most_recent_init_time(site_id, model)?;
        self.retrieve(site_id, model, init_time)
    }

    /// Retrieve all the soundings with data valid between the start and end times.
    pub fn retrieve_all_valid_in(
        &self,
        start: NaiveDateTime,
        end: NaiveDateTime,
        site_id: &str,
        model: Model,
    ) -> Result<Vec<String>, BufkitDataErr> {
        let init_times = self.init_times_for_soundings_valid_between(start, end, site_id, model)?;

        let string_data: Result<Vec<String>, _> = init_times
            .into_iter()
            .map(|init_t| self.retrieve(site_id, model, init_t))
            .collect();

        string_data
    }

    fn compressed_file_name(
        &self,
        site_id: &str,
        model: Model,
        init_time: NaiveDateTime,
    ) -> String {
        let file_string = init_time.format("%Y%m%d%HZ").to_string();

        format!(
            "{}_{}_{}.buf.gz",
            file_string,
            model.as_static(),
            site_id.to_uppercase()
        )
    }

    fn parse_compressed_file(
        &self,
        fname: &str,
    ) -> Option<(NaiveDateTime, NaiveDateTime, Model, String)> {
        let tokens: Vec<&str> = fname.split(|c| c == '_' || c == '.').collect();

        if tokens.len() != 5 {
            return None;
        }

        let year = tokens[0][0..4].parse::<i32>().ok()?;
        let month = tokens[0][4..6].parse::<u32>().ok()?;
        let day = tokens[0][6..8].parse::<u32>().ok()?;
        let hour = tokens[0][8..10].parse::<u32>().ok()?;
        let init_time = NaiveDate::from_ymd(year, month, day).and_hms(hour, 0, 0);

        let model = Model::from_str(tokens[1]).ok()?;

        let site = tokens[2].to_owned();

        if tokens[3] != "buf" || tokens[4] != "gz" {
            return None;
        }

        let file = File::open(self.data_root.join(fname)).ok()?;
        let mut decoder = GzDecoder::new(file);
        let mut s = String::new();
        decoder.read_to_string(&mut s).ok()?;

        let snd = BufkitData::init(&s, fname).ok()?.into_iter().last()?.0;
        let end_time = snd.valid_time()?;

        Some((init_time, end_time, model, site))
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

    /// Remove a file from the archive.
    pub fn remove(
        &self,
        site_id: &str,
        model: Model,
        init_time: &NaiveDateTime,
    ) -> Result<(), BufkitDataErr> {
        let file_name: String = self.db_conn.query_row(
            "SELECT file_name FROM files WHERE site = ?1 AND model = ?2 AND init_time = ?3",
            &[
                &site_id.to_uppercase() as &dyn ToSql,
                &model.as_static() as &dyn ToSql,
                init_time as &dyn ToSql,
            ],
            |row| row.get(0),
        )?;

        remove_file(self.data_root.join(file_name)).map_err(BufkitDataErr::IO)?;

        self.db_conn.execute(
            "DELETE FROM files WHERE site = ?1 AND model = ?2 AND init_time = ?3",
            &[
                &site_id.to_uppercase() as &dyn ToSql,
                &model.as_static() as &dyn ToSql,
                init_time as &dyn ToSql,
            ],
        )?;

        Ok(())
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
        let arch = Archive::create(tmp.path())?;

        Ok(TestArchive { tmp, arch })
    }

    // Function to fetch a list of test files.
    fn get_test_data(
    ) -> Result<Vec<(String, Model, NaiveDateTime, NaiveDateTime, String)>, BufkitDataErr> {
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
            let (snd, _) = bufkit_file
                .data()?
                .into_iter()
                .nth(0)
                .ok_or(BufkitDataErr::NotEnoughData)?;

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

            let init_time = snd.valid_time().expect("NO VALID TIME?!");

            let (snd, _) = bufkit_file
                .data()?
                .into_iter()
                .last()
                .ok_or(BufkitDataErr::NotEnoughData)?;
            let end_time = snd.valid_time().expect("NO VALID TIME?!");

            let raw_string = bufkit_file.raw_text();

            to_return.push((
                site.to_owned(),
                model,
                init_time,
                end_time,
                raw_string.to_owned(),
            ))
        }

        Ok(to_return)
    }

    // Function to fill the archive with some example data.
    fn fill_test_archive(arch: &mut Archive) -> Result<(), BufkitDataErr> {
        let test_data = get_test_data().expect("Error loading test data.");

        for (site, model, init_time, end_time, raw_data) in test_data {
            arch.add(&site, model, init_time, end_time, &raw_data)?;
        }
        Ok(())
    }

    // ---------------------------------------------------------------------------------------------
    // Connecting, creating, and maintaining the archive.
    // ---------------------------------------------------------------------------------------------
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

    // ---------------------------------------------------------------------------------------------
    // The file system aspects of the archive, e.g. the root directory of the archive
    // ---------------------------------------------------------------------------------------------
    #[test]
    fn test_get_root() {
        let TestArchive { tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let root = arch.root();
        assert_eq!(root, tmp.path());
    }

    // ---------------------------------------------------------------------------------------------
    // Query or modify site metadata
    // ---------------------------------------------------------------------------------------------
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
                time_zone: None,
            },
            Site {
                id: "ksea".to_uppercase(),
                name: Some("Seattle".to_owned()),
                notes: Some("A coastal city with coffe and rain".to_owned()),
                state: Some(StateProv::WA),
                auto_download: true,
                time_zone: Some(FixedOffset::west(8 * 3600)),
            },
            Site {
                id: "kmso".to_uppercase(),
                name: Some("Missoula".to_owned()),
                notes: Some("In a valley.".to_owned()),
                state: None,
                auto_download: true,
                time_zone: Some(FixedOffset::west(7 * 3600)),
            },
        ];

        for site in test_sites {
            arch.add_site(site).expect("Error adding site.");
        }

        assert!(arch.site_exists("ksea").expect("Error checking existence"));
        assert!(arch.site_exists("kord").expect("Error checking existence"));
        assert!(!arch.site_exists("xyz").expect("Error checking existence"));

        let retrieved_sites = arch.sites().expect("Error retrieving sites.");

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
                time_zone: None,
            },
            Site {
                id: "ksea".to_uppercase(),
                name: Some("Seattle".to_owned()),
                notes: Some("A coastal city with coffe and rain".to_owned()),
                state: Some(StateProv::WA),
                auto_download: true,
                time_zone: Some(FixedOffset::west(8 * 3600)),
            },
            Site {
                id: "kmso".to_uppercase(),
                name: Some("Missoula".to_owned()),
                notes: Some("In a valley.".to_owned()),
                state: None,
                auto_download: true,
                time_zone: Some(FixedOffset::west(7 * 3600)),
            },
        ];

        for site in test_sites {
            arch.add_site(site).expect("Error adding site.");
        }

        assert_eq!(arch.site_info("ksea").unwrap(), test_sites[1]);
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
                time_zone: None,
            },
            Site {
                id: "ksea".to_uppercase(),
                name: Some("Seattle".to_owned()),
                notes: Some("A coastal city with coffe and rain".to_owned()),
                state: Some(StateProv::WA),
                auto_download: true,
                time_zone: Some(FixedOffset::west(8 * 3600)),
            },
            Site {
                id: "kmso".to_uppercase(),
                name: Some("Missoula".to_owned()),
                notes: Some("In a valley.".to_owned()),
                state: None,
                auto_download: false,
                time_zone: None,
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
            time_zone: Some(FixedOffset::west(7 * 3600)),
        };

        arch.set_site_info(&zootown).expect("Error updating site.");

        assert_eq!(arch.site_info("kmso").unwrap(), zootown);
        assert_ne!(arch.site_info("kmso").unwrap(), test_sites[2]);
    }

    // ---------------------------------------------------------------------------------------------
    // Query archive inventory
    // ---------------------------------------------------------------------------------------------
    #[test]
    fn test_models_for_site() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let models = arch.models("kmso").expect("Error querying archive.");

        assert!(models.contains(&Model::GFS));
        assert!(models.contains(&Model::NAM));
        assert!(!models.contains(&Model::NAM4KM));
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
        assert_eq!(arch.inventory("kmso", Model::NAM).unwrap(), expected);
    }

    #[test]
    fn test_count() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        // 7 and not 10 because of duplicate GFS models in the input.
        assert_eq!(arch.count().expect("db error"), 7);
    }

    #[test]
    fn test_count_init_times() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        assert_eq!(
            arch.count_init_times("kmso", Model::GFS).expect("db error"),
            4
        );
        assert_eq!(
            arch.count_init_times("kmso", Model::NAM).expect("db error"),
            3
        );
    }

    // ---------------------------------------------------------------------------------------------
    // Add, remove, and retrieve files from the archive
    // ---------------------------------------------------------------------------------------------
    #[test]
    fn test_files_round_trip() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_data = get_test_data().expect("Error loading test data.");

        for (site, model, init_time, end_time, raw_data) in test_data {
            arch.add(&site, model, init_time, end_time, &raw_data)
                .expect("Failure to add.");
            let recovered_str = arch
                .retrieve(&site, model, init_time)
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
            .most_recent_init_time("kmso", Model::GFS)
            .expect("Error getting valid time.");

        assert_eq!(init_time, NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0));

        arch.most_recent_file("kmso", Model::GFS)
            .expect("Failed to retrieve sounding.");
    }

    #[test]
    fn test_file_exists() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        println!("Checking for files that should exist.");
        assert!(arch
            .file_exists(
                "kmso",
                Model::GFS,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(arch
            .file_exists(
                "kmso",
                Model::GFS,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(6, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(arch
            .file_exists(
                "kmso",
                Model::GFS,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(12, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(arch
            .file_exists(
                "kmso",
                Model::GFS,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0)
            )
            .expect("Error checking for existence"));

        println!("Checking for files that should NOT exist.");
        assert!(!arch
            .file_exists(
                "kmso",
                Model::GFS,
                &NaiveDate::from_ymd(2018, 4, 1).and_hms(0, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(!arch
            .file_exists(
                "kmso",
                Model::GFS,
                &NaiveDate::from_ymd(2018, 4, 1).and_hms(6, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(!arch
            .file_exists(
                "kmso",
                Model::GFS,
                &NaiveDate::from_ymd(2018, 4, 1).and_hms(12, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(!arch
            .file_exists(
                "kmso",
                Model::GFS,
                &NaiveDate::from_ymd(2018, 4, 1).and_hms(18, 0, 0)
            )
            .expect("Error checking for existence"));
    }

    #[test]
    fn test_remove_file() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let init_time = NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0);
        let model = Model::GFS;
        let site = "kmso";

        assert!(arch
            .file_exists(site, model, &init_time)
            .expect("Error checking db"));
        arch.remove(site, model, &init_time)
            .expect("Error while removing.");
        assert!(!arch
            .file_exists(site, model, &init_time)
            .expect("Error checking db"));
    }
}
