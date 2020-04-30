//! An archive of bufkit soundings.

use std::{
    collections::HashSet,
    convert::TryFrom,
    io::{Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
};

use crate::{
    errors::BufkitDataErr,
    inventory::Inventory,
    models::Model,
    site::{Site, StateProv},
};

/// The archive.
#[derive(Debug)]
pub struct Archive {
    root: PathBuf,                 // The root directory.
    data_root: PathBuf,            // the directory containing the downloaded files.
    db_conn: rusqlite::Connection, // An sqlite connection.
}

impl Archive {
    const DATA_DIR: &'static str = "data";
    const DB_FILE: &'static str = "index.sqlite";

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

        db_conn.execute_batch(include_str!("create_index.sql"))?;

        Ok(Archive {
            root,
            data_root,
            db_conn,
        })
    }

    /// Open an existing archive.
    pub fn connect(root: &dyn AsRef<Path>) -> Result<Self, BufkitDataErr> {
        let data_root = root.as_ref().join(Archive::DATA_DIR);
        let db_file = root.as_ref().join(Archive::DB_FILE);
        let root = root.as_ref().to_path_buf();

        // Create and set up the archive
        let db_conn = rusqlite::Connection::open_with_flags(
            db_file,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;

        Ok(Archive {
            root,
            data_root,
            db_conn,
        })
    }

    /// Validate files listed in the index are in the archive too, if not remove them from the
    /// index.
    pub fn clean(&self) -> Result<(), BufkitDataErr> {
        let arch = Archive::connect(&self.root)?;

        arch.db_conn
            .execute("PRAGMA cache_size=10000", rusqlite::NO_PARAMS)?;

        println!("Building set of files from the index.");
        let index_vals = self.get_all_files_from_index(&arch)?;

        println!("Building set of files from the file system.");
        let file_system_vals = self.get_all_files_in_data_dir(&arch)?;

        println!("Comparing sets for files in index but not in the archive.");
        let files_in_index_but_not_on_file_system = index_vals.difference(&file_system_vals);
        self.remove_missing_files_from_index(
            &arch,
            &mut files_in_index_but_not_on_file_system.into_iter(),
        )?;

        println!("Comparing sets for files in archive but not in the index.");
        let files_not_in_index = file_system_vals.difference(&index_vals);
        self.handle_files_in_archive_but_not_index(&arch, &mut files_not_in_index.into_iter())?;

        println!("Checking for orphaned stations.");
        self.handle_orphaned_stations(&arch)?;

        println!("Compressing index.");
        arch.db_conn.execute("VACUUM", rusqlite::NO_PARAMS)?;

        Ok(())
    }

    #[inline]
    fn get_all_files_from_index(&self, arch: &Archive) -> Result<HashSet<String>, BufkitDataErr> {
        let mut all_files_stmt = arch.db_conn.prepare("SELECT file_name FROM files")?;

        let index_vals: Result<HashSet<String>, BufkitDataErr> = all_files_stmt
            .query_map(rusqlite::NO_PARAMS, |row| row.get::<_, String>(0))?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .collect();

        index_vals
    }

    #[inline]
    fn get_all_files_in_data_dir(&self, arch: &Archive) -> Result<HashSet<String>, BufkitDataErr> {
        Ok(std::fs::read_dir(&arch.data_root)?
            .filter_map(Result::ok)
            .map(|de| de.path())
            .filter(|p| p.is_file())
            .filter_map(|p| p.file_name().map(ToOwned::to_owned))
            .map(|p| p.to_string_lossy().to_string())
            .collect())
    }

    #[inline]
    fn remove_missing_files_from_index(
        &self,
        arch: &Archive,
        files_in_index_but_not_on_file_system: &mut dyn Iterator<Item = &String>,
    ) -> Result<(), BufkitDataErr> {
        let mut del_stmt = arch
            .db_conn
            .prepare("DELETE FROM files WHERE file_name = ?1")?;

        arch.db_conn
            .execute("BEGIN TRANSACTION", rusqlite::NO_PARAMS)?;

        for missing_file in files_in_index_but_not_on_file_system {
            del_stmt.execute(&[missing_file])?;
            println!("Removing {} from index.", missing_file);
        }
        arch.db_conn
            .execute("COMMIT TRANSACTION", rusqlite::NO_PARAMS)?;

        Ok(())
    }

    #[inline]
    fn handle_files_in_archive_but_not_index(
        &self,
        arch: &Archive,
        files_not_in_index: &mut dyn Iterator<Item = &String>,
    ) -> Result<(), BufkitDataErr> {
        let mut insert_stmt = arch.db_conn.prepare(
            "
                    INSERT INTO files (station_num, model, init_time, end_time, file_name)
                    VALUES (?1, ?2, ?3, ?4, ?5)
                ",
        )?;

        arch.db_conn
            .execute("BEGIN TRANSACTION", rusqlite::NO_PARAMS)?;
        for extra_file in files_not_in_index {
            let message = if let Some((init_time, end_time, model, site)) =
                arch.extract_site_info_from_file(&extra_file)
            {
                if !arch.site_exists(site.station_num)? {
                    arch.add_site(&site)?;
                }

                match insert_stmt.execute(&[
                    &site.station_num,
                    &model.as_static_str() as &dyn rusqlite::types::ToSql,
                    &init_time as &dyn rusqlite::types::ToSql,
                    &end_time as &dyn rusqlite::types::ToSql,
                    &extra_file,
                ]) {
                    Ok(_) => format!("Added {}", extra_file),
                    Err(_) => {
                        std::fs::remove_file(arch.data_root.join(extra_file))?;
                        format!("Duplicate file removed: {}", extra_file)
                    }
                }
            } else {
                // Remove non-bufkit file
                std::fs::remove_file(arch.data_root.join(extra_file))?;
                format!("Removed non-bufkit file: {}", extra_file)
            };

            println!("{}", message);
        }
        arch.db_conn
            .execute("COMMIT TRANSACTION", rusqlite::NO_PARAMS)?;

        Ok(())
    }

    #[inline]
    fn handle_orphaned_stations(&self, arch: &Archive) -> Result<(), BufkitDataErr> {
        let mut stations_with_ids_stmt =
            arch.db_conn.prepare("SELECT station_num FROM site_ids")?;
        let stations_with_ids: Result<HashSet<i64>, BufkitDataErr> = stations_with_ids_stmt
            .query_map(rusqlite::NO_PARAMS, |row| row.get::<_, i64>(0))?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .collect();
        let stations_with_ids = stations_with_ids?;
        let mut stations_in_index_stmt = arch
            .db_conn
            .prepare("SELECT DISTINCT station_num FROM sites")?;
        let stations_in_index: Result<HashSet<i64>, BufkitDataErr> = stations_in_index_stmt
            .query_map(rusqlite::NO_PARAMS, |row| row.get::<_, i64>(0))?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .collect();
        let stations_in_index = stations_in_index?;

        let orphans = stations_in_index.difference(&stations_with_ids);
        for &orphan in orphans {
            if let Some(site) = arch.site_info(orphan as u32) {
                println!("     {}", site);
            // TODO - retrieve a sounding for the orphan, the most recent. Get it's id from
            // within the file, and set_site_info!
            } else {
                println!("     {} - unknown", orphan);
            }
        }

        Ok(())
    }

    /// Retrieve a path to the root. Allows caller to store files in the archive.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Retrieve a list of sites in the archive.
    pub fn sites(&self) -> Result<Vec<Site>, BufkitDataErr> {
        let mut stmt = self.db_conn.prepare(
            "
                SELECT 
                    sites.station_num,
                    site_ids.id,
                    name,
                    state,
                    notes,
                    auto_download,
                    tz_offset_sec 
            FROM sites JOIN site_ids ON sites.station_num = site_ids.station_num",
        )?;

        let vals: Result<Vec<Site>, BufkitDataErr> = stmt
            .query_and_then(rusqlite::NO_PARAMS, Self::parse_row_to_site)?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .collect();

        vals
    }

    /// Retrieve the information about a single site id
    pub fn site_info(&self, station_num: u32) -> Option<Site> {
        self.db_conn
            .query_row_and_then(
                "
                    SELECT 
                         sites.station_num,
                         site_ids.id,
                         name,
                         state,
                         notes,
                         auto_download,
                         tz_offset_sec
                    FROM sites LEFT JOIN site_ids ON sites.station_num = site_ids.station_num
                    WHERE sites.station_num = ?1
                ",
                &[&station_num],
                Self::parse_row_to_site,
            )
            .ok()
    }

    /// Given a site_id string, get the corresponding Site object.
    pub fn site_for_id(&self, site_id: &str) -> Option<Site> {
        self.db_conn
            .query_row_and_then(
                "
                    SELECT 
                         sites.station_num,
                         site_ids.id,
                         name,
                         state,
                         notes,
                         auto_download,
                         tz_offset_sec
                    FROM site_ids JOIN sites ON site_ids.station_num = sites.station_num
                    WHERE site_ids.id = ?1
                ",
                &[&site_id.to_uppercase()],
                Self::parse_row_to_site,
            )
            .ok()
    }

    fn parse_row_to_site(row: &rusqlite::Row) -> Result<Site, rusqlite::Error> {
        let station_num = row.get(0)?;
        let id = row.get(1)?;
        let name = row.get(2)?;
        let notes = row.get(4)?;
        let auto_download = row.get(5)?;
        let state: Option<StateProv> = row
            .get::<_, String>(3)
            .ok()
            .and_then(|a_string| StateProv::from_str(&a_string).ok());

        let time_zone: Option<chrono::FixedOffset> =
            row.get::<_, i32>(6).ok().map(|offset: i32| {
                if offset < 0 {
                    chrono::FixedOffset::west(offset.abs())
                } else {
                    chrono::FixedOffset::east(offset)
                }
            });

        Ok(Site {
            station_num,
            id,
            name,
            notes,
            state,
            auto_download,
            time_zone,
        })
    }

    /// Given a site, get the current ID used for that site.
    pub fn id_for_site(&self, site: &Site) -> Option<String> {
        self.db_conn
            .query_row_and_then(
                "SELECT id from site_ids WHERE station_num = ?1",
                &[&site.station_num],
                |row| row.get(0),
            )
            .ok()
    }

    /// Modify a site's values.
    pub fn set_site_info(&self, site: &Site) -> Result<(), BufkitDataErr> {
        self.db_conn.execute(
            "
                UPDATE sites
                SET (state,name,notes,auto_download,tz_offset_sec)
                = (?2, ?3, ?4, ?5, ?6)
                WHERE station_num = ?1
            ",
            &[
                &site.station_num,
                &site.state.map(|state_prov| state_prov.as_static_str())
                    as &dyn rusqlite::types::ToSql,
                &site.name,
                &site.notes,
                &site.auto_download,
                &site.time_zone.map(|tz| tz.local_minus_utc()),
            ],
        )?;

        self.check_update_site_id(site)
    }

    /// Add a site to the list of sites.
    pub fn add_site(&self, site: &Site) -> Result<(), BufkitDataErr> {
        self.db_conn.execute(
            "INSERT INTO sites (station_num, state, name, notes, auto_download, tz_offset_sec)
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            &[
                &site.station_num as &dyn rusqlite::ToSql,
                &site.state.map(|state_prov| state_prov.as_static_str())
                    as &dyn rusqlite::types::ToSql,
                &site.name,
                &site.notes,
                &site.auto_download,
                &site.time_zone.map(|tz| tz.local_minus_utc()),
            ],
        )?;

        self.check_update_site_id(site)?;

        Ok(())
    }

    #[inline]
    fn check_update_site_id(&self, site: &Site) -> Result<(), BufkitDataErr> {
        if let Some(ref site_id) = site.id {
            if let Some(other_site) = self.site_for_id(site_id) {
                if other_site.station_num != site.station_num {
                    self.db_conn.execute(
                        "DELETE FROM site_ids WHERE station_num = ?1",
                        &[&other_site.station_num],
                    )?;
                }
            } else {
                self.db_conn.execute(
                    "INSERT INTO site_ids (station_num, id) VALUES (?1, ?2)",
                    &[
                        &site.station_num as &dyn rusqlite::ToSql,
                        &site_id.to_uppercase() as &dyn rusqlite::ToSql,
                    ],
                )?;
            }
        }

        Ok(())
    }

    /// Check if a site already exists
    pub fn site_exists(&self, station_num: u32) -> Result<bool, BufkitDataErr> {
        let number: i32 = self.db_conn.query_row(
            "SELECT COUNT(*) FROM sites WHERE station_num = ?1",
            &[&station_num],
            |row| row.get(0),
        )?;

        Ok(number == 1)
    }

    /// Get a list of all the available model initialization times for a given site and model.
    pub fn init_times(
        &self,
        site: &Site,
        model: Model,
    ) -> Result<Vec<chrono::NaiveDateTime>, BufkitDataErr> {
        let mut stmt = self.db_conn.prepare(
            "
                SELECT init_time FROM files
                WHERE station_num = ?1 AND model = ?2
                ORDER BY init_time ASC
            ",
        )?;

        let init_times: Vec<Result<chrono::NaiveDateTime, _>> = stmt
            .query_map(
                &[
                    &site.station_num as &dyn rusqlite::ToSql,
                    &model.as_static_str() as &dyn rusqlite::ToSql,
                ],
                |row| row.get::<_, chrono::NaiveDateTime>(0),
            )?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .collect();

        let init_times: Vec<chrono::NaiveDateTime> =
            init_times.into_iter().filter_map(Result::ok).collect();

        Ok(init_times)
    }

    /// Get the number of values files in the archive for the model and intitialization time.
    pub fn count_init_times(&self, site: &Site, model: Model) -> Result<i64, BufkitDataErr> {
        let num_records: i64 = self.db_conn.query_row(
            "
                SELECT COUNT(init_time) FROM files
                WHERE station_num = ?1 AND model = ?2
            ",
            &[
                &site.station_num as &dyn rusqlite::ToSql,
                &model.as_static_str() as &dyn rusqlite::ToSql,
            ],
            |row| row.get(0),
        )?;

        Ok(num_records)
    }

    /// Get an inventory of soundings for a site & model.
    pub fn inventory(&self, site: &Site, model: Model) -> Result<Inventory, BufkitDataErr> {
        let init_times = self.init_times(site, model)?;
        Inventory::new(init_times, model, site)
    }

    /// Get a list of models in the archive for this site.
    pub fn models(&self, site: &Site) -> Result<Vec<Model>, BufkitDataErr> {
        let mut stmt = self
            .db_conn
            .prepare("SELECT DISTINCT model FROM files WHERE station_num = ?1")?;

        let vals: Result<Vec<Model>, BufkitDataErr> = stmt
            .query_map(&[&site.station_num], |row| row.get::<_, String>(0))?
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
        site: &Site,
        model: Model,
    ) -> Result<chrono::NaiveDateTime, BufkitDataErr> {
        let init_time: chrono::NaiveDateTime = self.db_conn.query_row(
            "
                SELECT init_time FROM files
                WHERE station_num = ?1 AND model = ?2
                ORDER BY init_time DESC
                LIMIT 1
            ",
            &[
                &site.station_num as &dyn rusqlite::ToSql,
                &model.as_static_str() as &dyn rusqlite::ToSql,
            ],
            |row| row.get(0),
        )?;

        Ok(init_time)
    }

    /// Retrieve all the initialization times of all sounding files that have a sounding with a
    /// valid time in the specified range (inclusive).
    pub fn init_times_for_soundings_valid_between(
        &self,
        start: chrono::NaiveDateTime,
        end: chrono::NaiveDateTime,
        site: &Site,
        model: Model,
    ) -> Result<Vec<chrono::NaiveDateTime>, BufkitDataErr> {
        let mut stmt = self.db_conn.prepare(
            "
                SELECT init_time
                FROM files
                WHERE station_num = ?1 AND model = ?2 AND init_time <= ?4 AND end_time >= ?3
                ORDER BY init_time ASC
            ",
        )?;

        let init_times: Result<Vec<chrono::NaiveDateTime>, _> = stmt
            .query_map(
                &[
                    &site.station_num as &dyn rusqlite::types::ToSql,
                    &model.as_static_str() as &dyn rusqlite::types::ToSql,
                    &start as &dyn rusqlite::types::ToSql,
                    &end as &dyn rusqlite::types::ToSql,
                ],
                |row| row.get::<_, chrono::NaiveDateTime>(0),
            )?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .collect();

        init_times
    }

    /// Check to see if a file is present in the archive and it is retrieveable.
    pub fn file_exists(
        &self,
        site: &Site,
        model: Model,
        init_time: &chrono::NaiveDateTime,
    ) -> Result<bool, BufkitDataErr> {
        let num_records: i32 = self.db_conn.query_row(
            "SELECT COUNT(*) FROM files WHERE station_num = ?1 AND model = ?2 AND init_time = ?3",
            &[
                &site.station_num as &dyn rusqlite::types::ToSql,
                &model.as_static_str() as &dyn rusqlite::types::ToSql,
                init_time as &dyn rusqlite::types::ToSql,
            ],
            |row| row.get(0),
        )?;

        Ok(num_records == 1)
    }

    /// Get the number of files stored in the archive.
    pub fn count(&self) -> Result<i64, BufkitDataErr> {
        let num_records: i64 =
            self.db_conn
                .query_row("SELECT COUNT(*) FROM files", rusqlite::NO_PARAMS, |row| {
                    row.get(0)
                })?;

        Ok(num_records)
    }

    /// Add a bufkit file to the archive.
    pub fn add(
        &self,
        site: &Site,
        model: Model,
        init_time: chrono::NaiveDateTime,
        end_time: chrono::NaiveDateTime,
        text_data: &str,
    ) -> Result<(), BufkitDataErr> {
        let site_id = if let Some(ref site_id) = site.id {
            site_id
        } else {
            return Err(BufkitDataErr::InvalidSiteId("None".to_owned()));
        };

        if !self.site_exists(site.station_num)? {
            self.add_site(&Site {
                station_num: site.station_num,
                id: site.id.clone(),
                name: None,
                notes: None,
                state: None,
                auto_download: false,
                time_zone: None,
            })?;
        }

        if let Some(Site {
            station_num: station_check,
            ..
        }) = self.site_for_id(site_id)
        {
            if station_check != site.station_num {
                self.db_conn.execute(
                    "DELETE FROM site_ids WHERE station_num = ?1",
                    &[&station_check],
                )?;
                self.db_conn.execute(
                    "INSERT INTO site_ids (station_num, id) VALUES (?1, ?2)",
                    &[
                        &site.station_num as &dyn rusqlite::ToSql,
                        &site_id.to_uppercase() as &dyn rusqlite::ToSql,
                    ],
                )?;
            }
        }

        let file_name = self.compressed_file_name(site_id, model, init_time);
        let file = std::fs::File::create(self.data_root.join(&file_name))?;
        let mut encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        encoder.write_all(text_data.as_bytes())?;

        self.db_conn.execute(
            "INSERT OR REPLACE INTO files (station_num, model, init_time, end_time, file_name)
                  VALUES (?1, ?2, ?3, ?4, ?5)",
            &[
                &site.station_num as &dyn rusqlite::types::ToSql,
                &model.as_static_str() as &dyn rusqlite::types::ToSql,
                &init_time as &dyn rusqlite::types::ToSql,
                &end_time,
                &file_name,
            ],
        )?;

        Ok(())
    }

    /// Retrieve a file from the archive.
    pub fn retrieve(
        &self,
        site: &Site,
        model: Model,
        init_time: chrono::NaiveDateTime,
    ) -> Result<String, BufkitDataErr> {
        let file_name: String = self.db_conn.query_row(
            "SELECT file_name FROM files WHERE station_num = ?1 AND model = ?2 AND init_time = ?3",
            &[
                &site.station_num as &dyn rusqlite::types::ToSql,
                &model.as_static_str() as &dyn rusqlite::types::ToSql,
                &init_time as &dyn rusqlite::types::ToSql,
            ],
            |row| row.get(0),
        )?;

        let file = std::fs::File::open(self.data_root.join(file_name))?;
        let mut decoder = flate2::read::GzDecoder::new(file);
        let mut s = String::new();
        decoder.read_to_string(&mut s)?;
        Ok(s)
    }

    /// Retrieve the  most recent file
    pub fn most_recent_file(&self, site: &Site, model: Model) -> Result<String, BufkitDataErr> {
        let init_time = self.most_recent_init_time(site, model)?;
        self.retrieve(site, model, init_time)
    }

    /// Retrieve all the soundings with data valid between the start and end times.
    pub fn retrieve_all_valid_in(
        &self,
        start: chrono::NaiveDateTime,
        end: chrono::NaiveDateTime,
        site: &Site,
        model: Model,
    ) -> Result<Vec<String>, BufkitDataErr> {
        let init_times = self.init_times_for_soundings_valid_between(start, end, site, model)?;

        let string_data: Result<Vec<String>, _> = init_times
            .into_iter()
            .map(|init_t| self.retrieve(site, model, init_t))
            .collect();

        string_data
    }

    fn compressed_file_name(
        &self,
        site_id: &str,
        model: Model,
        init_time: chrono::NaiveDateTime,
    ) -> String {
        let file_string = init_time.format("%Y%m%d%HZ").to_string();

        format!(
            "{}_{}_{}.buf.gz",
            file_string,
            model.as_static_str(),
            site_id.to_uppercase()
        )
    }

    fn extract_site_info_from_file(
        &self,
        fname: &str,
    ) -> Option<(chrono::NaiveDateTime, chrono::NaiveDateTime, Model, Site)> {
        let tokens: Vec<&str> = fname.split(|c| c == '_' || c == '.').collect();

        if tokens.len() != 5 || tokens[3] != "buf" || tokens[4] != "gz" {
            return None;
        }

        let model = Model::from_str(tokens[1]).ok()?;
        let id = Some(tokens[2].to_owned());

        let file = std::fs::File::open(self.data_root.join(fname)).ok()?;
        let mut decoder = flate2::read::GzDecoder::new(file);
        let mut s = String::new();
        decoder.read_to_string(&mut s).ok()?;

        let snds = sounding_bufkit::BufkitData::init(&s, fname).ok()?;
        let mut snds = snds.into_iter();

        let first = snds.next()?.0;
        let last = snds.last()?.0;

        let init_time = first.valid_time()?;
        let end_time = last.valid_time()?;

        let station_num: u32 = first
            .station_info()
            .station_num()
            .into_option()
            .and_then(|int32| u32::try_from(int32).ok())?;

        let site = Site {
            station_num,
            id,
            ..Site::default()
        };
        Some((init_time, end_time, model, site))
    }

    /// Get the file name this would have if uncompressed.
    pub fn file_name(
        &self,
        site_id: &str,
        model: Model,
        init_time: &chrono::NaiveDateTime,
    ) -> String {
        let file_string = init_time.format("%Y%m%d%HZ").to_string();

        format!(
            "{}_{}_{}.buf",
            file_string,
            model.as_static_str(),
            site_id.to_uppercase()
        )
    }

    /// Remove a file from the archive.
    pub fn remove(
        &self,
        site: &Site,
        model: Model,
        init_time: &chrono::NaiveDateTime,
    ) -> Result<(), BufkitDataErr> {
        let file_name: String = self.db_conn.query_row(
            "SELECT file_name FROM files WHERE station_num = ?1 AND model = ?2 AND init_time = ?3",
            &[
                &site.station_num as &dyn rusqlite::types::ToSql,
                &model.as_static_str() as &dyn rusqlite::types::ToSql,
                init_time as &dyn rusqlite::types::ToSql,
            ],
            |row| row.get(0),
        )?;

        std::fs::remove_file(self.data_root.join(file_name)).map_err(BufkitDataErr::IO)?;

        self.db_conn.execute(
            "DELETE FROM files WHERE station_num = ?1 AND model = ?2 AND init_time = ?3",
            &[
                &site.station_num as &dyn rusqlite::types::ToSql,
                &model.as_static_str() as &dyn rusqlite::types::ToSql,
                init_time as &dyn rusqlite::types::ToSql,
            ],
        )?;

        Ok(())
    }
}

#[cfg(test)]
mod unit {
    use super::*;

    use std::fs::read_dir;

    use chrono::{NaiveDate, NaiveDateTime};
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
        let arch = Archive::create(&tmp.path())?;

        Ok(TestArchive { tmp, arch })
    }

    // Function to fetch a list of test files.
    fn get_test_data(
    ) -> Result<Vec<(Site, Model, NaiveDateTime, NaiveDateTime, String)>, BufkitDataErr> {
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
                .next()
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

            let station_num: u32 = snd
                .station_info()
                .station_num()
                .into_option()
                .and_then(|int32| u32::try_from(int32).ok())
                .unwrap();

            let raw_string = bufkit_file.raw_text();

            let site = Site {
                station_num,
                id: Some(site.to_uppercase()),
                ..Site::default()
            };

            to_return.push((site, model, init_time, end_time, raw_string.to_owned()))
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

    #[test]
    fn test_sites_round_trip() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = &[
            Site {
                id: Some("kord".to_uppercase()),
                station_num: 1,
                name: Some("Chicago/O'Hare".to_owned()),
                notes: Some("Major air travel hub.".to_owned()),
                state: Some(StateProv::IL),
                auto_download: false,
                time_zone: None,
            },
            Site {
                id: Some("ksea".to_uppercase()),
                station_num: 2,
                name: Some("Seattle".to_owned()),
                notes: Some("A coastal city with coffe and rain".to_owned()),
                state: Some(StateProv::WA),
                auto_download: true,
                time_zone: Some(chrono::FixedOffset::west(8 * 3600)),
            },
            Site {
                id: Some("kmso".to_uppercase()),
                station_num: 3,
                name: Some("Missoula".to_owned()),
                notes: Some("In a valley.".to_owned()),
                state: None,
                auto_download: true,
                time_zone: Some(chrono::FixedOffset::west(7 * 3600)),
            },
        ];

        for site in test_sites {
            arch.add_site(site).expect("Error adding site.");
        }

        assert!(arch.site_for_id("ksea").is_some());
        assert!(arch.site_for_id("kord").is_some());
        assert!(arch.site_for_id("xyz").is_none());

        let retrieved_sites = arch.sites().expect("Error retrieving sites.");

        for site in retrieved_sites {
            println!("{:#?}", site);
            assert!(
                test_sites
                    .iter()
                    .find(|st| st.station_num == site.station_num)
                    .is_some()
                    && test_sites.iter().find(|st| st.id == site.id).is_some()
            );
        }
    }

    #[test]
    fn test_get_site_info() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = &[
            Site {
                station_num: 1,
                id: Some("kord".to_uppercase()),
                name: Some("Chicago/O'Hare".to_owned()),
                notes: Some("Major air travel hub.".to_owned()),
                state: Some(StateProv::IL),
                auto_download: false,
                time_zone: None,
            },
            Site {
                station_num: 2,
                id: Some("ksea".to_uppercase()),
                name: Some("Seattle".to_owned()),
                notes: Some("A coastal city with coffe and rain".to_owned()),
                state: Some(StateProv::WA),
                auto_download: true,
                time_zone: Some(chrono::FixedOffset::west(8 * 3600)),
            },
            Site {
                station_num: 3,
                id: Some("kmso".to_uppercase()),
                name: Some("Missoula".to_owned()),
                notes: Some("In a valley.".to_owned()),
                state: None,
                auto_download: true,
                time_zone: Some(chrono::FixedOffset::west(7 * 3600)),
            },
        ];

        for site in test_sites {
            arch.add_site(site).expect("Error adding site.");
        }

        assert_eq!(arch.site_for_id("ksea").unwrap(), test_sites[1]);
    }

    #[test]
    fn test_set_site_info() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = &[
            Site {
                station_num: 1,
                id: Some("kord".to_uppercase()),
                name: Some("Chicago/O'Hare".to_owned()),
                notes: Some("Major air travel hub.".to_owned()),
                state: Some(StateProv::IL),
                auto_download: false,
                time_zone: None,
            },
            Site {
                station_num: 2,
                id: Some("ksea".to_uppercase()),
                name: Some("Seattle".to_owned()),
                notes: Some("A coastal city with coffe and rain".to_owned()),
                state: Some(StateProv::WA),
                auto_download: true,
                time_zone: Some(chrono::FixedOffset::west(8 * 3600)),
            },
            Site {
                station_num: 3,
                id: Some("kmso".to_uppercase()),
                name: Some("Missoula".to_owned()),
                notes: Some("In a valley.".to_owned()),
                state: None,
                auto_download: true,
                time_zone: Some(chrono::FixedOffset::west(7 * 3600)),
            },
        ];

        for site in test_sites {
            arch.add_site(site).expect("Error adding site.");
        }

        let zootown = Site {
            station_num: 3,
            id: Some("kmso".to_uppercase()),
            name: Some("Zootown".to_owned()),
            notes: Some("Mountains, not coast.".to_owned()),
            state: None,
            auto_download: true,
            time_zone: Some(chrono::FixedOffset::west(7 * 3600)),
        };

        arch.set_site_info(&zootown).expect("Error updating site.");

        assert_eq!(arch.site_for_id("kmso").unwrap(), zootown);
        assert_ne!(arch.site_for_id("kmso").unwrap(), test_sites[2]);
    }

    #[test]
    fn test_models_for_site() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let kmso = arch.site_for_id("kmso").expect("Error retreiving MSO");

        let models = arch.models(&kmso).expect("Error querying archive.");

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

        let kmso = arch.site_for_id("kmso").expect("Error retreiving MSO");

        assert_eq!(arch.inventory(&kmso, Model::NAM).unwrap(), expected);
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

        let kmso = arch.site_for_id("kmso").expect("Error retreiving MSO");

        assert_eq!(
            arch.count_init_times(&kmso, Model::GFS).expect("db error"),
            4
        );
        assert_eq!(
            arch.count_init_times(&kmso, Model::NAM).expect("db error"),
            3
        );
    }

    #[test]
    fn test_files_round_trip() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_data = get_test_data().expect("Error loading test data.");

        for (site, model, init_time, end_time, raw_data) in test_data {
            arch.add(&site, model, init_time, end_time, &raw_data)
                .expect("Failure to add.");
            let site_obj = arch.site_for_id(&site.id.unwrap()).unwrap();

            let recovered_str = arch
                .retrieve(&site_obj, model, init_time)
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

        let kmso = arch.site_for_id("kmso").unwrap();

        let init_time = arch
            .most_recent_init_time(&kmso, Model::GFS)
            .expect("Error getting valid time.");

        assert_eq!(init_time, NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0));

        arch.most_recent_file(&kmso, Model::GFS)
            .expect("Failed to retrieve sounding.");
    }

    #[test]
    fn test_file_exists() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let kmso = arch.site_for_id("kmso").unwrap();

        println!("Checking for files that should exist.");
        assert!(arch
            .file_exists(
                &kmso,
                Model::GFS,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(arch
            .file_exists(
                &kmso,
                Model::GFS,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(6, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(arch
            .file_exists(
                &kmso,
                Model::GFS,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(12, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(arch
            .file_exists(
                &kmso,
                Model::GFS,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0)
            )
            .expect("Error checking for existence"));

        println!("Checking for files that should NOT exist.");
        assert!(!arch
            .file_exists(
                &kmso,
                Model::GFS,
                &NaiveDate::from_ymd(2018, 4, 1).and_hms(0, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(!arch
            .file_exists(
                &kmso,
                Model::GFS,
                &NaiveDate::from_ymd(2018, 4, 1).and_hms(6, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(!arch
            .file_exists(
                &kmso,
                Model::GFS,
                &NaiveDate::from_ymd(2018, 4, 1).and_hms(12, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(!arch
            .file_exists(
                &kmso,
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

        let site = arch.site_for_id("kmso").unwrap();
        let init_time = NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0);
        let model = Model::GFS;

        assert!(arch
            .file_exists(&site, model, &init_time)
            .expect("Error checking db"));
        arch.remove(&site, model, &init_time)
            .expect("Error while removing.");
        assert!(!arch
            .file_exists(&site, model, &init_time)
            .expect("Error checking db"));
    }
}
