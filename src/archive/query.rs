use chrono::NaiveDateTime;
use rusqlite::OptionalExtension;
use std::{collections::HashSet, io::Read, iter::FromIterator, str::FromStr};

use super::Archive;

use crate::{
    errors::BufkitDataErr,
    models::Model,
    site::{SiteInfo, StateProv, StationNumber},
};

mod auto_download_info;
pub use auto_download_info::DownloadInfo;
mod station_summary;
pub use station_summary::StationSummary;

impl Archive {
    /// Retrieve a list of sites in the archive.
    pub fn sites(&self) -> Result<Vec<SiteInfo>, BufkitDataErr> {
        let mut stmt = self
            .db_conn
            .prepare(include_str!("query/retrieve_sites.sql"))?;

        let vals: Result<Vec<SiteInfo>, BufkitDataErr> = stmt
            .query_and_then(rusqlite::NO_PARAMS, Self::parse_row_to_site)?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .collect();

        vals
    }

    fn parse_row_to_site(row: &rusqlite::Row) -> Result<SiteInfo, rusqlite::Error> {
        let station_num: u32 = row.get(0)?;
        let station_num = StationNumber::from(station_num);

        let name: Option<String> = row.get(1)?;
        let notes: Option<String> = row.get(3)?;
        let state: Option<StateProv> = row
            .get::<_, String>(2)
            .ok()
            .and_then(|a_string| StateProv::from_str(&a_string).ok());

        let time_zone: Option<chrono::FixedOffset> =
            row.get::<_, i32>(4).ok().map(|offset: i32| {
                if offset < 0 {
                    chrono::FixedOffset::west(offset.abs())
                } else {
                    chrono::FixedOffset::east(offset)
                }
            });

        let auto_download: bool = row.get(5)?;

        Ok(SiteInfo {
            station_num,
            name,
            notes,
            state,
            time_zone,
            auto_download,
        })
    }

    /// Retrieve the sites with their most recent station id for the given model.
    pub fn sites_and_ids_for(
        &self,
        model: Model,
    ) -> Result<Vec<(SiteInfo, String)>, BufkitDataErr> {
        let mut stmt = self
            .db_conn
            .prepare(include_str!("query/sites_and_ids_for_model.sql"))?;

        let parse_row = |row: &rusqlite::Row| -> Result<(SiteInfo, String), rusqlite::Error> {
            let site_info = Self::parse_row_to_site(row)?;
            let site_id: String = row.get(6)?;
            Ok((site_info, site_id))
        };

        let vals: Result<Vec<(SiteInfo, String)>, BufkitDataErr> = stmt
            .query_and_then(&[&model.as_static_str()], parse_row)?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .collect();

        vals
    }

    /// Retrieve the information about a single site id
    pub fn site(&self, station_num: StationNumber) -> Option<SiteInfo> {
        self.db_conn
            .query_row_and_then(
                "
                    SELECT
                         station_num,
                         name,
                         state,
                         notes,
                         tz_offset_sec,
                         auto_download
                    FROM sites 
                    WHERE station_num = ?1
                ",
                &[&Into::<u32>::into(station_num)],
                Self::parse_row_to_site,
            )
            .ok()
    }

    /// Get a list of models in the archive for this site.
    pub fn models(&self, station_num: StationNumber) -> Result<Vec<Model>, BufkitDataErr> {
        let station_num: u32 = Into::<u32>::into(station_num);

        let mut stmt = self
            .db_conn
            .prepare("SELECT DISTINCT model FROM files WHERE station_num = ?1")?;

        let vals: Result<Vec<Model>, BufkitDataErr> = stmt
            .query_map(&[&station_num], |row| row.get::<_, String>(0))?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .map(|res| {
                res.and_then(|name| Model::from_str(&name).map_err(BufkitDataErr::StrumError))
            })
            .collect();

        vals
    }

    /// Get a list of auto-download sites with the id to use to download them.
    pub fn auto_downloads(&self) -> Result<Vec<DownloadInfo>, BufkitDataErr> {
        let mut stmt = self.db_conn.prepare(
            "
                SELECT id, files.station_num, model, MAX(init_time)
                FROM sites JOIN files ON sites.station_num = files.station_num
                WHERE auto_download = 1
                GROUP BY files.station_num, model
            ",
        )?;

        let auto_dl_info: Vec<DownloadInfo> = stmt
            .query_map(rusqlite::NO_PARAMS, |row| {
                let id = row.get(0)?;
                let stn_num = row.get::<_, u32>(1).map(StationNumber::from)?;
                let model: String = row.get(2)?;
                Ok((id, stn_num, model))
            })?
            .filter_map(|res| res.ok())
            .filter_map(|(id, stn_num, model_str)| {
                let model: Model = Model::from_str(&model_str).ok()?;
                Some((id, stn_num, model))
            })
            .map(|(id, station_num, model)| DownloadInfo {
                id,
                station_num,
                model,
            })
            .collect();

        if auto_dl_info.is_empty() {
            return Err(BufkitDataErr::NotInIndex);
        }

        Ok(auto_dl_info)
    }

    /// Retrieve a file from the archive.
    pub fn retrieve(
        &self,
        station_num: StationNumber,
        model: Model,
        init_time: chrono::NaiveDateTime,
    ) -> Result<String, BufkitDataErr> {
        let station_num: u32 = Into::<u32>::into(station_num);

        let file_name: Result<String, _> = self.db_conn.query_row(
            "SELECT file_name FROM files WHERE station_num = ?1 AND model = ?2 AND init_time = ?3",
            &[
                &station_num as &dyn rusqlite::types::ToSql,
                &model.as_static_str() as &dyn rusqlite::types::ToSql,
                &init_time as &dyn rusqlite::types::ToSql,
            ],
            |row| row.get(0),
        );

        let file_name = match file_name {
            Ok(fname) => fname,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Err(BufkitDataErr::NotInIndex),
            Err(x) => return Err(BufkitDataErr::Database(x)),
        };

        let file = std::fs::File::open(self.data_root().join(file_name))?;
        let mut decoder = flate2::read::GzDecoder::new(file);
        let mut s = String::new();
        decoder.read_to_string(&mut s)?;
        Ok(s)
    }

    /// Retrieve the  most recent file.
    pub fn retrieve_most_recent(
        &self,
        station_num: StationNumber,
        model: Model,
    ) -> Result<String, BufkitDataErr> {
        let station_num: u32 = Into::<u32>::into(station_num);

        let file_name: Result<String, _> = self.db_conn.query_row(
            "
                SELECT file_name 
                FROM files 
                WHERE station_num = ?1 AND model = ?2 
                ORDER BY init_time DESC 
                LIMIT 1
            ",
            &[
                &station_num as &dyn rusqlite::types::ToSql,
                &model.as_static_str() as &dyn rusqlite::types::ToSql,
            ],
            |row| row.get(0),
        );

        let file_name = match file_name {
            Ok(fname) => fname,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Err(BufkitDataErr::NotInIndex),
            Err(x) => return Err(BufkitDataErr::Database(x)),
        };

        let file = std::fs::File::open(self.data_root().join(file_name))?;
        let mut decoder = flate2::read::GzDecoder::new(file);
        let mut s = String::new();
        decoder.read_to_string(&mut s)?;
        Ok(s)
    }

    /// Retrieve all the soundings with any data valid between the start and end times.
    pub fn retrieve_all_valid_in(
        &self,
        station_num: StationNumber,
        model: Model,
        start: chrono::NaiveDateTime,
        end: chrono::NaiveDateTime,
    ) -> Result<impl Iterator<Item = String>, BufkitDataErr> {
        let station_num: u32 = Into::<u32>::into(station_num);

        let mut stmt = self.db_conn.prepare(
            "
                    SELECT file_name 
                    FROM files 
                    WHERE station_num = ?1 AND model = ?2 AND 
                        (
                            (init_time <= ?3 AND end_time >= ?4) OR 
                            (init_time >= ?3 AND init_time < ?4) OR 
                            (end_time > ?3 AND end_time <= ?4)
                        )
                    ORDER BY init_time ASC 
                ",
        )?;

        let file_names: Vec<String> = stmt
            .query_map(
                &[
                    &station_num as &dyn rusqlite::types::ToSql,
                    &model.as_static_str() as &dyn rusqlite::types::ToSql,
                    &start as &dyn rusqlite::types::ToSql,
                    &end as &dyn rusqlite::types::ToSql,
                ],
                |row| row.get(0),
            )?
            .filter_map(|res| res.ok())
            .collect();

        if file_names.is_empty() {
            return Err(BufkitDataErr::NotInIndex);
        }

        let root = self.data_root();
        Ok(file_names.into_iter().filter_map(move |fname| {
            std::fs::File::open(root.join(fname)).ok().and_then(|f| {
                let mut decoder = flate2::read::GzDecoder::new(f);
                let mut s = String::new();
                match decoder.read_to_string(&mut s) {
                    Ok(_) => Some(s),
                    Err(_) => None,
                }
            })
        }))
    }

    /// Check to see if a file is present in the archive and it is retrieveable.
    pub fn file_exists(
        &self,
        site: StationNumber,
        model: Model,
        init_time: NaiveDateTime,
    ) -> Result<bool, BufkitDataErr> {
        let num_records: i32 = self.db_conn.query_row(
            "SELECT COUNT(*) FROM files WHERE station_num = ?1 AND model = ?2 AND init_time = ?3",
            &[
                &Into::<i64>::into(site) as &dyn rusqlite::types::ToSql,
                &model.as_static_str() as &dyn rusqlite::types::ToSql,
                &init_time as &dyn rusqlite::types::ToSql,
            ],
            |row| row.get(0),
        )?;

        Ok(num_records == 1)
    }

    /// Retrieve the most recent station number used with this ID and model.
    pub fn station_num_for_id(
        &self,
        id: &str,
        model: Model,
    ) -> Result<StationNumber, BufkitDataErr> {
        let station_num: Result<u32, _> = self.db_conn.query_row(
            include_str!("query/station_num_for_id_and_model.sql"),
            &[
                &id.to_uppercase() as &dyn rusqlite::types::ToSql,
                &model.as_static_str(),
            ],
            |row| row.get(0),
        );

        let station_num = match station_num {
            Ok(num) => StationNumber::from(num),
            Err(rusqlite::Error::QueryReturnedNoRows) => return Err(BufkitDataErr::NotInIndex),
            Err(x) => return Err(BufkitDataErr::Database(x)),
        };

        Ok(station_num)
    }

    /// Retrieve a list of site ids use with the station number.
    pub fn ids(
        &self,
        station_num: StationNumber,
        model: Model,
    ) -> Result<Vec<String>, BufkitDataErr> {
        let station_num: u32 = Into::<u32>::into(station_num);

        let mut stmt = self.db_conn.prepare(
            "
                SELECT DISTINCT id 
                FROM files
                WHERE station_num = ?1 AND model = ?2
            ",
        )?;

        let sites: Result<Vec<String>, _> = stmt
            .query_map(
                &[
                    &station_num as &dyn rusqlite::types::ToSql,
                    &model.as_static_str() as &dyn rusqlite::types::ToSql,
                ],
                |row| row.get(0),
            )?
            .collect();

        sites.map_err(BufkitDataErr::Database)
    }

    /// Retrieve the most recently used ID with a site.
    pub fn most_recent_id(
        &self,
        station_num: StationNumber,
        model: Model,
    ) -> Result<Option<String>, BufkitDataErr> {
        let station_num_raw: u32 = Into::<u32>::into(station_num);

        let mut stmt = self.db_conn.prepare(
            "
                SELECT id, init_time 
                FROM files
                WHERE station_num = ?1 AND model = ?2
                ORDER BY init_time DESC
            ",
        )?;

        let most_recent_site: String = match stmt
            .query_row(
                &[
                    &station_num_raw as &dyn rusqlite::types::ToSql,
                    &model.as_static_str() as &dyn rusqlite::types::ToSql,
                ],
                |row| row.get(0),
            )
            .optional()?
        {
            Some(id) => id,
            None => return Ok(None),
        };

        let most_recent_station_num = self.station_num_for_id(&most_recent_site, model)?;

        if most_recent_station_num == station_num {
            Ok(Some(most_recent_site))
        } else {
            Ok(None)
        }
    }

    /// Get an inventory of soundings for a site & model.
    pub fn inventory(
        &self,
        station_num: StationNumber,
        model: Model,
    ) -> Result<Vec<NaiveDateTime>, BufkitDataErr> {
        let station_num: u32 = Into::<u32>::into(station_num);

        let mut stmt = self.db_conn.prepare(
            "
                SELECT init_time
                FROM files
                WHERE station_num = ?1 AND model = ?2
                ORDER BY init_time ASC
            ",
        )?;

        let inv: Result<Vec<NaiveDateTime>, _> = stmt
            .query_map(
                &[
                    &station_num as &dyn rusqlite::types::ToSql,
                    &model.as_static_str() as &dyn rusqlite::types::ToSql,
                ],
                |row| row.get(0),
            )?
            .collect();

        inv.map_err(BufkitDataErr::Database)
    }

    /// Get list of missing init times.
    ///
    /// If time_range is `None`, this will find the first and last entries and then look for any
    /// gaps. If time_range is specified, then the end times are inclusive.
    pub fn missing_inventory(
        &self,
        station_num: StationNumber,
        model: Model,
        time_range: Option<(NaiveDateTime, NaiveDateTime)>,
    ) -> Result<Vec<NaiveDateTime>, BufkitDataErr> {
        let (start, end) = if let Some((start, end)) = time_range {
            (start, end)
        } else {
            self.first_and_last_dates(station_num, model)?
        };

        let inv = self.inventory(station_num, model)?;
        let inv: HashSet<NaiveDateTime> = HashSet::from_iter(inv.into_iter());

        let mut to_ret = vec![];
        for curr_time in model.all_runs(&start, &end) {
            if !inv.contains(&curr_time) {
                to_ret.push(curr_time);
            }
        }

        Ok(to_ret)
    }

    /// Get the number of files in the archive for the given station and model.
    pub fn count(&self, station_num: StationNumber, model: Model) -> Result<u32, BufkitDataErr> {
        let station_num: u32 = Into::<u32>::into(station_num);
        self.db_conn
            .query_row(
                "
                SELECT COUNT(*)
                FROM files
                WHERE station_num = ?1 AND model = ?2
            ",
                &[
                    &station_num as &dyn rusqlite::types::ToSql,
                    &model.as_static_str(),
                ],
                |row| row.get(0),
            )
            .map_err(BufkitDataErr::Database)
    }

    fn first_and_last_dates(
        &self,
        station_num: StationNumber,
        model: Model,
    ) -> Result<(NaiveDateTime, NaiveDateTime), BufkitDataErr> {
        let station_num: u32 = Into::<u32>::into(station_num);

        let start = self.db_conn.query_row(
            "
                    SELECT init_time
                    FROM files
                    WHERE station_num = ?1 AND model = ?2
                    ORDER BY init_time ASC
                    LIMIT 1
                ",
            &[
                &station_num as &dyn rusqlite::types::ToSql,
                &model.as_static_str() as &dyn rusqlite::types::ToSql,
            ],
            |row| row.get(0),
        )?;

        let end = self.db_conn.query_row(
            "
                    SELECT init_time
                    FROM files
                    WHERE station_num = ?1 AND model = ?2
                    ORDER BY init_time DESC
                    LIMIT 1
                ",
            &[
                &station_num as &dyn rusqlite::types::ToSql,
                &model.as_static_str() as &dyn rusqlite::types::ToSql,
            ],
            |row| row.get(0),
        )?;

        Ok((start, end))
    }
}

#[cfg(test)]
mod unit {
    use super::*;
    use crate::archive::unit::*; // test helpers.

    use chrono::NaiveDate;

    #[test]
    fn test_site_info() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = &get_test_sites();

        for site in test_sites {
            arch.add_site(site).expect("Error adding site.");
        }

        let si = arch
            .site(StationNumber::from(1))
            .expect("Error retrieving site.");
        assert_eq!(si.name, Some("Chicago/O'Hare".to_owned()));
        assert_eq!(si.notes, Some("Major air travel hub.".to_owned()));
        assert_eq!(si.state, Some(StateProv::IL));
        assert_eq!(si.time_zone, None);

        let si = arch
            .site(StationNumber::from(2))
            .expect("Error retrieving site.");
        assert_eq!(si.name, Some("Seattle".to_owned()));
        assert_eq!(
            si.notes,
            Some("A coastal city with coffe and rain".to_owned())
        );
        assert_eq!(si.state, Some(StateProv::WA));
        assert_eq!(si.time_zone, Some(chrono::FixedOffset::west(8 * 3600)));

        let si = arch
            .site(StationNumber::from(3))
            .expect("Error retrieving site.");
        assert_eq!(si.name, Some("Missoula".to_owned()));
        assert_eq!(si.notes, Some("In a valley.".to_owned()));
        assert_eq!(si.state, None);
        assert_eq!(si.time_zone, Some(chrono::FixedOffset::west(7 * 3600)));

        assert!(arch.site(StationNumber::from(0)).is_none());
        assert!(arch.site(StationNumber::from(100)).is_none());
    }

    #[test]
    fn test_models_for_site() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch);

        let kmso = StationNumber::from(727730); // Station number for KMSO
        let models = arch.models(kmso).expect("Error querying archive.");

        assert!(models.contains(&Model::GFS));
        assert!(models.contains(&Model::NAM));
        assert!(!models.contains(&Model::NAM4KM));
    }

    #[test]
    fn test_retrieve() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch);

        let kmso = StationNumber::from(727730); // Station number for KMSO
        let init_time = NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0);
        let model = Model::GFS;

        let res = arch.retrieve(kmso, model, init_time);
        assert!(res.is_ok());

        let init_time = NaiveDate::from_ymd(2117, 4, 1).and_hms(18, 0, 0);
        let res = arch.retrieve(kmso, model, init_time);
        match res {
            Err(BufkitDataErr::NotInIndex) => {}
            Err(_) => panic!("Wrong error type returned."),
            Ok(_) => panic!("This should not exist in the database."),
        }
    }

    #[test]
    fn test_retrieve_most_recent() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch);

        let kmso = StationNumber::from(727730); // Station number for KMSO
        let init_time = NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0);
        let model = Model::GFS;

        let res = arch.retrieve_most_recent(kmso, model);

        if let Ok(str_data) = res {
            let retrieved_init_time = sounding_bufkit::BufkitData::init(&str_data, "")
                .expect("Failure parsing.")
                .into_iter()
                .next()
                .expect("No data in file?")
                .0
                .valid_time()
                .expect("No valid time with sounding?");

            assert_eq!(retrieved_init_time, init_time);
        } else {
            panic!("Nothing found!");
        }
    }

    #[test]
    fn test_file_exists() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch);

        let kmso_station_num = StationNumber::from(727730); // Station number for KMSO
        let model = Model::NAM;

        let first = NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0);
        let second = NaiveDate::from_ymd(2017, 4, 1).and_hms(12, 0, 0);
        let last = NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0);
        let missing = NaiveDate::from_ymd(2017, 4, 1).and_hms(6, 0, 0);
        assert!(arch.file_exists(kmso_station_num, model, first).unwrap());
        assert!(arch.file_exists(kmso_station_num, model, second).unwrap());
        assert!(arch.file_exists(kmso_station_num, model, last).unwrap());
        assert!(!arch.file_exists(kmso_station_num, model, missing).unwrap());
    }

    #[test]
    fn test_station_num_for_id() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch);

        let kmso_station_num = StationNumber::from(727730); // Station number for KMSO

        if let Ok(retrieved_station_num) = arch.station_num_for_id("kmso", Model::GFS) {
            assert_eq!(retrieved_station_num, kmso_station_num);
        } else {
            panic!("Could not find station number!");
        }

        if let Ok(retrieved_station_num) = arch.station_num_for_id("KMSO", Model::GFS) {
            assert_eq!(retrieved_station_num, kmso_station_num);
        } else {
            panic!("Could not find station number!");
        }

        if let Ok(retrieved_station_num) = arch.station_num_for_id("KmSo", Model::NAM) {
            assert_eq!(retrieved_station_num, kmso_station_num);
        } else {
            panic!("Could not find station number!");
        }

        match arch.station_num_for_id("xyz", Model::GFS) {
            Err(BufkitDataErr::NotInIndex) => {}
            Ok(num) => panic!("Found station that does not exists! station_num = {}", num),
            Err(err) => panic!("Other error: {}", err),
        }
    }

    #[test]
    fn test_ids() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch);

        let kmso_station_num = StationNumber::from(727730); // Station number for KMSO
        let ids = arch
            .ids(kmso_station_num, Model::GFS)
            .expect("Database error.");
        assert!(ids.contains(&"KMSO".to_owned()));
        assert_eq!(ids.len(), 1);

        let ids = arch
            .ids(kmso_station_num, Model::NAM)
            .expect("Database error.");
        assert!(ids.contains(&"KMSO".to_owned()));
        assert_eq!(ids.len(), 1);

        let ids = arch
            .ids(kmso_station_num, Model::NAM4KM)
            .expect("Database error.");
        assert_eq!(ids.len(), 0);

        let fake_station_num = StationNumber::from(5);
        let ids = arch
            .ids(fake_station_num, Model::GFS)
            .expect("Database error.");
        assert_eq!(ids.len(), 0);
    }

    #[test]
    fn test_most_recent_id() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch);

        let kmso_station_num = StationNumber::from(727730); // Station number for KMSO
        let id = arch
            .most_recent_id(kmso_station_num, Model::GFS)
            .expect("Database error.");
        assert_eq!(id.unwrap(), "KMSO".to_owned());

        let id = arch
            .most_recent_id(kmso_station_num, Model::NAM)
            .expect("Database error.");
        assert_eq!(id.unwrap(), "KMSO".to_owned());
    }

    #[test]
    fn test_inventory() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch);

        let kmso = StationNumber::from(727730); // Station number for KMSO
        let first = NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0);
        let second = NaiveDate::from_ymd(2017, 4, 1).and_hms(12, 0, 0);
        let last = NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0);
        let missing = NaiveDate::from_ymd(2017, 4, 1).and_hms(6, 0, 0);

        let inv = arch.inventory(kmso, Model::NAM).expect("Data base error?");
        assert!(inv.contains(&first));
        assert!(inv.contains(&second));
        assert!(inv.contains(&last));
        assert!(!inv.contains(&missing));
    }

    #[test]
    fn test_missing_inventory() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch);

        let kmso = StationNumber::from(727730); // Station number for KMSO
        let first = NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0);
        let second = NaiveDate::from_ymd(2017, 4, 1).and_hms(12, 0, 0);
        let last = NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0);
        let missing = NaiveDate::from_ymd(2017, 4, 1).and_hms(6, 0, 0);

        let missing_times = arch
            .missing_inventory(kmso, Model::NAM, None)
            .expect("Data base error?");
        assert!(!missing_times.contains(&first));
        assert!(!missing_times.contains(&second));
        assert!(!missing_times.contains(&last));
        assert!(missing_times.contains(&missing));

        let larger_range = (
            NaiveDate::from_ymd(2017, 3, 31).and_hms(0, 0, 0),
            NaiveDate::from_ymd(2017, 4, 2).and_hms(12, 0, 0),
        );
        let missing_times = arch
            .missing_inventory(kmso, Model::NAM, Some(larger_range))
            .expect("Data base error?");
        assert!(missing_times.contains(&NaiveDate::from_ymd(2017, 3, 31).and_hms(0, 0, 0)));
        assert!(missing_times.contains(&NaiveDate::from_ymd(2017, 3, 31).and_hms(6, 0, 0)));
        assert!(missing_times.contains(&NaiveDate::from_ymd(2017, 3, 31).and_hms(12, 0, 0)));
        assert!(missing_times.contains(&NaiveDate::from_ymd(2017, 3, 31).and_hms(18, 0, 0)));
        assert!(!missing_times.contains(&first));
        assert!(!missing_times.contains(&second));
        assert!(!missing_times.contains(&last));
        assert!(missing_times.contains(&missing));
        assert!(missing_times.contains(&NaiveDate::from_ymd(2017, 4, 2).and_hms(0, 0, 0)));
        assert!(missing_times.contains(&NaiveDate::from_ymd(2017, 4, 2).and_hms(6, 0, 0)));
        assert!(missing_times.contains(&NaiveDate::from_ymd(2017, 4, 2).and_hms(12, 0, 0)));
    }

    #[test]
    fn test_retrieve_all_valid_in() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch);

        let kmso = StationNumber::from(727730); // Station number for KMSO
        let start = NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0);
        let end = NaiveDate::from_ymd(2017, 4, 1).and_hms(12, 0, 0);
        assert_eq!(
            arch.retrieve_all_valid_in(kmso, Model::GFS, start, end)
                .unwrap()
                .into_iter()
                .count(),
            1
        );

        let end = NaiveDate::from_ymd(2017, 4, 2).and_hms(0, 0, 0);
        assert_eq!(
            arch.retrieve_all_valid_in(kmso, Model::GFS, start, end)
                .unwrap()
                .into_iter()
                .count(),
            3
        );

        let start = NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0);
        assert_eq!(
            arch.retrieve_all_valid_in(kmso, Model::GFS, start, end)
                .unwrap()
                .into_iter()
                .count(),
            3
        );

        let start = NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0);
        let end = NaiveDate::from_ymd(2017, 4, 1).and_hms(12, 0, 0);
        assert_eq!(
            arch.retrieve_all_valid_in(kmso, Model::NAM, start, end)
                .unwrap()
                .into_iter()
                .count(),
            1
        );

        let end = NaiveDate::from_ymd(2017, 4, 2).and_hms(0, 0, 0);
        assert_eq!(
            arch.retrieve_all_valid_in(kmso, Model::NAM, start, end)
                .unwrap()
                .into_iter()
                .count(),
            3
        );

        let start = NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0);
        assert_eq!(
            arch.retrieve_all_valid_in(kmso, Model::NAM, start, end)
                .unwrap()
                .into_iter()
                .count(),
            3
        );
    }

    #[test]
    fn test_count() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch);

        let kmso = StationNumber::from(727730); // Station number for KMSO
        let model = Model::GFS;
        assert_eq!(arch.count(kmso, model).unwrap(), 3);

        let model = Model::NAM;
        assert_eq!(arch.count(kmso, model).unwrap(), 3);
    }
}
