use std::{io::Read, str::FromStr};

use super::Archive;

use crate::{
    errors::BufkitDataErr,
    models::Model,
    site::{SiteInfo, StateProv, StationNumber},
};

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
        let auto_download: bool = row.get(4)?;
        let state: Option<StateProv> = row
            .get::<_, String>(2)
            .ok()
            .and_then(|a_string| StateProv::from_str(&a_string).ok());

        let time_zone: Option<chrono::FixedOffset> =
            row.get::<_, i32>(5).ok().map(|offset: i32| {
                if offset < 0 {
                    chrono::FixedOffset::west(offset.abs())
                } else {
                    chrono::FixedOffset::east(offset)
                }
            });

        Ok(SiteInfo {
            station_num,
            name,
            notes,
            state,
            auto_download,
            time_zone,
        })
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
                         auto_download,
                         tz_offset_sec
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

    /*
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

    /// Get an inventory of soundings for a site & model.
    pub fn inventory(&self, site: &Site, model: Model) -> Result<Inventory, BufkitDataErr> {
        let init_times = self.init_times(site, model)?;
        Inventory::new(init_times, model, site)
    }
    */

    /*
        /// Get a list of all the available model initialization times for a given site and model.
        pub(crate) fn init_times(
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

        /// Retrieve the model initialization time of the most recent model in the archive.
        pub(crate) fn most_recent_init_time(
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
        pub(crate) fn init_times_for_soundings_valid_between(
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
    */
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
        assert_eq!(si.auto_download, false);
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
        assert_eq!(si.auto_download, true);
        assert_eq!(si.time_zone, Some(chrono::FixedOffset::west(8 * 3600)));

        let si = arch
            .site(StationNumber::from(3))
            .expect("Error retrieving site.");
        assert_eq!(si.name, Some("Missoula".to_owned()));
        assert_eq!(si.notes, Some("In a valley.".to_owned()));
        assert_eq!(si.state, None);
        assert_eq!(si.auto_download, true);
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

    /*
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
    fn test_auto_download_sites() {
        // list of strings with 4 letter ids used for downloading
        unimplemented!()
    }

    #[test]
    fn test_id_info() {
        // given a list of station numbers, return a list of (station_num, id, most recent date)
        // used for auto download sites? Inventory?
        unimplemented!()
    }

    #[test]
    fn test_station_num_for_id() {
        unimplemented!()
    }

    */
}
