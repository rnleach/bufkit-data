use std::{io::Read, str::FromStr};

use super::Archive;

use crate::{
    errors::BufkitDataErr,
    inventory::Inventory,
    models::Model,
    site::{Site, StateProv},
};

impl Archive {
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
    pub fn site(&self, station_num: u32) -> Option<Site> {
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

        let file = std::fs::File::open(self.data_root().join(file_name))?;
        let mut decoder = flate2::read::GzDecoder::new(file);
        let mut s = String::new();
        decoder.read_to_string(&mut s)?;
        Ok(s)
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

    /// Retrieve the  most recent file
    pub fn most_recent_file(&self, site: &Site, model: Model) -> Result<String, BufkitDataErr> {
        let init_time = self.most_recent_init_time(site, model)?;
        self.retrieve(site, model, init_time)
    }

    /// Get an inventory of soundings for a site & model.
    pub fn inventory(&self, site: &Site, model: Model) -> Result<Inventory, BufkitDataErr> {
        let init_times = self.init_times(site, model)?;
        Inventory::new(init_times, model, site)
    }

    fn parse_row_to_site(row: &rusqlite::Row) -> Result<Site, rusqlite::Error> {
        let station_num: u32 = row.get(0)?;
        let id: Option<String> = row.get(1)?;
        let name: Option<String> = row.get(2)?;
        let notes: Option<String> = row.get(4)?;
        let auto_download: bool = row.get(5)?;
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
}
