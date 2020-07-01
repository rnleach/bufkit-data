use chrono::NaiveDateTime;
use std::{convert::TryFrom, io::Write};

use super::Archive;

use crate::{coords::Coords, errors::BufkitDataErr, models::Model, site::Site};

/// The end result of adding a file to the archive.
#[derive(Debug)]
pub enum AddFileResult {
    /// No site conflicts or changes. Includes the site as parsed from the file.
    Ok(Site),
    /// This is a new site and it was added to the database as a new site.
    New(Site),
    /// Some error occurred during processing.
    Error(BufkitDataErr),
    /// The site identifier provided with this file has moved to a new station number. The
    /// correlation between station numbers has been updated.
    SiteMovedStation {
        /// The site information before the update.
        old: Site,
        /// The site information as it exists now.
        new: Site,
    },
    /// Different site / model combinations usually have different sets of coordinates. However,
    /// this is a new set of never before seen coordinates.
    SiteMovedCoords {
        /// The new site as parsed from the file
        site: Site,
        /// The new coordinates of the site
        coords: Coords,
    },
}

impl Archive {
    /// Add a bufkit file to the archive.
    pub fn add(&self, site_id: &str, model: Model, text_data: &str) -> AddFileResult {
        let site_id = site_id.to_uppercase();

        let (site, init_time, end_time, coords) = match Self::parse_site_info(text_data, site_id) {
            Ok(val) => val,
            Err(err) => return AddFileResult::Error(err),
        };

        let site_validation = self.validate_site(site, coords);
        let site: &Site = match site_validation {
            err @ AddFileResult::Error(_) => return err,
            AddFileResult::Ok(ref site)
            | AddFileResult::New(ref site)
            | AddFileResult::SiteMovedCoords { ref site, .. }
            | AddFileResult::SiteMovedStation { new: ref site, .. } => site,
        };

        let site_id = if let Some(id) = site.id.as_ref() {
            id
        } else {
            return AddFileResult::Error(BufkitDataErr::GeneralError("Logic Error".to_owned()));
        };

        let file_name = self.compressed_file_name(site_id, model, init_time);

        match std::fs::File::create(self.data_root().join(&file_name))
            .map_err(BufkitDataErr::IO)
            .and_then(|file| {
                let mut encoder =
                    flate2::write::GzEncoder::new(file, flate2::Compression::default());
                encoder
                    .write_all(text_data.as_bytes())
                    .map_err(BufkitDataErr::IO)
            })
            .and_then(|_| {
                self.db_conn
                    .execute(
                        include_str!("add_data/add_file.sql"),
                        &[
                            &site.station_num as &dyn rusqlite::types::ToSql,
                            &model.as_static_str() as &dyn rusqlite::types::ToSql,
                            &init_time as &dyn rusqlite::types::ToSql,
                            &end_time,
                            &file_name,
                        ],
                    )
                    .map_err(BufkitDataErr::Database)
            }) {
            Ok(_) => {}
            Err(err) => return AddFileResult::Error(err),
        }

        site_validation
    }

    /// Add a site to the list of sites.
    pub fn add_site(&self, site: &Site, coords: Coords) -> Result<(), BufkitDataErr> {
        self.db_conn.execute(
            "INSERT OR IGNORE INTO coords (station_num, lat, lon) VALUES (?1, ?2, ?3)",
            &[
                &site.station_num as &dyn rusqlite::types::ToSql,
                &coords.lat,
                &coords.lon,
            ],
        )?;

        let (lat, lon): (f64, f64) = self.db_conn.query_row(
            "
                SELECT COUNT(lat), SUM(lat), SUM(lon) FROM coords 
                WHERE station_num = ?1
            ",
            &[&site.station_num],
            |row| {
                let cnt: f64 = row.get(0)?;
                let lats: f64 = row.get(1)?;
                let lons: f64 = row.get(2)?;
                Ok((lats / cnt, lons / cnt))
            },
        )?;

        self.db_conn.execute(
            include_str!("add_data/add_site.sql"),
            &[
                &site.station_num as &dyn rusqlite::ToSql,
                &site.name,
                &site.state.map(|state_prov| state_prov.as_static_str())
                    as &dyn rusqlite::types::ToSql,
                &site.notes,
                &site.time_zone.map(|tz| tz.local_minus_utc()),
                &site.auto_download,
                &lat,
                &lon,
            ],
        )?;

        self.check_update_site_id(site)?;

        Ok(())
    }

    #[inline]
    fn check_update_site_id(&self, site: &Site) -> Result<(), BufkitDataErr> {
        if let Some(ref site_id) = site.id {
            let mut needs_insert = true;

            if let Some(other_site) = self.site_for_id(site_id) {
                if other_site.station_num != site.station_num {
                    self.db_conn.execute(
                        include_str!("add_data/remove_site_id_by_station_num.sql"),
                        &[&other_site.station_num],
                    )?;
                } else {
                    needs_insert = false;
                }
            }

            if needs_insert {
                self.db_conn.execute(
                    include_str!("add_data/insert_site_id.sql"),
                    &[
                        &site.station_num as &dyn rusqlite::ToSql,
                        &site_id.to_uppercase() as &dyn rusqlite::ToSql,
                    ],
                )?;
            }
        }

        Ok(())
    }

    /// Modify a site's values.
    pub fn update_site(&self, site: &Site) -> Result<(), BufkitDataErr> {
        self.db_conn.execute(
            include_str!("add_data/update_site.sql"),
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

    /// Remove a file from the archive.
    pub fn remove(
        &self,
        site: &Site,
        model: Model,
        init_time: &chrono::NaiveDateTime,
    ) -> Result<(), BufkitDataErr> {
        let file_name: String = self.db_conn.query_row(
            include_str!("add_data/find_file_name.sql"),
            &[
                &site.station_num as &dyn rusqlite::types::ToSql,
                &model.as_static_str() as &dyn rusqlite::types::ToSql,
                init_time as &dyn rusqlite::types::ToSql,
            ],
            |row| row.get(0),
        )?;

        std::fs::remove_file(self.data_root().join(file_name)).map_err(BufkitDataErr::IO)?;

        self.db_conn.execute(
            include_str!("add_data/delete_file.sql"),
            &[
                &site.station_num as &dyn rusqlite::types::ToSql,
                &model.as_static_str() as &dyn rusqlite::types::ToSql,
                init_time as &dyn rusqlite::types::ToSql,
            ],
        )?;

        Ok(())
    }

    fn validate_site(&self, site: Site, coords: Coords) -> AddFileResult {
        let site_by_num = self.site(site.station_num);
        let site_by_id = self.site_for_id(site.id.as_ref().unwrap());

        let result = match (site_by_num, site_by_id) {
            (Some(site_by_num), Some(site_by_id)) => {
                if site_by_num.station_num == site_by_id.station_num {
                    AddFileResult::Ok(site_by_num)
                } else {
                    let old = site_by_num.clone();
                    let new = Site {
                        id: site_by_id.id,
                        ..site_by_num
                    };
                    match self.check_update_site_id(&new) {
                        Ok(()) => {}
                        Err(err) => return AddFileResult::Error(err),
                    }
                    AddFileResult::SiteMovedStation { old, new }
                }
            }
            (None, None) => self
                .add_site(&site, coords)
                .map(|_| AddFileResult::New(site))
                .unwrap_or_else(|err| AddFileResult::Error(err)),
            (Some(site_by_num), None) => {
                // New station id for an old station_num return SiteMovedStation
                let old = site_by_num.clone();
                let new = Site {
                    id: site.id,
                    ..site_by_num
                };
                match self.check_update_site_id(&new) {
                    Ok(()) => {}
                    Err(err) => return AddFileResult::Error(err),
                }
                AddFileResult::SiteMovedStation { old, new }
            }
            (None, Some(site_by_id)) => {
                let old = site_by_id.clone();
                let new = site;
                match self.check_update_site_id(&new) {
                    Ok(()) => {}
                    Err(err) => return AddFileResult::Error(err),
                }
                AddFileResult::SiteMovedStation { old, new }
            }
        };

        match result {
            AddFileResult::Ok(site) => {
                // Check for move coordinates
                // return appropriate value, Ok, or SiteMovedCoords
                unimplemented!();
            }
            result => return result,
        }
    }

    fn parse_site_info(
        text: &str,
        site_id: String,
    ) -> Result<(Site, chrono::NaiveDateTime, chrono::NaiveDateTime, Coords), BufkitDataErr> {
        let bdata = sounding_bufkit::BufkitData::init(text, &site_id)?;
        let mut iter = bdata.into_iter();

        let first = iter.next().ok_or(BufkitDataErr::NotEnoughData)?.0;
        let last = iter.last().ok_or(BufkitDataErr::NotEnoughData)?.0;

        let init_time: NaiveDateTime = first.valid_time().ok_or(BufkitDataErr::MissingValidTime)?;
        let end_time: NaiveDateTime = last.valid_time().ok_or(BufkitDataErr::MissingValidTime)?;
        let coords: Coords = first
            .station_info()
            .location()
            .map(Coords::from)
            .ok_or(BufkitDataErr::MissingStationData)?;

        let station_num: i32 = first
            .station_info()
            .station_num()
            .ok_or(BufkitDataErr::MissingStationData)?;
        let station_num: u32 = TryFrom::try_from(station_num)
            .map_err(|_| BufkitDataErr::GeneralError("negative station number?".to_owned()))?;

        let site = Site {
            station_num,
            id: Some(site_id),
            ..Site::default()
        };

        Ok((site, init_time, end_time, coords))
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
}
