use chrono::NaiveDateTime;
use metfor::Quantity;
use std::{convert::TryFrom, io::Write};

use super::Archive;

use crate::{
    coords::Coords,
    errors::BufkitDataErr,
    models::Model,
    site::{SiteInfo, StationNumber},
};

/// The end result of adding a file to the archive.
#[derive(Debug)]
pub enum AddFileResult {
    /// No site conflicts or changes. Includes the site as parsed from the file.
    Ok(StationNumber),
    /// This is a new site and it was added to the database as a new site.
    New(StationNumber),
    /// Some error occurred during processing.
    Error(BufkitDataErr),
    /// The site identifier provided with this file has moved to a new station number. The
    /// correlation between station numbers has been updated.
    IdMovedStation {
        /// The site information before the update.
        old: StationNumber,
        /// The site information as it exists now.
        new: StationNumber,
    },
}

impl Archive {
    /// Add a bufkit file to the archive.
    pub fn add(&self, site_id_hint: &str, model: Model, text_data: &str) -> AddFileResult {
        let site_id_hint = site_id_hint.to_uppercase();

        let (station_num, parsed_site_id, init_time, end_time, coords, elevation) =
            match Self::parse_site_info(text_data) {
                Ok(val) => val,
                Err(err) => return AddFileResult::Error(err),
            };

        if let Some(parsed_id) = parsed_site_id {
            if site_id_hint != parsed_id {
                return AddFileResult::Error(BufkitDataErr::LogicError("ids do not match."));
            }
        }
        let site_id = Some(site_id_hint);

        if let None = self.site(station_num) {
            let new_site = SiteInfo {
                station_num,
                ..SiteInfo::default()
            };
            match self.add_site(&new_site) {
                Ok(()) => {}
                Err(err) => return AddFileResult::Error(err),
            }
        }

        let file_name = self.compressed_file_name(station_num, model, init_time);

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
                        include_str!("modify/add_file.sql"),
                        &[
                            &Into::<u32>::into(station_num) as &dyn rusqlite::types::ToSql,
                            &model.as_static_str() as &dyn rusqlite::types::ToSql,
                            &init_time as &dyn rusqlite::types::ToSql,
                            &end_time,
                            &file_name,
                            &site_id,
                            &coords.lat,
                            &coords.lon,
                            &elevation.unpack(),
                        ],
                    )
                    .map_err(BufkitDataErr::Database)
            }) {
            Ok(_) => AddFileResult::Ok(station_num),
            Err(err) => AddFileResult::Error(err),
        }
    }

    /// Add a site to the list of sites.
    ///
    /// If a site with this station number already exists, return an error from the underlying
    /// database.
    pub fn add_site(&self, site: &SiteInfo) -> Result<(), BufkitDataErr> {
        self.db_conn.execute(
            include_str!("modify/add_site.sql"),
            &[
                &Into::<u32>::into(site.station_num) as &dyn rusqlite::ToSql,
                &site.name,
                &site.state.map(|state_prov| state_prov.as_static_str())
                    as &dyn rusqlite::types::ToSql,
                &site.notes,
                &site.time_zone.map(|tz| tz.local_minus_utc()),
                &site.auto_download,
            ],
        )?;

        Ok(())
    }

    /// Modify a site's values.
    pub fn update_site(&self, site: &SiteInfo) -> Result<(), BufkitDataErr> {
        self.db_conn
            .execute(
                include_str!("modify/update_site.sql"),
                &[
                    &Into::<u32>::into(site.station_num),
                    &site.state.map(|state_prov| state_prov.as_static_str())
                        as &dyn rusqlite::types::ToSql,
                    &site.name,
                    &site.notes,
                    &site.auto_download,
                    &site.time_zone.map(|tz| tz.local_minus_utc()),
                ],
            )
            .map_err(|err| err.into())
            .map(|_| {})
    }

    /// Remove a file from the archive.
    pub fn remove(
        &self,
        station_num: StationNumber,
        model: Model,
        init_time: chrono::NaiveDateTime,
    ) -> Result<(), BufkitDataErr> {
        let station_num: u32 = Into::<u32>::into(station_num);

        let file_name: String = self.db_conn.query_row(
            include_str!("modify/find_file_name.sql"),
            &[
                &station_num as &dyn rusqlite::types::ToSql,
                &model.as_static_str() as &dyn rusqlite::types::ToSql,
                &init_time as &dyn rusqlite::types::ToSql,
            ],
            |row| row.get(0),
        )?;

        std::fs::remove_file(self.data_root().join(file_name)).map_err(BufkitDataErr::IO)?;

        self.db_conn.execute(
            include_str!("modify/delete_file_from_index.sql"),
            &[
                &station_num as &dyn rusqlite::types::ToSql,
                &model.as_static_str() as &dyn rusqlite::types::ToSql,
                &init_time as &dyn rusqlite::types::ToSql,
            ],
        )?;

        Ok(())
    }

    /// Remove a site and all of its files from the archive.
    pub fn remove_site(&self, station_num: StationNumber) -> Result<(), BufkitDataErr> {
        let station_num: u32 = Into::<u32>::into(station_num);

        let mut qstmt = self
            .db_conn
            .prepare(include_str!("modify/find_all_files_for_site.sql"))?;
        let mut dstmt = self
            .db_conn
            .prepare(include_str!("modify/delete_file_by_name.sql"))?;

        let file_deletion_results: Result<Vec<()>, _> = qstmt
            .query_map(&[&station_num], |row| row.get(0))?
            .map(|res: Result<String, rusqlite::Error>| res.map_err(BufkitDataErr::Database))
            .map(|res| {
                res.and_then(|fname| {
                    std::fs::remove_file(self.data_root().join(&fname))
                        .map_err(BufkitDataErr::IO)
                        .map(|_| fname)
                })
            })
            .map(|res| {
                res.and_then(|fname| {
                    dstmt
                        .execute(&[fname])
                        .map_err(BufkitDataErr::Database)
                        .map(|_num_rows_affected| ())
                })
            })
            .collect();
        file_deletion_results?;

        self.db_conn
            .execute(include_str!("modify/delete_site.sql"), &[&station_num])?;

        Ok(())
    }

    fn parse_site_info(
        text: &str,
    ) -> Result<
        (
            StationNumber,
            Option<String>,
            chrono::NaiveDateTime,
            chrono::NaiveDateTime,
            Coords,
            metfor::Meters,
        ),
        BufkitDataErr,
    > {
        let bdata = sounding_bufkit::BufkitData::init(text, "")?;
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

        let elevation = match first.station_info().elevation().into_option() {
            Some(elev) => elev,
            None => return Err(BufkitDataErr::MissingStationData),
        };

        let station_num: i32 = first
            .station_info()
            .station_num()
            .ok_or(BufkitDataErr::MissingStationData)?;
        let station_num: StationNumber = u32::try_from(station_num)
            .map_err(|_| BufkitDataErr::GeneralError("negative station number?".to_owned()))
            .map(StationNumber::from)?;

        Ok((
            station_num,
            first
                .station_info()
                .station_id()
                .map(|id| id.to_uppercase()),
            init_time,
            end_time,
            coords,
            elevation,
        ))
    }

    fn compressed_file_name(
        &self,
        station_num: StationNumber,
        model: Model,
        init_time: chrono::NaiveDateTime,
    ) -> String {
        let file_string = init_time.format("%Y%m%d%HZ").to_string();

        format!(
            "{}_{}_{}.buf.gz",
            file_string,
            model.as_static_str(),
            station_num,
        )
    }
}

#[cfg(test)]
mod unit {
    use super::*;
    use crate::archive::unit::*; // Set up and tear down functions.

    use chrono::NaiveDate;

    #[test]
    fn test_add() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch);
    }

    #[test]
    fn test_no_duplicate_sites() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = &get_test_sites();

        for site in test_sites {
            arch.add_site(site).expect("Error adding site.");
        }

        // Try adding them again, this should fail.
        for site in test_sites {
            assert!(arch.add_site(site).is_err());
        }
    }

    #[test]
    fn test_update_site() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = &get_test_sites();

        for site in test_sites {
            arch.add_site(site).expect("Error adding site.");
        }

        const STN: StationNumber = StationNumber::new(3);

        let zootown = SiteInfo {
            station_num: StationNumber::from(STN),
            name: Some("Zootown".to_owned()),
            notes: Some("Mountains, not coast.".to_owned()),
            state: None,
            auto_download: true,
            time_zone: Some(chrono::FixedOffset::west(7 * 3600)),
        };

        arch.update_site(&zootown).expect("Error updating site.");

        assert_eq!(arch.site(STN).unwrap(), zootown);
        assert_ne!(arch.site(STN).unwrap(), test_sites[2]);
    }

    #[test]
    fn test_remove_file() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch);

        let site = StationNumber::from(727730); // Station number for KMSO
        let init_time = NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0);
        let model = Model::GFS;

        assert!(arch
            .file_exists(site, model, &init_time)
            .expect("Error checking db"));
        arch.remove(site, model, init_time)
            .expect("Error while removing.");
        assert!(!arch
            .file_exists(site, model, &init_time)
            .expect("Error checking db"));
    }

    #[test]
    fn test_remove_site() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch);

        let station_num = StationNumber::from(727730); // Station number for KMSO
        let init_time_model_pairs = [
            (NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0), Model::GFS),
            (NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0), Model::NAM),
            (NaiveDate::from_ymd(2017, 4, 1).and_hms(6, 0, 0), Model::GFS),
            (
                NaiveDate::from_ymd(2017, 4, 1).and_hms(12, 0, 0),
                Model::GFS,
            ),
            (
                NaiveDate::from_ymd(2017, 4, 1).and_hms(12, 0, 0),
                Model::NAM,
            ),
            (
                NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0),
                Model::GFS,
            ),
            (
                NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0),
                Model::NAM,
            ),
        ];

        for &(init_time, model) in &init_time_model_pairs {
            assert!(arch
                .file_exists(station_num, model, &init_time)
                .expect("Error checking db"));
        }

        arch.remove_site(station_num).expect("db error deleting.");

        for &(init_time, model) in &init_time_model_pairs {
            assert!(!arch
                .file_exists(station_num, model, &init_time)
                .expect("Error checking db"));
        }
    }
}
