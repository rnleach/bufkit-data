use std::io::Write;

use super::Archive;

use crate::{errors::BufkitDataErr, models::Model, site::Site};

impl Archive {
    /// Add a bufkit file to the archive.
    pub fn add(
        &self,
        site: Site,
        model: Model,
        init_time: chrono::NaiveDateTime,
        end_time: chrono::NaiveDateTime,
        text_data: &str,
    ) -> Result<Site, BufkitDataErr> {
        let site = if let Some(site_found) = self.site(site.station_num) {
            site_found
        } else {
            self.add_site(&Site {
                station_num: site.station_num,
                id: site.id.clone(),
                name: None,
                notes: None,
                state: None,
                auto_download: false,
                time_zone: None,
            })?;

            site
        };

        let site_id = if let Some(ref site_id) = site.id {
            site_id
        } else {
            return Err(BufkitDataErr::InvalidSiteId("None".to_owned()));
        };

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
        let file = std::fs::File::create(self.data_root().join(&file_name))?;
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

        Ok(site)
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
            let mut needs_insert = true;

            if let Some(other_site) = self.site_for_id(site_id) {
                if other_site.station_num != site.station_num {
                    self.db_conn.execute(
                        "DELETE FROM site_ids WHERE station_num = ?1",
                        &[&other_site.station_num],
                    )?;
                } else {
                    needs_insert = false;
                }
            }

            if needs_insert {
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

    /// Modify a site's values.
    pub fn update_site(&self, site: &Site) -> Result<(), BufkitDataErr> {
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

        std::fs::remove_file(self.data_root().join(file_name)).map_err(BufkitDataErr::IO)?;

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
