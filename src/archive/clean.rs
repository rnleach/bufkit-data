//! The cleaning method for Archive is complex, so it has its own module.

use std::{collections::HashSet, io::Read, str::FromStr};

use crate::{
    coords::Coords,
    errors::BufkitDataErr,
    models::Model,
    site::{SiteInfo, StationNumber},
};

use metfor::{Meters, Quantity};

use super::{Archive, InternalSiteInfo};

struct CleanMethodInternalSiteInfo {
    station_num: StationNumber,
    model: Model,
    id: Option<String>,
    init_time: chrono::NaiveDateTime,
    end_time: chrono::NaiveDateTime,
    coords: Coords,
    elevation: Meters,
}

impl Archive {
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
        let mut files_in_index_but_not_on_file_system = index_vals.difference(&file_system_vals);
        self.remove_missing_files_from_index(&arch, &mut files_in_index_but_not_on_file_system)?;

        println!("Comparing sets for files in archive but not in the index.");
        let mut files_not_in_index = file_system_vals.difference(&index_vals);
        self.handle_files_in_archive_but_not_index(&arch, &mut files_not_in_index)?;

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
        Ok(std::fs::read_dir(&arch.data_root())?
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
                INSERT INTO files (
                    station_num, 
                    model,
                    init_time,
                    end_time,
                    file_name, 
                    id, 
                    lat, 
                    lon, 
                    elevation_m
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ",
        )?;

        arch.db_conn
            .execute("BEGIN TRANSACTION", rusqlite::NO_PARAMS)?;
        for extra_file in files_not_in_index {
            let message = if let Some(CleanMethodInternalSiteInfo {
                station_num,
                model,
                id,
                init_time,
                end_time,
                coords,
                elevation,
            }) = arch.extract_site_info_from_file(&extra_file)
            {
                if arch.site(station_num).is_none() {
                    let site = SiteInfo {
                        station_num,
                        ..SiteInfo::default()
                    };

                    arch.add_site(&site)?;
                };

                let station_num: u32 = station_num.into();

                match insert_stmt.execute(&[
                    &station_num as &dyn rusqlite::types::ToSql,
                    &model.as_static_str() as &dyn rusqlite::types::ToSql,
                    &init_time as &dyn rusqlite::types::ToSql,
                    &end_time as &dyn rusqlite::types::ToSql,
                    &extra_file,
                    &id,
                    &coords.lat,
                    &coords.lon,
                    &elevation.unpack(),
                ]) {
                    Ok(_) => format!("Added {}", extra_file),
                    Err(_) => {
                        std::fs::remove_file(arch.data_root().join(extra_file))?;
                        format!("Duplicate file removed: {}", extra_file)
                    }
                }
            } else {
                // Remove non-bufkit file
                std::fs::remove_file(arch.data_root().join(extra_file))?;
                format!("Removed non-bufkit file: {}", extra_file)
            };

            println!("{}", message);
        }
        arch.db_conn
            .execute("COMMIT TRANSACTION", rusqlite::NO_PARAMS)?;

        Ok(())
    }

    fn extract_site_info_from_file(&self, fname: &str) -> Option<CleanMethodInternalSiteInfo> {
        let tokens: Vec<&str> = fname.split(|c| c == '_' || c == '.').collect();

        if tokens.len() != 5 || tokens[3] != "buf" || tokens[4] != "gz" {
            return None;
        }

        let model = Model::from_str(tokens[1]).ok()?;

        let file = std::fs::File::open(self.data_root().join(fname)).ok()?;
        let mut decoder = flate2::read::GzDecoder::new(file);
        let mut s = String::new();
        decoder.read_to_string(&mut s).ok()?;

        let InternalSiteInfo {
            station_num,
            id: parsed_site_id,
            init_time,
            end_time,
            coords,
            elevation,
        } = Self::parse_site_info(&s).ok()?;

        let id = if parsed_site_id.is_some() {
            parsed_site_id
        } else {
            Some(tokens[2].to_owned())
        };

        Some(CleanMethodInternalSiteInfo {
            station_num,
            model,
            id,
            init_time,
            end_time,
            coords,
            elevation,
        })
    }
}
