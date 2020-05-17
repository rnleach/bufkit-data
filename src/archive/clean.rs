//! The cleaning method for Archive is complex, so it has its own module.

use std::{collections::HashSet, convert::TryFrom, io::Read, str::FromStr};

use crate::{errors::BufkitDataErr, models::Model, site::Site};

use super::Archive;

// FIXME: Completely redo this.
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
                let site: Site = if let Some(site_found) = arch.site(site.station_num) {
                    site_found
                } else {
                    arch.add_site(&site)?;
                    site
                };

                match insert_stmt.execute(&[
                    &site.station_num,
                    &model.as_static_str() as &dyn rusqlite::types::ToSql,
                    &init_time as &dyn rusqlite::types::ToSql,
                    &end_time as &dyn rusqlite::types::ToSql,
                    &extra_file,
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
            if let Some(site) = arch.site(orphan as u32) {
                println!("     {}", site);
            } else {
                println!("     {} - unknown", orphan);
            }
        }

        Ok(())
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

        let file = std::fs::File::open(self.data_root().join(fname)).ok()?;
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
}
