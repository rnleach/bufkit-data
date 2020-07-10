//! An archive of bufkit soundings.

#[cfg(test)]
use crate::{BufkitDataErr, Model, SiteInfo, StateProv, StationNumber};
use std::path::PathBuf;

/// The archive.
#[derive(Debug)]
pub struct Archive {
    root: PathBuf,                 // The root directory.
    db_conn: rusqlite::Connection, // An sqlite connection.
}

mod modify;
pub use modify::AddFileResult;
mod clean;
mod query;
mod root;

impl Archive {
    /// Check to see if a file is present in the archive and it is retrieveable.
    #[cfg(test)]
    fn file_exists(
        &self,
        site: StationNumber,
        model: Model,
        init_time: &chrono::NaiveDateTime,
    ) -> Result<bool, BufkitDataErr> {
        let num_records: i32 = self.db_conn.query_row(
            "SELECT COUNT(*) FROM files WHERE station_num = ?1 AND model = ?2 AND init_time = ?3",
            &[
                &Into::<i64>::into(site) as &dyn rusqlite::types::ToSql,
                &model.as_static_str() as &dyn rusqlite::types::ToSql,
                init_time as &dyn rusqlite::types::ToSql,
            ],
            |row| row.get(0),
        )?;

        Ok(num_records == 1)
    }
}

#[cfg(test)]
mod unit {
    use super::*;
    use crate::{Model, StationNumber};

    use tempdir::TempDir;

    // struct to hold temporary data for tests.
    pub(super) struct TestArchive {
        pub tmp: TempDir,
        pub arch: Archive,
    }

    // Function to create a new archive to test.
    pub(super) fn create_test_archive() -> Result<TestArchive, BufkitDataErr> {
        let tmp = TempDir::new("bufkit-data-test-archive")?;
        let arch = Archive::create(&tmp.path())?;

        Ok(TestArchive { tmp, arch })
    }

    // Get some simplified data for testing.
    pub(super) fn get_test_data() -> [(String, Model, String); 8] {
        [
            (
                "KMSO".to_owned(),
                Model::NAM,
                include_str!("../example_data/2017040100Z_nam_kmso.buf").to_owned(),
            ),
            (
                "KMSO".to_owned(),
                Model::GFS,
                include_str!("../example_data/2017040106Z_gfs3_kmso.buf").to_owned(),
            ),
            (
                "KMSO".to_owned(),
                Model::GFS,
                include_str!("../example_data/2017040106Z_gfs3_kmso.buf").to_owned(),
            ),
            (
                "KMSO".to_owned(),
                Model::GFS,
                include_str!("../example_data/2017040112Z_gfs3_kmso.buf").to_owned(),
            ),
            (
                "KMSO".to_owned(),
                Model::NAM,
                include_str!("../example_data/2017040112Z_nam_kmso.buf").to_owned(),
            ),
            (
                "KMSO".to_owned(),
                Model::GFS,
                include_str!("../example_data/2017040118Z_gfs3_kmso.buf").to_owned(),
            ),
            (
                "KMSO".to_owned(),
                Model::GFS,
                include_str!("../example_data/2017040118Z_gfs_kmso.buf").to_owned(),
            ),
            (
                "KMSO".to_owned(),
                Model::NAM,
                include_str!("../example_data/2017040118Z_namm_kmso.buf").to_owned(),
            ),
        ]
    }

    // Function to fill the archive with some example data.
    pub(super) fn fill_test_archive(arch: &mut Archive) {
        for (site, model, raw_data) in get_test_data().iter() {
            match arch.add(site, *model, raw_data) {
                AddFileResult::Ok(_) | AddFileResult::New(_) => {}
                AddFileResult::Error(err) => {
                    println!("{:?}", err);
                    panic!("Test archive error filling.");
                }
                _ => panic!("Test archive error filling."),
            }
        }
    }

    // A handy set of sites to use when testing sites.
    pub(super) fn get_test_sites() -> [SiteInfo; 3] {
        [
            SiteInfo {
                station_num: StationNumber::from(1),
                name: Some("Chicago/O'Hare".to_owned()),
                notes: Some("Major air travel hub.".to_owned()),
                state: Some(StateProv::IL),
                auto_download: false,
                time_zone: None,
            },
            SiteInfo {
                station_num: StationNumber::from(2),
                name: Some("Seattle".to_owned()),
                notes: Some("A coastal city with coffe and rain".to_owned()),
                state: Some(StateProv::WA),
                auto_download: true,
                time_zone: Some(chrono::FixedOffset::west(8 * 3600)),
            },
            SiteInfo {
                station_num: StationNumber::from(3),
                name: Some("Missoula".to_owned()),
                notes: Some("In a valley.".to_owned()),
                state: None,
                auto_download: true,
                time_zone: Some(chrono::FixedOffset::west(7 * 3600)),
            },
        ]
    }

    #[test]
    fn test_sites_round_trip() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = &get_test_sites();

        for site in test_sites {
            arch.add_site(site).expect("Error adding site.");
        }

        let retrieved_sites = arch.sites().expect("Error retrieving sites.");

        for site in retrieved_sites {
            println!("{:#?}", site);
            assert!(test_sites
                .iter()
                .find(|st| st.station_num == site.station_num)
                .is_some());
        }
    }

    /*
    #[test]
    fn test_adding_duplicates() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch);

        let kmso = StationNumber::from(727730); // Station number for KMSO
        let start = NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0);
        let end = NaiveDate::from_ymd(2100, 1, 1).and_hms(0, 0, 0);

        // TODO: Needs inventory to work. Use inventory to check.
        assert_eq!(
            arch.init_times_for_soundings_valid_between(start, end, &kmso, Model::GFS)
                .expect("db error")
                .iter()
                .count(),
            3
        );
        assert_eq!(
            arch.init_times_for_soundings_valid_between(start, end, &kmso, Model::NAM)
                .expect("db error")
                .iter()
                .count(),
            3
        );
    }

    #[test]
    fn test_files_round_trip() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_data = get_test_data().expect("Error loading test data.");

        for (site, model, raw_data) in test_data {
            let init_time = sounding_bufkit::BufkitData::init(&raw_data, "x")
                .unwrap()
                .into_iter()
                .next()
                .unwrap()
                .0
                .valid_time()
                .unwrap();

            dbg!(init_time);
            dbg!(&site);

            let site = match arch.add(&site, model, &raw_data) {
                AddFileResult::Ok(site) | AddFileResult::New(site) => site,
                x => panic!("Error adding site: {:?}", x),
            };

            dbg!(&site);

            let recovered_str = arch
                .retrieve(&site, model, init_time)
                .expect("Failure to load");

            assert!(raw_data == recovered_str);
        }
    }
    */
}
