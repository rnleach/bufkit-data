//! An archive of bufkit soundings.

use crate::{coords::Coords, errors::BufkitDataErr, site::StationNumber};

#[cfg(feature = "pylib")]
use pyo3::prelude::*;

use std::convert::TryFrom;

/// The archive.
#[cfg_attr(feature = "pylib", pyclass(module = "bufkit_data", unsendable))]
#[derive(Debug)]
pub struct Archive {
    root: std::path::PathBuf,      // The root directory.
    db_conn: rusqlite::Connection, // An sqlite connection.
}

mod clean;
mod modify;

mod query;
pub use query::StationSummary;

mod root;

struct InternalSiteInfo {
    station_num: StationNumber,
    id: Option<String>,
    init_time: chrono::NaiveDateTime,
    end_time: chrono::NaiveDateTime,
    coords: Coords,
    elevation: metfor::Meters,
}

impl Archive {
    fn parse_site_info(text: &str) -> Result<InternalSiteInfo, BufkitDataErr> {
        let bdata = sounding_bufkit::BufkitData::init(text, "")?;
        let mut iter = bdata.into_iter();

        let first = iter.next().ok_or(BufkitDataErr::NotEnoughData)?.0;
        let last = iter.last().ok_or(BufkitDataErr::NotEnoughData)?.0;

        let init_time: chrono::NaiveDateTime =
            first.valid_time().ok_or(BufkitDataErr::MissingValidTime)?;
        let end_time: chrono::NaiveDateTime =
            last.valid_time().ok_or(BufkitDataErr::MissingValidTime)?;
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

        Ok(InternalSiteInfo {
            station_num,
            id: first
                .station_info()
                .station_id()
                .map(|id| id.to_uppercase()),
            init_time,
            end_time,
            coords,
            elevation,
        })
    }
}

#[cfg(test)]
mod unit {
    use super::*;
    use crate::{Model, SiteInfo, StateProv, StationNumber};

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
    pub(super) fn get_test_data() -> [(String, Model, String); 7] {
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
            match arch.add(site, None, None, *model, raw_data) {
                Ok(_) => {}
                Err(err) => {
                    println!("{:?}", err);
                    panic!("Test archive error filling.");
                }
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
                time_zone: None,
            },
            SiteInfo {
                station_num: StationNumber::from(2),
                name: Some("Seattle".to_owned()),
                notes: Some("A coastal city with coffe and rain".to_owned()),
                state: Some(StateProv::WA),
                time_zone: Some(chrono::FixedOffset::west_opt(8 * 3600).unwrap()),
            },
            SiteInfo {
                station_num: StationNumber::from(3),
                name: Some("Missoula".to_owned()),
                notes: Some("In a valley.".to_owned()),
                state: None,
                time_zone: Some(chrono::FixedOffset::west_opt(7 * 3600).unwrap()),
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

    #[test]
    fn test_files_round_trip() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_data = get_test_data();

        for (site, model, raw_data) in test_data.iter() {
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

            let site = match arch.add(site, None, None, *model, raw_data) {
                Ok(site) => site,
                x => panic!("Error adding site: {:?}", x),
            };

            dbg!(&site);

            let recovered_str = arch
                .retrieve(site, *model, init_time)
                .expect("Failure to load");

            assert!(raw_data == &recovered_str);
        }
    }

    #[test]
    fn test_adding_duplicates() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        let kmso = StationNumber::from(727730); // Station number for KMSO

        fill_test_archive(&mut arch);

        assert_eq!(
            arch.inventory(kmso, Model::GFS)
                .expect("db error")
                .iter()
                .count(),
            3
        );
        assert_eq!(
            arch.inventory(kmso, Model::NAM)
                .expect("db error")
                .iter()
                .count(),
            3
        );

        // Do it again and make sure the numbers are the same.
        fill_test_archive(&mut arch);

        assert_eq!(
            arch.inventory(kmso, Model::GFS)
                .expect("db error")
                .iter()
                .count(),
            3
        );
        assert_eq!(
            arch.inventory(kmso, Model::NAM)
                .expect("db error")
                .iter()
                .count(),
            3
        );
    }
}
