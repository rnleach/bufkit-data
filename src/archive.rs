//! An archive of bufkit soundings.

#[cfg(test)]
use crate::{BufkitDataErr, Inventory, Model, Site, StateProv};
use std::path::PathBuf;

/// The archive.
#[derive(Debug)]
pub struct Archive {
    root: PathBuf,                 // The root directory.
    db_conn: rusqlite::Connection, // An sqlite connection.
}

mod add_data;
pub use add_data::AddFileResult;
mod clean;
mod query;
mod root;

impl Archive {
    /// Check to see if a file is present in the archive and it is retrieveable.
    #[cfg(test)]
    fn file_exists(
        &self,
        site: &Site,
        model: Model,
        init_time: &chrono::NaiveDateTime,
    ) -> Result<bool, BufkitDataErr> {
        let num_records: i32 = self.db_conn.query_row(
            "SELECT COUNT(*) FROM files WHERE station_num = ?1 AND model = ?2 AND init_time = ?3",
            &[
                &site.station_num as &dyn rusqlite::types::ToSql,
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
    use crate::{coords::Coords, Model};

    use std::fs::read_dir;

    use chrono::NaiveDate;
    use tempdir::TempDir;

    use sounding_bufkit::BufkitFile;

    // struct to hold temporary data for tests.
    struct TestArchive {
        tmp: TempDir,
        arch: Archive,
    }

    // Function to create a new archive to test.
    fn create_test_archive() -> Result<TestArchive, BufkitDataErr> {
        let tmp = TempDir::new("bufkit-data-test-archive")?;
        let arch = Archive::create(&tmp.path())?;

        Ok(TestArchive { tmp, arch })
    }

    // Function to fetch a list of test files.
    fn get_test_data() -> Result<Vec<(String, Model, String)>, BufkitDataErr> {
        let path = PathBuf::new().join("example_data");

        let files = read_dir(path)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                entry.file_type().ok().and_then(|ft| {
                    if ft.is_file() {
                        Some(entry.path())
                    } else {
                        None
                    }
                })
            });

        let mut to_return = vec![];

        for path in files {
            let bufkit_file = BufkitFile::load(&path)?;

            let model = if path.to_string_lossy().to_string().contains("gfs") {
                Model::GFS
            } else {
                Model::NAM
            };
            let site = if path.to_string_lossy().to_string().contains("kmso") {
                "kmso".to_owned()
            } else {
                panic!("Unprepared for this test data!");
            };

            let raw_string = bufkit_file.raw_text();

            to_return.push((site, model, raw_string.to_owned()))
        }

        Ok(to_return)
    }

    // Function to fill the archive with some example data.
    fn fill_test_archive(arch: &mut Archive) -> Result<(), BufkitDataErr> {
        let test_data = get_test_data().expect("Error loading test data.");

        for (site, model, raw_data) in test_data {
            match arch.add(&site, model, &raw_data) {
                AddFileResult::Ok(_) | AddFileResult::New(_) => {}
                AddFileResult::Error(err) => {
                    println!("{:?}", err);
                    panic!("Test archivve error filling.");
                }
                _ => panic!("Test archive error filling."),
            }
        }
        Ok(())
    }

    #[test]
    fn test_archive_create_new() {
        assert!(create_test_archive().is_ok());
    }

    #[test]
    fn test_archive_connect() {
        let TestArchive { tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");
        drop(arch);

        assert!(Archive::connect(&tmp.path()).is_ok());
        assert!(Archive::connect(&"unlikely_directory_in_my_project").is_err());
    }

    #[test]
    fn test_get_root() {
        let TestArchive { tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let root = arch.root();
        assert_eq!(root, tmp.path());
    }

    #[test]
    fn test_sites_round_trip() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = &[
            (
                Site {
                    id: Some("kord".to_uppercase()),
                    station_num: 1,
                    name: Some("Chicago/O'Hare".to_owned()),
                    notes: Some("Major air travel hub.".to_owned()),
                    state: Some(StateProv::IL),
                    auto_download: false,
                    time_zone: None,
                },
                Coords {
                    lat: 41.979_72,
                    lon: -87.904_44,
                },
            ),
            (
                Site {
                    id: Some("ksea".to_uppercase()),
                    station_num: 2,
                    name: Some("Seattle".to_owned()),
                    notes: Some("A coastal city with coffe and rain".to_owned()),
                    state: Some(StateProv::WA),
                    auto_download: true,
                    time_zone: Some(chrono::FixedOffset::west(8 * 3600)),
                },
                Coords {
                    lat: 47.444_72,
                    lon: -122.313_61,
                },
            ),
            (
                Site {
                    id: Some("kmso".to_uppercase()),
                    station_num: 3,
                    name: Some("Missoula".to_owned()),
                    notes: Some("In a valley.".to_owned()),
                    state: None,
                    auto_download: true,
                    time_zone: Some(chrono::FixedOffset::west(7 * 3600)),
                },
                Coords {
                    lat: 46.920_83,
                    lon: -114.092_50,
                },
            ),
        ];

        for &(ref site, coords) in test_sites {
            arch.add_site(site, coords).expect("Error adding site.");
        }

        assert!(arch.site_for_id("ksea").is_some());
        assert!(arch.site_for_id("kord").is_some());
        assert!(arch.site_for_id("xyz").is_none());

        let retrieved_sites = arch.sites().expect("Error retrieving sites.");

        for site in retrieved_sites {
            println!("{:#?}", site);
            assert!(
                test_sites
                    .iter()
                    .find(|(st, _)| st.station_num == site.station_num)
                    .is_some()
                    && test_sites.iter().find(|(st, _)| st.id == site.id).is_some()
            );
        }
    }

    #[test]
    fn test_get_site_info() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = &[
            (
                Site {
                    id: Some("kord".to_uppercase()),
                    station_num: 1,
                    name: Some("Chicago/O'Hare".to_owned()),
                    notes: Some("Major air travel hub.".to_owned()),
                    state: Some(StateProv::IL),
                    auto_download: false,
                    time_zone: None,
                },
                Coords {
                    lat: 41.979_72,
                    lon: -87.904_44,
                },
            ),
            (
                Site {
                    id: Some("ksea".to_uppercase()),
                    station_num: 2,
                    name: Some("Seattle".to_owned()),
                    notes: Some("A coastal city with coffe and rain".to_owned()),
                    state: Some(StateProv::WA),
                    auto_download: true,
                    time_zone: Some(chrono::FixedOffset::west(8 * 3600)),
                },
                Coords {
                    lat: 47.444_72,
                    lon: -122.313_61,
                },
            ),
            (
                Site {
                    id: Some("kmso".to_uppercase()),
                    station_num: 3,
                    name: Some("Missoula".to_owned()),
                    notes: Some("In a valley.".to_owned()),
                    state: None,
                    auto_download: true,
                    time_zone: Some(chrono::FixedOffset::west(7 * 3600)),
                },
                Coords {
                    lat: 46.920_83,
                    lon: -114.092_50,
                },
            ),
        ];

        for &(ref site, coords) in test_sites {
            arch.add_site(site, coords).expect("Error adding site.");
        }

        assert_eq!(arch.site_for_id("ksea").unwrap(), test_sites[1].0);
    }

    #[test]
    fn test_set_site_info() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = &[
            (
                Site {
                    id: Some("kord".to_uppercase()),
                    station_num: 1,
                    name: Some("Chicago/O'Hare".to_owned()),
                    notes: Some("Major air travel hub.".to_owned()),
                    state: Some(StateProv::IL),
                    auto_download: false,
                    time_zone: None,
                },
                Coords {
                    lat: 41.979_72,
                    lon: -87.904_44,
                },
            ),
            (
                Site {
                    id: Some("ksea".to_uppercase()),
                    station_num: 2,
                    name: Some("Seattle".to_owned()),
                    notes: Some("A coastal city with coffe and rain".to_owned()),
                    state: Some(StateProv::WA),
                    auto_download: true,
                    time_zone: Some(chrono::FixedOffset::west(8 * 3600)),
                },
                Coords {
                    lat: 47.444_72,
                    lon: -122.313_61,
                },
            ),
            (
                Site {
                    id: Some("kmso".to_uppercase()),
                    station_num: 3,
                    name: Some("Missoula".to_owned()),
                    notes: Some("In a valley.".to_owned()),
                    state: None,
                    auto_download: true,
                    time_zone: Some(chrono::FixedOffset::west(7 * 3600)),
                },
                Coords {
                    lat: 46.920_83,
                    lon: -114.092_50,
                },
            ),
        ];

        for &(ref site, coords) in test_sites {
            arch.add_site(site, coords).expect("Error adding site.");
        }

        let zootown = Site {
            station_num: 3,
            id: Some("kmso".to_uppercase()),
            name: Some("Zootown".to_owned()),
            notes: Some("Mountains, not coast.".to_owned()),
            state: None,
            auto_download: true,
            time_zone: Some(chrono::FixedOffset::west(7 * 3600)),
        };

        arch.update_site(&zootown).expect("Error updating site.");

        assert_eq!(arch.site_for_id("kmso").unwrap(), zootown);
        assert_ne!(arch.site_for_id("kmso").unwrap(), test_sites[2].0);
    }

    #[test]
    fn test_models_for_site() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let kmso = arch.site_for_id("kmso").expect("Error retreiving MSO");

        let models = arch.models(&kmso).expect("Error querying archive.");

        assert!(models.contains(&Model::GFS));
        assert!(models.contains(&Model::NAM));
        assert!(!models.contains(&Model::NAM4KM));
    }

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
    fn test_adding_duplicates() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let start = NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0);
        let end = NaiveDate::from_ymd(2100, 1, 1).and_hms(0, 0, 0);

        let kmso = arch.site_for_id("kmso").expect("Error retreiving MSO");

        assert_eq!(
            arch.init_times_for_soundings_valid_between(start, end, &kmso, Model::GFS)
                .expect("db error")
                .iter()
                .count(),
            4
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

    #[test]
    fn test_get_most_recent_file() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let kmso = arch.site_for_id("kmso").unwrap();

        let init_time = arch
            .most_recent_init_time(&kmso, Model::GFS)
            .expect("Error getting valid time.");

        assert_eq!(init_time, NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0));

        arch.most_recent_file(&kmso, Model::GFS)
            .expect("Failed to retrieve sounding.");
    }

    #[test]
    fn test_file_exists() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let kmso = arch.site_for_id("kmso").unwrap();

        println!("Checking for files that should exist.");
        assert!(arch
            .file_exists(
                &kmso,
                Model::GFS,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(arch
            .file_exists(
                &kmso,
                Model::GFS,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(6, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(arch
            .file_exists(
                &kmso,
                Model::GFS,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(12, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(arch
            .file_exists(
                &kmso,
                Model::GFS,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0)
            )
            .expect("Error checking for existence"));

        println!("Checking for files that should NOT exist.");
        assert!(!arch
            .file_exists(
                &kmso,
                Model::GFS,
                &NaiveDate::from_ymd(2018, 4, 1).and_hms(0, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(!arch
            .file_exists(
                &kmso,
                Model::GFS,
                &NaiveDate::from_ymd(2018, 4, 1).and_hms(6, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(!arch
            .file_exists(
                &kmso,
                Model::GFS,
                &NaiveDate::from_ymd(2018, 4, 1).and_hms(12, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(!arch
            .file_exists(
                &kmso,
                Model::GFS,
                &NaiveDate::from_ymd(2018, 4, 1).and_hms(18, 0, 0)
            )
            .expect("Error checking for existence"));
    }

    #[test]
    fn test_remove_file() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let site = arch.site_for_id("kmso").unwrap();
        let init_time = NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0);
        let model = Model::GFS;

        assert!(arch
            .file_exists(&site, model, &init_time)
            .expect("Error checking db"));
        arch.remove(&site, model, &init_time)
            .expect("Error while removing.");
        assert!(!arch
            .file_exists(&site, model, &init_time)
            .expect("Error checking db"));
    }
}
