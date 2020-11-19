use super::Archive;
use crate::{
    errors::BufkitDataErr,
    models::Model,
    site::{StateProv, StationNumber},
};
use chrono::FixedOffset;
use std::{collections::HashMap, str::FromStr};

/// A summary of the information about a station.
#[derive(Debug)]
pub struct StationSummary {
    /// Station number
    pub station_num: StationNumber,
    /// List of ids associated with this site
    pub ids: Vec<String>,
    /// All the models in the archive associated with this site
    pub models: Vec<Model>,
    /// Station name, common name
    pub name: Option<String>,
    /// Notes related to the site
    pub notes: Option<String>,
    /// The state-province associated with the site.
    pub state: Option<StateProv>,
    /// The time zone offset to local standard time.
    pub time_zone: Option<FixedOffset>,
    /// The number of files in the archive related to this site.
    pub number_of_files: u32,
}

struct StationEntry {
    station_num: StationNumber,
    id: String,
    model: Model,
    name: Option<String>,
    notes: Option<String>,
    state: Option<StateProv>,
    time_zone: Option<FixedOffset>,
    number_of_files: u32,
}

impl StationSummary {
    /// Concantenate the ids into a comma separated list.
    pub fn ids_as_string(&self) -> String {
        self.ids.join(", ")
    }

    /// Concatenate the models into a comma separated list.
    pub fn models_as_string(&self) -> String {
        self.models
            .iter()
            .map(|m| m.as_static_str().to_owned())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl From<StationEntry> for StationSummary {
    fn from(entry: StationEntry) -> Self {
        let StationEntry {
            station_num,
            id,
            model,
            name,
            notes,
            state,
            time_zone,
            number_of_files,
        } = entry;
        StationSummary {
            station_num,
            ids: vec![id],
            models: vec![model],
            name,
            notes,
            state,
            time_zone,
            number_of_files,
        }
    }
}

impl Archive {
    /// Get a summary of all the stations in the archive.
    pub fn station_summaries(&self) -> Result<Vec<StationSummary>, BufkitDataErr> {
        let mut vals: HashMap<StationNumber, StationSummary> = HashMap::new();

        let mut stmt = self.db_conn.prepare(include_str!("station_summary.sql"))?;

        stmt.query_and_then(rusqlite::NO_PARAMS, Self::parse_row_to_entry)?
            .for_each(|stn_entry| {
                if let Ok(stn_entry) = stn_entry {
                    if let Some(summary) = vals.get_mut(&stn_entry.station_num) {
                        summary.ids.push(stn_entry.id);
                        summary.models.push(stn_entry.model);
                        summary.number_of_files += stn_entry.number_of_files;
                    } else {
                        vals.insert(stn_entry.station_num, StationSummary::from(stn_entry));
                    }
                }
            });

        let mut vals: Vec<StationSummary> = vals.into_iter().map(|(_, v)| v).collect();

        vals.iter_mut().for_each(|summary| {
            summary.ids.sort_unstable();
            summary.ids.dedup();
            summary.models.sort_unstable();
            summary.models.dedup();
        });

        Ok(vals)
    }

    fn parse_row_to_entry(row: &rusqlite::Row) -> Result<StationEntry, rusqlite::Error> {
        let station_num: StationNumber = row.get::<_, u32>(0).map(StationNumber::from)?;
        let id: String = row.get(1)?;

        let model: Model = row.get::<_, String>(2).and_then(|a_string| {
            Model::from_str(&a_string).map_err(|_| rusqlite::Error::InvalidQuery)
        })?;

        let name: Option<String> = row.get(3)?;

        let state: Option<StateProv> = row
            .get::<_, String>(4)
            .ok()
            .and_then(|a_string| StateProv::from_str(&a_string).ok());

        let notes: Option<String> = row.get(5)?;

        let time_zone: Option<chrono::FixedOffset> =
            row.get::<_, i32>(6).ok().map(|offset: i32| {
                if offset < 0 {
                    chrono::FixedOffset::west(offset.abs())
                } else {
                    chrono::FixedOffset::east(offset)
                }
            });

        let number_of_files: u32 = row.get(7)?;

        Ok(StationEntry {
            station_num,
            id,
            model,
            name,
            state,
            notes,
            time_zone,
            number_of_files,
        })
    }
}

#[cfg(test)]
mod unit {
    use crate::archive::unit::*; // test helpers.
    use crate::{Model, StationNumber};

    #[test]
    fn test_summaries() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch);

        let sums = arch.station_summaries().unwrap();

        for sum in sums {
            println!("{:?}", sum);

            assert_eq!(sum.ids.len(), 1);
            assert_eq!(sum.ids[0], "KMSO");

            assert_eq!(sum.models.len(), 2);
            assert!(sum.models.contains(&Model::GFS));
            assert!(sum.models.contains(&Model::NAM));

            assert_eq!(sum.station_num, StationNumber::new(727730));
            assert_eq!(sum.number_of_files, 6);
            assert!(sum.name.is_none());
            assert!(sum.notes.is_none());
            assert!(sum.time_zone.is_none());
            assert!(sum.state.is_none());
        }
    }
}
