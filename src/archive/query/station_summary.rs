use crate::{
    errors::BufkitDataErr,
    models::Model,
    site::{StateProv, StationNumber},
};
use chrono::FixedOffset;
use std::{collections::HashMap, str::FromStr};
use rusqlite::Statement;

#[cfg(feature = "pylib")]
use pyo3::prelude::*;

/// A summary of the information about a station.
#[cfg_attr(feature = "pylib", pyclass(module = "bufkit_data"))]
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
    /// Coordinates
    pub coords: Vec<(f64, f64)>,
    /// The number of files in the archive related to this site.
    pub number_of_files: u32,
}

struct StationEntry {
    station_num: StationNumber,
    id: Option<String>,
    model: Option<Model>,
    name: Option<String>,
    notes: Option<String>,
    state: Option<StateProv>,
    time_zone: Option<FixedOffset>,
    lat: f64,
    lon: f64,
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

    /// Concatenate the different coordinates as a string.
    pub fn coords_as_string(&self) -> String {
        self.coords
            .iter()
            .map(|(lat, lon)| format!("({},{})", lat, lon))
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
            lat,
            lon,
            number_of_files,
        } = entry;

        let mut models = vec![];
        if let Some(model) = model {
            models.push(model);
        }

        let mut ids = vec![];
        if let Some(id) = id {
            ids.push(id);
        }

        let coords = vec![(lat, lon),];

        StationSummary {
            station_num,
            ids,
            models,
            name,
            notes,
            state,
            time_zone,
            coords,
            number_of_files,
        }
    }
}

impl crate::Archive {
    /// Get a summary of all the stations in the archive.
    pub fn station_summaries(&self) -> Result<Vec<StationSummary>, BufkitDataErr> {
        let mut stmt = self.db_conn.prepare(include_str!("station_summary.sql"))?;

        Self::process_summary_statement(&mut stmt)
    }

    /// Get a summary of all the stations in the archive near a point..
    pub fn station_summaries_near(&self, lat: f64, lon: f64) -> Result<Vec<StationSummary>, BufkitDataErr> {

        let max_lat = lat + 0.5;
        let min_lat = lat - 0.5;
        let max_lon = lon + 0.5;
        let min_lon = lon - 0.5;

        let query_str = format!(r#"
                SELECT 
                    sites.station_num, 
                    files.id, 
                    files.model, 
                    sites.name, 
                    sites.state, 
                    sites.notes, 
                    sites.tz_offset_sec, 
                    files.lat,
                    files.lon,
                    COUNT(files.station_num)
                FROM sites LEFT JOIN files ON files.station_num = sites.station_num
                WHERE files.lat > {} AND files.lat < {} AND files.lon > {} AND files.lon < {}
                GROUP BY sites.station_num, id, model, lat, lon
            "#, min_lat, max_lat, min_lon, max_lon);

        let mut stmt = self.db_conn.prepare(&query_str)?;

        let mut summaries = Self::process_summary_statement(&mut stmt)?;

        // Haversine function in kilometers for the selected point
        let distance = move |coords: &(f64, f64)| -> f64 {
            let (clat, clon) = coords;
            
            let dlat = (lat - clat).to_radians();
            let dlon = (lon - clon).to_radians();

            let lat = lat.to_radians();
            let clat = clat.to_radians();

            let a = f64::powi(f64::sin(dlat / 2.0), 2) + f64::powi(f64::sin(dlon / 2.0), 2) * f64::cos(lat) * f64::cos(clat);

            let rad = 6371.0088;
            let c = 2.0 * f64::asin(f64::sqrt(a));
            rad * c
        };

        summaries.sort_unstable_by(|left, right| {
            let left_min_dist = left.coords.iter()
                .map(distance)
                .fold(1_000_000.0, |min, val| { if val < min { val } else { min }});

            let right_min_dist = right.coords.iter()
                .map(distance)
                .fold(1_000_000.0, |min, val| { if val < min { val } else { min }});

            left_min_dist.total_cmp(&right_min_dist)

        });

        Ok(summaries)
    }

    fn process_summary_statement(stmt: &mut Statement) -> Result<Vec<StationSummary>, BufkitDataErr> {

        let mut vals: HashMap<StationNumber, StationSummary> = HashMap::new();

        stmt.query_and_then([], Self::parse_row_to_entry)?
            .for_each(|stn_entry| {
                if let Ok(stn_entry) = stn_entry {
                    if let Some(summary) = vals.get_mut(&stn_entry.station_num) {
                        if let Some(id) = stn_entry.id {
                            summary.ids.push(id);
                        }

                        if let Some(model) = stn_entry.model {
                            summary.models.push(model);
                        }

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
        let id: Option<String> = row.get(1)?;

        let model: Option<Model> = row.get::<_, Option<String>>(2).and_then(|string_opt| {
            string_opt
                .map(|string| Model::from_str(&string).map_err(|_| rusqlite::Error::InvalidQuery))
                .transpose()
        })?;

        let name: Option<String> = row.get(3)?;

        let state: Option<StateProv> = row
            .get::<_, String>(4)
            .ok()
            .and_then(|a_string| StateProv::from_str(&a_string).ok());

        let notes: Option<String> = row.get(5)?;

        let time_zone: Option<chrono::FixedOffset> =
            row.get::<_, i32>(6).ok().and_then(|offset: i32| {
                if offset < 0 {
                    chrono::FixedOffset::west_opt(offset.abs())
                } else {
                    chrono::FixedOffset::east_opt(offset)
                }
            });

        let lat: f64 = row.get(7)?;
        let lon: f64 = row.get(8)?;
        let number_of_files: u32 = row.get(9)?;

        Ok(StationEntry {
            station_num,
            id,
            model,
            name,
            state,
            notes,
            time_zone,
            lat,
            lon,
            number_of_files,
        })
    }
}

#[cfg(feature = "pylib")]
#[cfg_attr(feature = "pylib", pymethods)]
impl StationSummary {

    fn __repr__(&self) -> PyResult<String> {
        let mut buf = String::with_capacity(1024);

        buf.push_str(&format!("Station Num: {}\n", self.station_num));

        if let Some(ref nm) = self.name {
            buf.push_str("       Name: ");
            buf.push_str(nm);
            buf.push('\n');
        }

        if let Some(ref st) = self.state {
            buf.push_str("      State: ");
            buf.push_str(st.as_static_str());
            buf.push('\n');
        }

        if let Some(ref note) = self.notes {
            buf.push_str("      Notes: ");
            buf.push_str(note);
            buf.push('\n');
        }

        buf.push_str("        Ids:");
        for id in &self.ids {
            buf.push_str(id);
            buf.push(',');
        }
        buf.push('\n');

        buf.push_str("     Coords:");
        for (lat, lon) in &self.coords {
            buf.push_str(&format!("({},{})", lat, lon));
            buf.push(',');
        }
        buf.push('\n');

        buf.push_str(&format!("  Num Files: {}\n", self.number_of_files));

        Ok(buf.to_string())
    }

    #[getter]
    fn get_station_num(&self) -> StationNumber {
        self.station_num
    }

    #[getter]
    fn get_station_name(&self) -> String {
        self.name.clone().unwrap_or_else(|| "No Name".to_owned())
    }

    #[getter]
    fn get_coords(&self) -> Vec<(f64, f64)> {
        self.coords.clone()
    }

    #[getter]
    fn get_ids(&self) -> Vec<String> {
        self.ids.clone()
    }

    #[getter]
    fn get_models(&self) -> Vec<Model> {
        self.models.clone()
    }

    #[getter]
    fn get_num_of_files(&self) -> u32 {
        self.number_of_files
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
