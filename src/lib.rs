//! Crate to manage and interface with an archive of
//! [bufkit](https://training.weather.gov/wdtd/tools/BUFKIT/index.php) files.
//!
//! This supports a set of command line tools for utilizing the archive. In general, it may be
//! useful to anyone interested in archiving bufkit files.
//!
//! The current implementation uses an [sqlite](https://www.sqlite.org/index.html) database to keep
//! track of files stored in a common directory. The files are compressed, and so should only be
//! accessed via the API provided by this crate.
//!
//! ## Python integration
//! When compiled with the `pylib` feature it minimally supports access from Python. At this time it
//! only supports reading files from the archive.
//!
//! For use with python, I recommend using a virtualenv and
//! [maturin](https://github.com/pyo3/maturin). Once the virtualenv is activated,
//! `pip install maturin` and install the bufkit_data package by going into the directory
//! bufkit-data is cloned into and running:
//!
//! ```shell
//! maturin develop --release --strip --cargo-extra-args="--features pylib"
//!
//! ```
//!
//! After this installation, you should be able to use `bufkit_data` from python with:
//! ```python
//! import bufkit_data as bd
//!
//! arch = bd.Archive("Path/to/my_archive")
//! ord = arch.id_to_station_num("kord", "nam4km")
//! most_recent_ord_nam = arch.most_recent(ord, "nam4km")
//!
//! from datetime import datetime as dt
//! valid_time = dt(2020, 5, 5, 12, 0)
//!
//! ord = arch.id_to_station_num("kord", "gfs")
//! old_ord_gfs = arch.retrieve_sounding(ord, "gfs", valid_time)
//!
//! ```
#![deny(missing_docs)]

//
// Public API
//
pub use crate::archive::{Archive, StationSummary};
pub use crate::errors::BufkitDataErr;
pub use crate::models::Model;
pub use crate::site::{SiteInfo, StateProv, StationNumber};


//
// Implementation only
//

#[cfg(feature = "pylib")]
use pyo3::prelude::*;

#[cfg_attr(feature = "pylib", pymodule)]
#[cfg(feature = "pylib")]
mod bufkit_data {

    #[pymodule_export]
    use crate::{
        archive::{Archive, StationSummary},
        models::Model,
        site::{SiteInfo, StationNumber},
    };

    use crate::errors::BufkitDataErr;
    use super::distance;

    use chrono:: NaiveDateTime;
    use pyo3::{ exceptions, prelude::*, IntoPyObjectExt};
    use std::str::FromStr;
    use strum::IntoEnumIterator;

    #[pymethods]
    impl Archive {

        #[new]
        fn connect_to(root: String) -> PyResult<Self> {
            Ok(Archive::connect(&root)?)
        }

        #[getter]
        fn get_root(&self) -> PyResult<String> {
            Ok(self
                .root()
                .to_str()
                .map(String::from)
                .ok_or(BufkitDataErr::LogicError(
                    "unable to convert path to string",
                ))?)
        }

        fn most_recent(&self, station_num: StationNumber, model: &str) -> PyResult<String> {
            let model = Model::from_str(model).map_err(BufkitDataErr::from)?;
            self.retrieve_most_recent(station_num, model)
                .map_err(Into::into)
        }

        fn retrieve_sounding(
            &self,
            station_num: StationNumber,
            model: &str,
            valid_time: NaiveDateTime,
        ) -> PyResult<String> {
            let model = Model::from_str(model).map_err(BufkitDataErr::from)?;

            self.retrieve(station_num, model, valid_time)
                .map_err(Into::into)
        }

        fn retrieve_all_in(
            &self,
            station_num: StationNumber,
            model: &str,
            start: NaiveDateTime,
            end: NaiveDateTime,
        ) -> PyResult<Vec<String>> {
            let model = Model::from_str(model).map_err(BufkitDataErr::from)?;

            self.retrieve_all_valid_in(station_num, model, start, end)
                .map(|iter| iter.collect())
                .map_err(Into::into)
        }

        fn id_to_station_num(&self, id: &str, model: &str) -> PyResult<StationNumber> {
            let model = Model::from_str(model).map_err(BufkitDataErr::from)?;
            self.station_num_for_id(id, model).map_err(Into::into)
        }

        fn last_id(&self, py: Python, station_num: StationNumber, model: &str) -> PyResult<Py<PyAny>> {
            let model = Model::from_str(model).map_err(BufkitDataErr::from)?;
            match self.most_recent_id(station_num, model)? {
                Some(val) =>val.into_py_any(py),
                None => Ok(py.None()),
            }
        }

        fn all_ids(&self, station_num: StationNumber, model: &str) -> PyResult<Vec<String>> {
            let model = Model::from_str(model).map_err(BufkitDataErr::from)?;
            self.ids(station_num, model).map_err(Into::into)
        }

        fn info_for_stn_num(&self, py: Python, station_num: StationNumber) -> PyResult<Py<PyAny>> {
            match self.site(station_num) {
                Some(site_info) => site_info.into_py_any(py),
                None => Ok(py.None()),
            }
        }

        /// Get a list of all sites in the archive.
        fn all_sites(&self) -> PyResult<Vec<SiteInfo>> {
            self.sites().map_err(Into::into)
        }

        /// Get a list of stations near a point and their distance from the point in miles.
        fn get_station_summaries_near(&self, lat: f64, lon: f64) -> PyResult<Vec<(StationSummary, f64)>> {
            let sums = self.station_summaries_near(lat, lon)?;

            let result: Vec<(StationSummary, f64)> = sums.into_iter().map(|sum| {
                   let (lat2, lon2) = sum.coords[0];
                   (sum, distance(lat, lon, lat2, lon2))
                })
                .collect();
            
            Ok(result)
        }
    }

    #[pyfunction]
    pub fn all_models() -> Vec<String> {
        Model::iter()
            .map(|m| m.as_static_str().to_owned())
            .collect()
    }

    impl std::convert::From<BufkitDataErr> for PyErr {
        fn from(err: BufkitDataErr) -> PyErr {
            exceptions::PyException::new_err(err.to_string())
        }
    }
}

#[cfg(feature = "pylib")]
fn distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64)  -> f64 {
        
        let dlat = (lat1 - lat2).to_radians();
        let dlon = (lon1 - lon2).to_radians();

        let lat1 = lat1.to_radians();
        let lat2 = lat2.to_radians();

        let a = f64::powi(f64::sin(dlat / 2.0), 2) + f64::powi(f64::sin(dlon / 2.0), 2) * f64::cos(lat1) * f64::cos(lat2);

        let rad = 3958.761;
        let c = 2.0 * f64::asin(f64::sqrt(a));
        rad * c
}

mod archive;
mod coords;
mod errors;
mod models;
mod site;
