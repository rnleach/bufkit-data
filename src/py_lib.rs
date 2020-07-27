use crate::{
    archive::Archive,
    errors::BufkitDataErr,
    models::Model,
    site::{SiteInfo, StationNumber},
};
use chrono::{NaiveDate, NaiveDateTime};
use pyo3::{
    exceptions,
    prelude::*,
    types::{PyDateAccess, PyDateTime, PyTimeAccess},
    wrap_pyfunction,
};
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
        valid_time: &PyDateTime,
    ) -> PyResult<String> {
        let model = Model::from_str(model).map_err(BufkitDataErr::from)?;
        let valid_time = convert_to_chrono(valid_time);

        self.retrieve(station_num, model, valid_time)
            .map_err(Into::into)
    }

    fn retrieve_all_in(
        &self,
        station_num: StationNumber,
        model: &str,
        start: &PyDateTime,
        end: &PyDateTime,
    ) -> PyResult<Vec<String>> {
        let model = Model::from_str(model).map_err(BufkitDataErr::from)?;
        let start = convert_to_chrono(start);
        let end = convert_to_chrono(end);

        self.retrieve_all_valid_in(station_num, model, start, end)
            .map(|iter| iter.collect())
            .map_err(Into::into)
    }

    fn id_to_station_num(&self, id: &str, model: &str) -> PyResult<StationNumber> {
        let model = Model::from_str(model).map_err(BufkitDataErr::from)?;
        self.station_num_for_id(id, model).map_err(Into::into)
    }

    fn last_id(&self, py: Python, station_num: StationNumber, model: &str) -> PyResult<PyObject> {
        let model = Model::from_str(model).map_err(BufkitDataErr::from)?;
        match self.most_recent_id(station_num, model)? {
            Some(val) => Ok(PyObject::from_py(val, py)),
            None => Ok(py.None()),
        }
    }

    fn all_ids(&self, station_num: StationNumber, model: &str) -> PyResult<Vec<String>> {
        let model = Model::from_str(model).map_err(BufkitDataErr::from)?;
        self.ids(station_num, model).map_err(Into::into)
    }

    fn info_for_stn_num(&self, py: Python, station_num: StationNumber) -> PyResult<PyObject> {
        match self.site(station_num) {
            Some(site_info) => Ok(site_info.into_py(py)),
            None => Ok(py.None()),
        }
    }

    /// Get a list of all sites in the archive.
    fn all_sites(&self) -> PyResult<Vec<SiteInfo>> {
        self.sites().map_err(Into::into)
    }
}

#[pyfunction]
fn all_models() -> Vec<String> {
    Model::iter()
        .map(|m| m.as_static_str().to_owned())
        .collect()
}

fn convert_to_chrono(dt: &PyDateTime) -> NaiveDateTime {
    let year = dt.get_year();
    let month: u32 = dt.get_month().into();
    let day: u32 = dt.get_day().into();
    let hour: u32 = dt.get_hour().into();
    let minute: u32 = dt.get_minute().into();
    let second: u32 = dt.get_second().into();
    NaiveDate::from_ymd(year, month, day).and_hms(hour, minute, second)
}

/// Read only access to a bufkit-data archive.
#[pymodule]
fn bufkit_data(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Archive>()?;
    m.add_class::<StationNumber>()?;
    m.add_class::<SiteInfo>()?;
    m.add_wrapped(wrap_pyfunction!(all_models))?;

    Ok(())
}

impl std::convert::From<BufkitDataErr> for PyErr {
    fn from(err: BufkitDataErr) -> PyErr {
        exceptions::Exception::py_err(err.to_string())
    }
}
