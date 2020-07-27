use crate::{archive::Archive, errors::BufkitDataErr, models::Model};
use chrono::{NaiveDate, NaiveDateTime};
use pyo3::{
    exceptions,
    prelude::*,
    types::{PyDateAccess, PyDateTime, PyTimeAccess},
};
use std::str::FromStr;

#[pymethods]
impl Archive {
    #[new]
    fn connect_to(root: String) -> PyResult<Self> {
        Ok(Archive::connect(&root)?)
    }

    fn root_path(&self) -> PyResult<String> {
        Ok(self
            .root()
            .to_str()
            .map(String::from)
            .ok_or(BufkitDataErr::LogicError(
                "unable to convert path to string",
            ))?)
    }

    fn most_recent(&self, id: &str, model: &str) -> PyResult<String> {
        let model = Model::from_str(model).map_err(BufkitDataErr::from)?;
        let station_num = self.station_num_for_id(id, model)?;
        self.retrieve_most_recent(station_num, model)
            .map_err(Into::into)
    }

    fn retrieve_sounding(
        &self,
        id: &str,
        model: &str,
        valid_time: &PyDateTime,
    ) -> PyResult<String> {
        let model = Model::from_str(model).map_err(BufkitDataErr::from)?;
        let station_num = self.station_num_for_id(id, model)?;
        let valid_time = convert_to_chrono(valid_time);

        self.retrieve(station_num, model, valid_time)
            .map_err(Into::into)
    }

    fn retrieve_all_in(
        &self,
        id: &str,
        model: &str,
        start: &PyDateTime,
        end: &PyDateTime,
    ) -> PyResult<Vec<String>> {
        let model = Model::from_str(model).map_err(BufkitDataErr::from)?;
        let station_num = self.station_num_for_id(id, model)?;
        let start = convert_to_chrono(start);
        let end = convert_to_chrono(end);

        self.retrieve_all_valid_in(station_num, model, start, end)
            .map(|iter| iter.collect())
            .map_err(Into::into)
    }
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

#[pymodule]
fn bufkit_data(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Archive>()?;

    Ok(())
}

impl std::convert::From<BufkitDataErr> for PyErr {
    fn from(err: BufkitDataErr) -> PyErr {
        exceptions::Exception::py_err(err.to_string())
    }
}
