#[cfg(feature = "pylib")]
use pyo3::prelude::*;
use std::fmt::Display;

/// New type wrapper for a station number.
#[cfg_attr(feature = "pylib", pyclass(module = "bufkit_data"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StationNumber {
    num: u32,
}

impl From<u32> for StationNumber {
    fn from(val: u32) -> Self {
        StationNumber { num: val }
    }
}

impl Into<u32> for StationNumber {
    fn into(self) -> u32 {
        self.num
    }
}

impl Into<i64> for StationNumber {
    fn into(self) -> i64 {
        i64::from(self.num)
    }
}

impl Display for StationNumber {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(formatter, "{}", self.num)
    }
}

impl StationNumber {
    /// Test to see if this is a valid station number.
    pub fn is_valid(self) -> bool {
        self.num > 0
    }

    /// Create a new one.
    pub const fn new(num: u32) -> Self {
        StationNumber { num }
    }
}

#[cfg(feature = "pylib")]
#[cfg_attr(feature = "pylib", pymethods)]
impl StationNumber {

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("bufkit_data::StationNumber({})", self.num))
    }

    #[new]
    fn py_new(num: u32) -> Self {
        Self::new(num)
    }

    #[getter]
    fn get_as_number(&self) -> u32 {
        self.num
    }
}

