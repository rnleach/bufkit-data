use std::fmt::Display;

/// New type wrapper for a station number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StationNumber(u32);

impl From<u32> for StationNumber {
    fn from(val: u32) -> Self {
        StationNumber(val)
    }
}

impl Into<u32> for StationNumber {
    fn into(self) -> u32 {
        self.0
    }
}

impl Into<i64> for StationNumber {
    fn into(self) -> i64 {
        i64::from(self.0)
    }
}

impl Display for StationNumber {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(formatter, "{}", self.0)
    }
}

impl StationNumber {
    /// Test to see if this is a valid station number.
    pub fn is_valid(self) -> bool {
        self.0 > 0
    }

    /// Create a new one.
    pub const fn new(num: u32) -> Self {
        StationNumber(num)
    }
}
