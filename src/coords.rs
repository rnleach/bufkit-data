//! Latitude and longitude coordinates for internal use only. For now.

/// The latitude and longitude
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Coords {
    pub lat: f64,
    pub lon: f64,
}

impl From<(f64, f64)> for Coords {
    fn from(pair: (f64, f64)) -> Self {
        Self {
            lat: pair.0,
            lon: pair.1,
        }
    }
}
