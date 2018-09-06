// State abreviations
pub const STATES: &[&str] = &[
    "WA", "ID", "MT", "OR", "CA", "NV", "UT", "NM", "AZ", "CO", "WY", "NB", "TX", "IL", "IN", "OH",
    "OK", "SD", "ND", "KS", "MS", "AL", "LA", "GA", "FL", "SC", "NC", "VT", "NH", "MN", "WI", "MI",
    "NY", "CT", "RI", "DE", "MD", "VA", "WV", "AK", "HI", "IA", "MA", "ME", "PA", "KY", "TN", "MO",
];

/// Description of a site with a sounding
#[allow(missing_docs)]
#[derive(Debug, PartialEq)]
pub struct Site {
    pub id: String,
    pub name: Option<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub elev_m: Option<f64>,
    pub notes: Option<String>,
    pub state: Option<&'static str>,
}

impl Site {
    /// Return true if there is any missing data.
    pub fn incomplete(&self) -> bool {
        self.lat.is_none()
            || self.lon.is_none()
            || self.elev_m.is_none()
            || self.name.is_none()
            || self.notes.is_none()
            || self.state.is_none()
    }
}
