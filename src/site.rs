// State abreviations
pub const STATES: &[&str] = &[
    // US States
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
    "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT",
    "VA", "WA", "WV", "WI", "WY",
    // US Commonwealth and Territories
    "AS", // American Samoa
    "DC", // District of Columbia
    "FM", // Federated States of Micronesia
    "MH", // Marshall Islands
    "MP", // Northern Mariana Islands
    "PW", // Palau
    "PR", // Puerto Rico
    "VI", // Virgin Islands
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
