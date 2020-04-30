use chrono::FixedOffset;
use std::fmt::Display;
use strum_macros::{EnumIter, EnumString, IntoStaticStr};

/// Description of a site with a sounding.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Site {
    /// Station number, this should be unique to the site. Site ids sometimes change around.
    pub station_num: u32,
    /// Site ID. This is usually a 3 or 4 letter ID. These change from time to time and are not
    /// always unique to a site. If you need an identifief unique to a location, use the
    /// station_num. Because these change, it is possible to orphan a site with a site id, hence
    /// the option.
    pub id: Option<String>,
    /// A longer, more human readable name.
    pub name: Option<String>,
    /// Any relevant notes about the site.
    pub notes: Option<String>,
    /// The state or providence where this location is located. This allows querying sites by what
    /// state or providence they are in.
    pub state: Option<StateProv>,
    /// For programs that download files, this allows marking some sites for automatic download
    /// without further specification.
    pub auto_download: bool,
    /// Time zone information
    pub time_zone: Option<FixedOffset>,
}

impl Site {
    /// Return true if there is any missing data. It ignores the notes field since this is only
    /// rarely used.
    pub fn incomplete(&self) -> bool {
        self.name.is_none() || self.state.is_none() || self.time_zone.is_none()
    }
}

impl Display for Site {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        writeln!(
            formatter,
            "Site: station_num - {:6} | name - {:20} | state - {:2} | notes - {} | download - {}",
            self.station_num,
            self.id.as_deref().unwrap_or("None"),
            self.state.map(|s| s.as_static_str()).unwrap_or("None"),
            self.notes.as_deref().unwrap_or("None"),
            self.auto_download,
        )
    }
}

impl Default for Site {
    fn default() -> Self {
        Site {
            station_num: 0,
            id: None,
            name: None,
            notes: None,
            state: None,
            auto_download: false,
            time_zone: None,
        }
    }
}

/// State/Providence abreviations for declaring a state in the site.
#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, EnumString, IntoStaticStr, EnumIter)]
#[allow(missing_docs)]
pub enum StateProv {
    AL, // Alabama
    AK, // Alaska
    AZ, // Arizona
    AR, // Arkansas
    CA, // California
    CO, // Colorado
    CT, // Connecticut
    DE, // Delaware
    FL, // Florida
    GA, // Georgia
    HI, // Hawaii
    ID, // Idaho
    IL, // Illinois
    IN, // Indiana
    IA, // Iowa
    KS, // Kansas
    KY, // Kentucky
    LA, // Louisiana
    ME, // Maine
    MD, // Maryland
    MA, // Massachussetts
    MI, // Michigan
    MN, // Minnesota
    MS, // Mississippi
    MO, // Missouri
    MT, // Montana
    NE, // Nebraska
    NV, // Nevada
    NH, // New Hampshire
    NJ, // New Jersey
    NM, // New Mexico
    NY, // New York
    NC, // North Carolina
    ND, // North Dakota
    OH, // Ohio
    OK, // Oklahoma
    OR, // Oregon
    PA, // Pensylvania
    RI, // Rhode Island
    SC, // South Carolina
    SD, // South Dakota
    TN, // Tennessee
    TX, // Texas
    UT, // Utah
    VT, // Vermont
    VA, // Virginia
    WA, // Washington
    WV, // West Virginia
    WI, // Wisconsin
    WY, // Wyoming
    // US Commonwealth and Territories
    AS, // American Samoa
    DC, // District of Columbia
    FM, // Federated States of Micronesia
    MH, // Marshall Islands
    MP, // Northern Mariana Islands
    PW, // Palau
    PR, // Puerto Rico
    VI, // Virgin Islands
}

impl StateProv {
    /// Get a static string representation.
    pub fn as_static_str(self) -> &'static str {
        self.into()
    }
}

/*--------------------------------------------------------------------------------------------------
                                          Unit Tests
--------------------------------------------------------------------------------------------------*/
#[cfg(test)]
mod unit {
    use super::*;

    use std::str::FromStr;
    use strum::IntoEnumIterator;

    #[test]
    fn test_site_incomplete() {
        let complete_site = Site {
            station_num: 1,
            id: Some("kxly".to_owned()),
            name: Some("tv station".to_owned()),
            state: Some(StateProv::VI),
            notes: Some("".to_owned()),
            auto_download: false,
            time_zone: Some(FixedOffset::west(7 * 3600)),
        };

        let incomplete_site = Site {
            station_num: 1,
            id: Some("kxly".to_owned()),
            name: Some("tv station".to_owned()),
            state: None,
            notes: None,
            auto_download: true,
            time_zone: None,
        };

        assert!(!complete_site.incomplete());
        assert!(incomplete_site.incomplete());
    }

    #[test]
    fn test_to_string_for_state_prov() {
        assert_eq!(StateProv::AL.as_static_str(), "AL");
    }

    #[test]
    fn test_from_string_for_state_prov() {
        assert_eq!(StateProv::from_str("AL").unwrap(), StateProv::AL);
    }

    #[test]
    fn round_trip_strings_for_state_prov() {
        for state_prov in StateProv::iter() {
            assert_eq!(
                StateProv::from_str(state_prov.as_static_str()).unwrap(),
                state_prov
            );
        }
    }
}
