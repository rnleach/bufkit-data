use chrono::FixedOffset;

/// Description of a site with a sounding.
#[derive(Clone, Debug, PartialEq)]
pub struct Site {
    /// Site id, usually a 3 or 4 letter identifier (e.g. kord katl ksea).
    pub id: String,
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

/// State/Providence abreviations for declaring a state in the site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString, AsStaticStr, EnumIter)]
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

/*--------------------------------------------------------------------------------------------------
                                          Unit Tests
--------------------------------------------------------------------------------------------------*/
#[cfg(test)]
mod unit {
    use super::*;

    use std::str::FromStr;
    use strum::{AsStaticRef, IntoEnumIterator};

    #[test]
    fn test_site_incomplete() {
        let complete_site = Site {
            id: "kxly".to_owned(),
            name: Some("tv station".to_owned()),
            state: Some(StateProv::VI),
            notes: Some("".to_owned()),
            auto_download: false,
            time_zone: Some(FixedOffset::west(7 * 3600)),
        };

        let incomplete_site = Site {
            id: "kxly".to_owned(),
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
        assert_eq!(StateProv::AL.as_static(), "AL");
    }

    #[test]
    fn test_from_string_for_state_prov() {
        assert_eq!(StateProv::from_str("AL").unwrap(), StateProv::AL);
    }

    #[test]
    fn round_trip_strings_for_state_prov() {
        for state_prov in StateProv::iter() {
            assert_eq!(
                StateProv::from_str(state_prov.as_static()).unwrap(),
                state_prov
            );
        }
    }
}
