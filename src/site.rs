/// Description of a site with a sounding
#[allow(missing_docs)]
#[derive(Debug, PartialEq)]
pub struct Site {
    pub id: String,
    pub name: Option<String>,
    pub notes: Option<String>,
    pub state: Option<StateProv>,
    pub auto_download: bool,
}

impl Site {
    /// Return true if there is any missing data.
    pub fn incomplete(&self) -> bool {
        self.name.is_none() || self.notes.is_none() || self.state.is_none()
    }
}

// State/Providence
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
        };

        let incomplete_site = Site {
            id: "kxly".to_owned(),
            name: Some("tv station".to_owned()),
            state: Some(StateProv::VI),
            notes: None,
            auto_download: true,
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
