use chrono::FixedOffset;
use std::fmt::Display;

mod station_num;
pub use station_num::StationNumber;

mod state_prov;
pub use state_prov::StateProv;

/// Description of a site with a sounding.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SiteInfo {
    /// Station number, this should be unique to the site. Site ids sometimes change around.
    pub station_num: StationNumber,
    /// A longer, more human readable name.
    pub name: Option<String>,
    /// Any relevant notes about the site.
    pub notes: Option<String>,
    /// The state or providence where this location is located. This allows querying sites by what
    /// state or providence they are in.
    pub state: Option<StateProv>,
    /// Time zone information
    pub time_zone: Option<FixedOffset>,
    /// Mark this site for automatic updates/downloads
    pub auto_download: bool,
}

impl SiteInfo {
    /// Return true if there is any missing data. It ignores the notes field since this is only
    /// rarely used. Also, there is no requirement for a site to have an id.
    pub fn incomplete(&self) -> bool {
        self.name.is_none()
            || self.state.is_none()
            || self.time_zone.is_none()
            || !self.station_num.is_valid()
    }

    /// Get description of the site without all the meta-data details.
    pub fn description(&self) -> String {
        let mut desc = String::new();

        if let Some(ref nm) = self.name {
            desc += nm;
            if let Some(st) = self.state {
                desc += ", ";
                desc += st.as_static_str();
            }

            desc += " ";
        }

        desc += &format!("({})", self.station_num);

        desc
    }
}

impl Display for SiteInfo {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        writeln!(
            formatter,
            "Site: station_num - {:6} | name - {:20} | state - {:2} | notes - {}",
            self.station_num,
            self.name.as_deref().unwrap_or("None"),
            self.state.map(|s| s.as_static_str()).unwrap_or("None"),
            self.notes.as_deref().unwrap_or("None"),
        )
    }
}

impl Default for SiteInfo {
    fn default() -> Self {
        SiteInfo {
            station_num: StationNumber::from(0),
            name: None,
            notes: None,
            state: None,
            time_zone: None,
            auto_download: false,
        }
    }
}

/*--------------------------------------------------------------------------------------------------
                                          Unit Tests
--------------------------------------------------------------------------------------------------*/
#[cfg(test)]
mod unit {
    use super::*;

    #[test]
    fn test_site_incomplete() {
        let complete_site = SiteInfo {
            station_num: StationNumber::from(1),
            name: Some("tv station".to_owned()),
            state: Some(StateProv::VI),
            notes: Some("".to_owned()),
            time_zone: Some(FixedOffset::west(7 * 3600)),
            auto_download: false,
        };

        let incomplete_site = SiteInfo {
            station_num: StationNumber::from(1),
            name: Some("tv station".to_owned()),
            state: None,
            notes: None,
            time_zone: None,
            auto_download: false,
        };

        assert!(!complete_site.incomplete());
        assert!(incomplete_site.incomplete());
    }
}
