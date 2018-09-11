//! Models potentially stored in the archive.

use std::fmt;

use chrono::{Duration, NaiveDateTime};

/// Models potentially stored in the archive.
#[allow(missing_docs)]
#[derive(Clone, Copy, PartialEq, Eq, Debug, EnumString, AsStaticStr, EnumIter)]
pub enum Model {
    #[strum(serialize = "gfs", serialize = "gfs3", serialize = "GFS", serialize = "GFS3")]
    GFS,
    #[strum(serialize = "nam", serialize = "namm", serialize = "NAM", serialize = "NAMM")]
    NAM,
    #[strum(serialize = "nam4km", serialize = "NAM4KM")]
    NAM4KM,
}

impl fmt::Display for Model {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Model::*;

        match self {
            GFS => write!(f, "{}", stringify!(GFS)),
            NAM => write!(f, "{}", stringify!(NAM)),
            NAM4KM => write!(f, "{}", stringify!(NAM4KM)),
        }
    }
}

impl Model {
    /// Get the number of hours between runs.
    pub fn hours_between_runs(self) -> i64 {
        match self {
            Model::GFS => 6,
            Model::NAM => 6,
            Model::NAM4KM => 6,
        }
    }

    /// Get the base hour of a model run.
    ///
    /// Most model run times are 0Z, 6Z, 12Z, 18Z. The base hour along with hours between runs
    /// allows you to reconstruct these times. Note that SREF starts at 03Z and runs every 6 hours,
    /// so it is different.
    pub fn base_hour(self) -> i64 {
        match self {
            _ => 0,
        }
    }

    /// Create an iterator of all the model runs between two times
    pub fn all_runs(
        self,
        start: &NaiveDateTime,
        end: &NaiveDateTime,
    ) -> impl Iterator<Item = NaiveDateTime> {
        debug_assert!(start <= end);

        let steps: i64 = (*end - *start).num_hours() / self.hours_between_runs();
        let delta_t = self.hours_between_runs();
        let start = start.clone();

        (0..=steps).map(move |step| start + Duration::hours(step * delta_t))
    }
}

/*--------------------------------------------------------------------------------------------------
                                          Unit Tests
--------------------------------------------------------------------------------------------------*/
#[cfg(test)]
mod unit {
    use super::*;

    use chrono::NaiveDate;

    #[test]
    fn test_all_runs() {
        let start = &NaiveDate::from_ymd(2018, 9, 1).and_hms(0, 0, 0);
        let end = &NaiveDate::from_ymd(2018, 9, 2).and_hms(0, 0, 0);

        assert_eq!(
            Model::GFS.hours_between_runs(),
            6,
            "test pre-condition failed."
        );
        assert_eq!(Model::GFS.all_runs(start, end).count(), 5);
    }
}
