//! Models potentially stored in the archive.

use chrono::{Duration, NaiveDateTime};
use std::fmt;

/// Models potentially stored in the archive.
#[derive(Clone, Copy, PartialEq, Eq, Debug, EnumString, AsStaticStr, EnumIter, Hash)]
pub enum Model {
    /// The U.S. Global Forecast System
    #[strum(
        to_string = "gfs",
        serialize = "gfs3",
        serialize = "GFS",
        serialize = "GFS3"
    )]
    GFS,
    /// The U.S. North American Model
    #[strum(
        to_string = "nam",
        serialize = "namm",
        serialize = "NAM",
        serialize = "NAMM"
    )]
    NAM,
    /// The high resolution nest of the `NAM`
    #[strum(to_string = "nam4km", serialize = "NAM4KM")]
    NAM4KM,
    /// This could be any special local model, but let it be WRF.
    #[strum(to_string = "local_wrf", serialize = "LOCAL_WRF")]
    LocalWrf,
    /// This is any other local model not accounted for so far.
    #[strum(to_string = "other_model", serialize = "OTHER")]
    Other,
}

impl fmt::Display for Model {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Model::*;

        match *self {
            GFS => write!(f, "{}", stringify!(GFS)),
            NAM => write!(f, "{}", stringify!(NAM)),
            NAM4KM => write!(f, "{}", stringify!(NAM4KM)),
            LocalWrf => write!(f, "{}", stringify!(LocalWRf)),
            Other => write!(f, "{}", stringify!(Other)),
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
            Model::LocalWrf => 24, // Probably won't be able to download anyway, can't build URL yet.
            Model::Other => 24, // Probably won't be able to download anyway, can't build URL yet.
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

        let delta_t = self.hours_between_runs();

        //
        // Find a good start time.
        //
        let mut round_start = start.date().and_hms(0, 0, 0) + Duration::hours(self.base_hour());
        // Make sure we didn't jump ahead into the future.
        while round_start > *start {
            round_start -= Duration::hours(self.hours_between_runs());
        }
        // Make sure we didn't jumb too far back.
        while round_start < *start {
            round_start += Duration::hours(self.hours_between_runs());
        }

        // Ultimately make sure we start before we end.
        while round_start > *end {
            round_start -= Duration::hours(self.hours_between_runs());
        }

        let steps: i64 = (*end - round_start).num_hours() / self.hours_between_runs();

        (0..=steps).map(move |step| round_start + Duration::hours(step * delta_t))
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
        assert_eq!(
            Model::GFS.hours_between_runs(),
            6,
            "test pre-condition failed."
        );

        let start = &NaiveDate::from_ymd(2018, 9, 1).and_hms(0, 0, 0);
        let end = &NaiveDate::from_ymd(2018, 9, 2).and_hms(0, 0, 0);
        assert_eq!(Model::GFS.all_runs(start, end).count(), 5);

        let start = &NaiveDate::from_ymd(2018, 9, 1).and_hms(0, 1, 0);
        let end = &NaiveDate::from_ymd(2018, 9, 2).and_hms(0, 0, 0);
        assert_eq!(Model::GFS.all_runs(start, end).count(), 4);
    }
}
