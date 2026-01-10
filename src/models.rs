//! Models potentially stored in the archive.

#[cfg(feature = "pylib")]
use pyo3::prelude::*;

use std::fmt;
use strum_macros::{EnumIter, EnumString, IntoStaticStr};

/// Models potentially stored in the archive.
#[derive(Clone, Copy, PartialEq, Eq, Debug, EnumString, IntoStaticStr, EnumIter, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "pylib", pyclass(module = "bufkit_data"))]
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
}

impl fmt::Display for Model {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use crate::models::Model::*;

        match *self {
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
            Model::GFS | Model::NAM | Model::NAM4KM => 6,
        }
    }

    /// Get the base hour of a model run.
    ///
    /// Most model run times are 0Z, 6Z, 12Z, 18Z. The base hour along with hours between runs
    /// allows you to reconstruct these times. Note that SREF starts at 03Z and runs every 6 hours,
    /// so it is different.
    pub fn base_hour(self) -> i64 {
        match self {
            Model::GFS | Model::NAM | Model::NAM4KM => 0,
        }
    }

    /// Create an iterator of all the model runs between two times
    pub fn all_runs(
        self,
        start: &chrono::NaiveDateTime,
        end: &chrono::NaiveDateTime,
    ) -> impl Iterator<Item = chrono::NaiveDateTime> + use<> {
        let delta_t = self.hours_between_runs();

        // Find a good start time that corresponds with an actual model run time.
        let round_start = if *start < *end {
            let mut strt =
                start.date().and_hms_opt(0, 0, 0).unwrap() + chrono::Duration::hours(self.base_hour());
            // Make sure we didn't jump ahead into the future.
            while strt > *start {
                strt -= chrono::Duration::hours(self.hours_between_runs());
            }
            // Make sure we didn't jumb too far back.
            while strt < *start {
                strt += chrono::Duration::hours(self.hours_between_runs());
            }

            strt
        } else {
            let mut strt =
                start.date().and_hms_opt(0, 0, 0).unwrap() + chrono::Duration::hours(self.base_hour());
            while strt < *start {
                strt += chrono::Duration::hours(self.hours_between_runs());
            }
            while strt > *start {
                strt -= chrono::Duration::hours(self.hours_between_runs());
            }

            strt
        };

        let steps: i64 = (*end - round_start).num_hours() / self.hours_between_runs();

        (0..=steps.abs())
            .map(move |step| round_start + chrono::Duration::hours(steps.signum() * step * delta_t))
    }

    /// Get a static str representation
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

    use chrono::NaiveDate;

    #[test]
    fn test_all_runs() {
        assert_eq!(
            Model::GFS.hours_between_runs(),
            6,
            "test pre-condition failed."
        );

        let start = &NaiveDate::from_ymd_opt(2018, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let end = &NaiveDate::from_ymd_opt(2018, 9, 2).unwrap().and_hms_opt(0, 0, 0).unwrap();
        assert_eq!(Model::GFS.all_runs(start, end).count(), 5);
        Model::GFS
            .all_runs(start, end)
            .scan(*start, |prev, rt| {
                eprintln!("{} <= {}", prev, rt);
                assert!(*prev <= rt);
                *prev = rt;
                Some(rt)
            })
            .for_each(|rt| assert!(rt >= *start && rt <= *end));
        eprintln!();

        let start = &NaiveDate::from_ymd_opt(2018, 9, 1).unwrap().and_hms_opt(0, 1, 0).unwrap();
        let end = &NaiveDate::from_ymd_opt(2018, 9, 2).unwrap().and_hms_opt(0, 0, 0).unwrap();
        assert_eq!(Model::GFS.all_runs(start, end).count(), 4);
        Model::GFS
            .all_runs(start, end)
            .scan(*start, |prev, rt| {
                eprintln!("{} <= {}", prev, rt);
                assert!(*prev <= rt);
                *prev = rt;
                Some(rt)
            })
            .for_each(|rt| assert!(rt >= *start && rt <= *end));
        eprintln!();

        let end = &NaiveDate::from_ymd_opt(2018, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let start = &NaiveDate::from_ymd_opt(2018, 9, 2).unwrap().and_hms_opt(0, 0, 0).unwrap();
        assert_eq!(Model::GFS.all_runs(start, end).count(), 5);
        Model::GFS
            .all_runs(start, end)
            .scan(*start, |prev, rt| {
                eprintln!("{} >= {}", prev, rt);
                assert!(*prev >= rt);
                *prev = rt;
                Some(rt)
            })
            .for_each(|rt| assert!(rt >= *end && rt <= *start));
        eprintln!();

        let end = &NaiveDate::from_ymd_opt(2018, 9, 1).unwrap().and_hms_opt(0, 1, 0).unwrap();
        let start = &NaiveDate::from_ymd_opt(2018, 9, 2).unwrap().and_hms_opt(0, 0, 0).unwrap();
        assert_eq!(Model::GFS.all_runs(start, end).count(), 4);
        Model::GFS
            .all_runs(start, end)
            .scan(*start, |prev, rt| {
                eprintln!("{} >= {}", prev, rt);
                assert!(*prev >= rt);
                *prev = rt;
                Some(rt)
            })
            .for_each(|rt| assert!(rt >= *end && rt <= *start));
        eprintln!();

        let end = &NaiveDate::from_ymd_opt(2018, 9, 1).unwrap().and_hms_opt(0, 1, 0).unwrap();
        let start = &NaiveDate::from_ymd_opt(2018, 9, 2).unwrap().and_hms_opt(0, 2, 0).unwrap();
        assert_eq!(Model::GFS.all_runs(start, end).count(), 4);
        Model::GFS
            .all_runs(start, end)
            .scan(*start, |prev, rt| {
                eprintln!("{} >= {}", prev, rt);
                assert!(*prev >= rt);
                *prev = rt;
                Some(rt)
            })
            .for_each(|rt| assert!(rt >= *end && rt <= *start));
    }
}
