use crate::errors::BufkitDataErr;
use crate::models::Model;
use crate::site::Site;
use chrono::{Duration, NaiveDateTime};

/// Inventory lists first & last initialization times of the models in the archive for a site &
/// model. It also contains a list of model initialization times that are missing between the first
/// and last.
#[derive(Debug, PartialEq, Eq)]
pub struct Inventory {
    /// The earliest model run in the archive.
    pub first: NaiveDateTime,
    /// The last model run in the archive.
    pub last: NaiveDateTime,
    /// A list of start and end times for missing model runs.
    pub missing: Vec<(NaiveDateTime, NaiveDateTime)>,
    /// Whether this site is flagged for automatic download.
    pub auto_download: bool,
}

impl Inventory {
    /// Create a new inventory. Assume the provided data is sorted from earliest to latest.
    pub(crate) fn new(
        init_times: impl IntoIterator<Item = NaiveDateTime>,
        model: Model,
        site: &Site,
    ) -> Result<Self, BufkitDataErr> {
        let mut init_times = init_times.into_iter();
        let delta_hours = Duration::hours(model.hours_between_runs());

        let first = init_times
            .by_ref()
            .next()
            .ok_or(BufkitDataErr::NotEnoughData)?;
        let mut missing = vec![];

        let mut next_init_time = first;

        for init_time in init_times {
            next_init_time += delta_hours;

            if next_init_time < init_time {
                let start = next_init_time;
                let last = init_time - delta_hours;
                missing.push((start, last));
            }

            while next_init_time < init_time {
                next_init_time += delta_hours;
            }
        }

        let last = next_init_time;
        let auto_download = site.auto_download;

        Ok(Inventory {
            first,
            last,
            missing,
            auto_download,
        })
    }
}
