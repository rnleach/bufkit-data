use crate::{models::Model, site::StationNumber};

/// Type to encapsulate information needed to download data.
pub struct DownloadInfo {
    /// The station id
    pub id: String,
    /// The station number.
    pub station_num: StationNumber,
    /// The model to download.
    pub model: Model,
}
