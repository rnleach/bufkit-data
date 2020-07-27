//! Module for errors.
use sounding_analysis::AnalysisError;
use sounding_bufkit::BufkitFileError;
use std::{error::Error, fmt::Display};

/// Error from the archive interface.
#[derive(Debug)]
pub enum BufkitDataErr {
    // Inherited errors from sounding stack
    /// Error forwarded from sounding-analysis
    SoundingAnalysis(AnalysisError),
    /// Error forwarded from sounding-bufkit
    SoundingBufkit(BufkitFileError),

    // Inherited errors from std
    /// Error forwarded from std
    IO(::std::io::Error),

    // Other forwarded errors
    /// Database error
    Database(::rusqlite::Error),
    /// Error forwarded from the strum crate
    StrumError(strum::ParseError),
    /// General error with any cause information erased and replaced by a string
    GeneralError(String),

    // My own errors from this crate
    /// File not found in the index.
    NotInIndex,
    /// The database structure is wrong.
    InvalidSchema,
    /// Invalid model name
    InvalidModelName(String),
    /// Site ID does not exist.
    InvalidSiteId(String),
    /// Not enough data to complete the task.
    NotEnoughData,
    /// Sounding was missing a valid time
    MissingValidTime,
    /// Missing station information.
    MissingStationData,
    /// There was an internal logic error.
    LogicError(&'static str),
}

impl Display for BufkitDataErr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        use crate::errors::BufkitDataErr::*;

        match self {
            SoundingAnalysis(err) => write!(f, "error from sounding-analysis: {}", err),
            SoundingBufkit(err) => write!(f, "error from sounding-bufkit: {}", err),

            IO(err) => write!(f, "std lib io error: {}", err),

            Database(err) => write!(f, "database error: {}", err),
            StrumError(err) => write!(f, "error forwarded from strum crate: {}", err),
            GeneralError(msg) => write!(f, "general error forwarded: {}", msg),

            NotInIndex => write!(f, "no match in the index"),
            InvalidSchema => write!(f, "invalid index format"),
            InvalidModelName(mdl_nm) => write!(f, "invalid model name: {}", mdl_nm),
            InvalidSiteId(site_id) => write!(f, "invalid site id: {}", site_id),
            NotEnoughData => write!(f, "not enough data to complete task"),
            MissingValidTime => write!(f, "sounding missing a valid time"),
            MissingStationData => write!(f, "not enough information about the station"),
            LogicError(msg) => write!(f, "internal logic error: {}", msg),
        }
    }
}

impl Error for BufkitDataErr {}

impl From<BufkitFileError> for BufkitDataErr {
    fn from(err: BufkitFileError) -> BufkitDataErr {
        BufkitDataErr::SoundingBufkit(err)
    }
}

impl From<AnalysisError> for BufkitDataErr {
    fn from(err: AnalysisError) -> BufkitDataErr {
        BufkitDataErr::SoundingAnalysis(err)
    }
}

impl From<::std::io::Error> for BufkitDataErr {
    fn from(err: ::std::io::Error) -> BufkitDataErr {
        BufkitDataErr::IO(err)
    }
}

impl From<::rusqlite::Error> for BufkitDataErr {
    fn from(err: ::rusqlite::Error) -> BufkitDataErr {
        BufkitDataErr::Database(err)
    }
}

impl From<strum::ParseError> for BufkitDataErr {
    fn from(err: strum::ParseError) -> BufkitDataErr {
        BufkitDataErr::StrumError(err)
    }
}

impl From<Box<dyn Error>> for BufkitDataErr {
    fn from(err: Box<dyn Error>) -> BufkitDataErr {
        BufkitDataErr::GeneralError(err.to_string())
    }
}
