//! Module for errors.
use std::{error::Error, fmt::Display};

/// Error from the archive interface.
#[derive(Debug)]
pub enum BufkitDataErr {
    // Inherited errors from sounding stack
    /// Error forwarded from sounding-analysis
    SoundingAnalysis(sounding_analysis::AnalysisError),
    /// Error forwarded from sounding-bufkit
    SoundingBufkit(sounding_bufkit::BufkitFileError),

    // Inherited errors from std
    /// Error forwarded from std
    IO(std::io::Error),

    // Other forwarded errors
    /// Database error
    Database(rusqlite::Error),
    /// Error forwarded from the strum crate
    StrumError(strum::ParseError),
    /// General error with any cause information erased and replaced by a string
    GeneralError(String),

    // My own errors from this crate
    /// File not found in the index.
    NotInIndex,
    /// Not enough data to complete the task.
    NotEnoughData,
    /// Sounding was missing a valid time
    MissingValidTime,
    /// Missing station information.
    MissingStationData,
    /// An error that is known and hard coded into the library.
    KnownArchiveError(&'static str),
    /// There was an internal logic error.
    LogicError(&'static str),
    /// The site id didn't match the hint when adding.
    MismatchedIDs {
        /// The ID that was provided as a hint.
        hint: String,
        /// The ID that was parsed from the file.
        parsed: String,
    },
    /// The station numbers didn't match.
    MismatchedStationNumbers {
        /// The StationNumber number with the original request.
        hint: crate::StationNumber,
        /// The StationNumber parsed from the file.
        parsed: crate::StationNumber,
    },
    /// Parsed and expected initialization times didn't match.
    MismatchedInitializationTimes {
        /// The initialization time that was expected.
        hint: chrono::NaiveDateTime,
        /// The inizialization time that was parsed from the file.
        parsed: chrono::NaiveDateTime,
    },
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
            NotEnoughData => write!(f, "not enough data to complete task"),
            MissingValidTime => write!(f, "sounding missing a valid time"),
            MissingStationData => write!(f, "not enough information about the station"),
            KnownArchiveError(msg) => write!(f, "Known error: {}", msg),
            LogicError(msg) => write!(f, "internal logic error: {}", msg),
            MismatchedIDs { hint, parsed } => {
                write!(f, "mismatched ids parsed: {} != hint:{}", parsed, hint)
            }
            MismatchedStationNumbers { .. } => write!(f, "mismatched station numbers"),
            MismatchedInitializationTimes { .. } => write!(f, "mismatched initialization times"),
        }
    }
}

impl Error for BufkitDataErr {}

impl From<sounding_bufkit::BufkitFileError> for BufkitDataErr {
    fn from(err: sounding_bufkit::BufkitFileError) -> BufkitDataErr {
        BufkitDataErr::SoundingBufkit(err)
    }
}

impl From<sounding_analysis::AnalysisError> for BufkitDataErr {
    fn from(err: sounding_analysis::AnalysisError) -> BufkitDataErr {
        BufkitDataErr::SoundingAnalysis(err)
    }
}

impl From<std::io::Error> for BufkitDataErr {
    fn from(err: std::io::Error) -> BufkitDataErr {
        BufkitDataErr::IO(err)
    }
}

impl From<rusqlite::Error> for BufkitDataErr {
    fn from(err: rusqlite::Error) -> BufkitDataErr {
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
