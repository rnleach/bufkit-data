//! Module for errors.

use sounding_analysis::AnalysisError;
use sounding_bufkit::BufkitFileError;

/// Error from the archive interface.
#[derive(Debug, Fail)]
pub enum BufkitDataErr {
    //
    // Inherited errors from sounding stack
    //
    /// Error forwarded from sounding-analysis
    #[fail(display = "Error from sounding-analysis: {}", _0)]
    SoundingAnalysis(#[cause] AnalysisError),
    /// Error forwarded from sounding-bufkit
    #[fail(display = "Error from sounding-bufkit: {}", _0)]
    SoundingBufkit(#[cause] BufkitFileError),

    //
    // Inherited errors from std
    //
    /// Error forwarded from std
    #[fail(display = "std io error {}", _0)]
    IO(#[cause] ::std::io::Error),

    //
    // My own errors from this crate
    //
    /// Invalid model name
    #[fail(display = "Invalid model name: {}.", _0)]
    InvalidModelName(String),
    /// Database error
    #[fail(display = "Error with sqlite database: {}.", _0)]
    Database(#[cause] ::rusqlite::Error),
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
