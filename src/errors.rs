//! Module for errors.
use failure;
use sounding_analysis::AnalysisError;
use sounding_bufkit::BufkitFileError;

use strum::ParseError;

/// Error from the archive interface.
#[derive(Debug, Fail)]
pub enum BufkitDataErr {
    //
    // Inherited errors from sounding stack
    //
    /// Error forwarded from sounding-analysis
    #[fail(display = "Error from sounding-analysis.")]
    SoundingAnalysis(#[cause] AnalysisError),
    /// Error forwarded from sounding-bufkit
    #[fail(display = "Error from sounding-bufkit.")]
    SoundingBufkit(#[cause] BufkitFileError),

    //
    // Inherited errors from std
    //
    /// Error forwarded from std
    #[fail(display = "std io error.")]
    IO(#[cause] ::std::io::Error),
    /// Error sending message...
    #[fail(display = "other error from std lib")]
    SenderError(#[cause] ::std::sync::mpsc::SendError<String>),

    //
    // Other forwarded errors
    //
    /// Database error
    #[fail(display = "Error with sqlite database.")]
    Database(#[cause] ::rusqlite::Error),
    /// A general error forwarded with the failure crate
    #[fail(display = "General error forwarded.")]
    GeneralError(Box<failure::Error>),
    /// Error forwarded from the strum crate
    #[fail(display = "Error from strum crate")]
    StrumError(Box<failure::Error>),

    //
    // My own errors from this crate
    //
    /// Invalid model name
    #[fail(display = "Invalid model name: {}.", _0)]
    InvalidModelName(String),
    /// Not enough data to complete the task.
    #[fail(display = "Not enough data")]
    NotEnoughData,
}

impl From<failure::Error> for BufkitDataErr {
    fn from(err: failure::Error) -> BufkitDataErr {
        BufkitDataErr::GeneralError(Box::new(err))
    }
}

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

impl From<ParseError> for BufkitDataErr {
    fn from(err: ParseError) -> BufkitDataErr {
        BufkitDataErr::StrumError(Box::new(failure::Error::from(err)))
    }
}