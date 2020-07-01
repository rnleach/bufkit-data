//! Crate to manage and interface with an archive of
//! [bufkit](https://training.weather.gov/wdtd/tools/BUFKIT/index.php) files.
//!
//! This supports a set of command line tools for utilizing the archive. In general, it may be
//! useful to anyone interested in archiving bufkit files.
//!
//! The current implementation uses an [sqlite](https://www.sqlite.org/index.html) database to keep
//! track of files stored in a common directory. The files are compressed, and so should only be
//! accessed via the API provided by this crate.
//!
#![deny(missing_docs)]

//
// Public API
//
pub use crate::archive::{AddFileResult, Archive};
pub use crate::errors::BufkitDataErr;
pub use crate::inventory::Inventory;
pub use crate::models::Model;
pub use crate::site::{Site, StateProv};

//
// Implementation only
//
mod archive;
mod coords;
mod errors;
mod inventory;
mod models;
mod site;
