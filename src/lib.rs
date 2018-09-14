#![deny(missing_docs)]
//! Package to manage and interface with an archive of bufkit files.

//
// Public API
//
pub use archive::Archive;
pub use cmd_line::CommonCmdLineArgs;
pub use errors::BufkitDataErr;
pub use inventory::Inventory;
pub use models::Model;
pub use site::{Site, StateProv};

//
// Implementation only
//
extern crate chrono;
#[macro_use]
extern crate clap;
extern crate dirs;
#[macro_use]
extern crate failure;
extern crate flate2;
extern crate rusqlite;
extern crate sounding_analysis;
extern crate sounding_base;
extern crate sounding_bufkit;
extern crate strum;
#[macro_use]
extern crate strum_macros;

mod archive;
mod cmd_line;
mod errors;
mod inventory;
mod models;
mod site;

#[cfg(test)]
extern crate tempdir;
