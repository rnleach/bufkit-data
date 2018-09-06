#![deny(missing_docs)]
//! Package to manage and interface with an archive of bufkit files.

//
// Public API
//
pub use archive::{default_root, Archive, Inventory};
pub use errors::BufkitDataErr;
pub use models::Model;
pub use site::Site;

//
// Implementation only
//
extern crate chrono;
#[macro_use]
extern crate failure;
extern crate flate2;
extern crate rusqlite;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_yaml;
extern crate sounding_analysis;
extern crate sounding_base;
extern crate sounding_bufkit;

mod archive;
mod errors;
mod models;
mod site;

#[cfg(test)]
extern crate tempdir;
