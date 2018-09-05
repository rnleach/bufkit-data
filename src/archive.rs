//! An archive of bufkit soundings.

use chrono::NaiveDateTime;
use rusqlite::Connection;
use std::path::PathBuf;

use errors::BufkitDataErr;
use models::Model;

/// Description of a site with a sounding
#[allow(missing_docs)]
pub struct Site {
    pub lat: f64,
    pub lon: f64,
    pub elev_m: f64,
    pub id: String,
    pub name: String,
    pub notes: String,
}

/// Inventory lists first & last initialization times of the models in the database for a site &
/// model. It also contains a list of model initialization times that are missing between the first
/// and last.
#[allow(missing_docs)]
pub struct Inventory {
    pub first: NaiveDateTime,
    pub last: NaiveDateTime,
    pub missing: Vec<NaiveDateTime>,
}

/// The archive.
pub struct Archive {
    root: PathBuf,
    db: Connection,
}

impl Archive {
    /// Initialize a new archive.
    pub fn create_new(root: PathBuf) -> Self {
        unimplemented!()
    }

    /// Open an existing archive.
    pub fn connect(root: PathBuf) -> Self {
        unimplemented!()
    }

    /// Retrieve a list of sites in the archive.
    pub fn get_sites(&self, only_with_missing: bool) -> Vec<Site> {
        unimplemented!()
    }

    /// Add a site to the list of sites.
    pub fn add_site(&mut self, site: &Site) -> Result<(), BufkitDataErr> {
        unimplemented!()
    }

    /// Add a bufkit file to the archive.
    pub fn add_file(
        &mut self,
        site_id: &str,
        model: Model,
        init_time: &NaiveDateTime,
    ) -> Result<(), BufkitDataErr> {
        unimplemented!()
    }

    /// Load a file from the archive and return its contents in a `String`.
    pub fn get_file(
        &self,
        site: &str,
        model: Model,
        init_time: &NaiveDateTime,
    ) -> Result<String, BufkitDataErr> {
        unimplemented!()
    }

    /// Check to see if a file is present in the archive.
    pub fn exists(
        &self,
        site: &str,
        model: Model,
        init_time: &NaiveDateTime,
    ) -> Result<bool, BufkitDataErr> {
        unimplemented!()
    }

    /// Get an inventory of soundings for a site & model.
    pub fn get_inventory(&self, site_id: &str, model: Model) -> Result<Inventory, BufkitDataErr> {
        unimplemented!()
    }

    //
    // TODO
    //

    // Add climate summary file and climate data cache files.
}
