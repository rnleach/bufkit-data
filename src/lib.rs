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
//! ## Python integration
//! When compiled with the `pylib` feature it minimally supports access from Python. At this time it
//! only supports reading files from the archive.
//! 
//! For use with python, I recommend using a virtualenv and
//! [maturin](https://github.com/pyo3/maturin). Once the virtualenv is activated, 
//! `pip install maturin` and install the bufkit_data package by going into the directory 
//! bufkit-data is cloned into and running:
//! 
//! ``` shell
//! maturin develop --release --strip --cargo-extra-args="--features pylib"
//! ```
//! 
//! After this installation, you should be able to use `bufkit_data` from python with:
//! ``` python
//! import bufkit_data as bd
//! 
//! arch = bd.Archive("Path/to/my_archive")
//! most_recent_ord_nam = arch.most_recent("kord", "nam4km")
//! 
//! from datetime import datetime as dt
//! valid_time = dt(2020, 5, 5, 12, 0)
//! 
//! old_ord_gfs = arch.retrieve_sounding("kord", "gfs", valid_time)
//! ```
#![deny(missing_docs)]

//
// Public API
//
pub use crate::archive::{AddFileResult, Archive, DownloadInfo, StationSummary};
pub use crate::errors::BufkitDataErr;
pub use crate::models::Model;
pub use crate::site::{SiteInfo, StateProv, StationNumber};

#[cfg(feature = "pylib")]
mod py_lib;

//
// Implementation only
//
mod archive;
mod coords;
mod errors;
mod models;
mod site;
