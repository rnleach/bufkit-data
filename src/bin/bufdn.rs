//! Bufkit Downloader.
//!
//! Downloads Bufkit files and stores them in your archive.

extern crate failure;

extern crate bufkit_data;

use bufkit_data::CommonCmdLineArgs;
use failure::{Error, Fail};

fn main() {
    if let Err(ref e) = run() {
        println!("error: {}", e);

        let mut fail: &Fail = e.as_fail();

        while let Some(cause) = fail.cause() {
            println!("caused by: {}", cause);

            if let Some(backtrace) = cause.backtrace() {
                println!("backtrace: {}\n\n\n", backtrace);
            }

            fail = cause;
        }

        ::std::process::exit(1);
    }
}

fn run() -> Result<(), Error> {
    let app = CommonCmdLineArgs::new_app("bufdn", "Download data into your archive.");

    let (common_args, _matches) = CommonCmdLineArgs::matches(app, false)?;

    // open the archive
    // build a list of soundings to download (site_id, model, init_time)
    // filter it by what already exists in the archive
    // filter it by known missing data from the source
    // download the data
    // add it to the archive

    println!("common_args: {:#?}", common_args);

    Ok(())
}
