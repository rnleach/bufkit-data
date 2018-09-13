//! Bufkit Downloader.
//!
//! Downloads Bufkit files and stores them in your archive.

extern crate bufkit_data;
extern crate chrono;
#[macro_use]
extern crate itertools;
extern crate failure;
extern crate reqwest;

use bufkit_data::{Archive, CommonCmdLineArgs, Model};
use chrono::{Datelike, Duration, NaiveDateTime, Timelike, Utc};
use failure::{Error, Fail};
use reqwest::{Client, StatusCode};
use std::io::Read;

static HOST_URL: &str = "http://mtarchive.geol.iastate.edu/";

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

// Result from a single step in the processing chain
enum StepResult {
    BufkitFileAsString(String),         // Data
    URLNotFound(String),                // URL
    OtherURLStatus(String, StatusCode), // URL, status code
    OtherDownloadError(Error),          // Any other error downloading
    ArhciveError(Error),                // Error adding it to the archive
}

fn run() -> Result<(), Error> {
    let app = CommonCmdLineArgs::new_app("bufdn", "Download data into your archive.");

    let (common_args, _matches) = CommonCmdLineArgs::matches(app, false)?;

    let arch = Archive::connect(common_args.root())?;
    let client = Client::new();

    // Start processing
    build_download_list(&common_args)
        // Filter out known bad combinations
        .filter(|(site, model, _)| !invalid_combination(site, *model))
        // Filter out data already in the databse
        .filter(|(site, model, init_time)| !arch.exists(site, *model, init_time).unwrap_or(false))
        // Add the url
        .map(|(site, model, init_time)| (site, model, init_time, build_url(site, model, &init_time)))
        // Attempt the download
        .map(|(site, model, init_time, url)|{
            let download_result = match client.get(&url).send(){
                Ok(ref mut response) => {
                    match response.status(){
                        StatusCode::Ok => {
                            let mut buffer = String::new();
                            match response.read_to_string(&mut buffer){
                                Ok(_) =>StepResult::BufkitFileAsString(buffer),
                                Err(err) => StepResult::OtherDownloadError(err.into()),
                            }
                        },
                        StatusCode::NotFound => StepResult::URLNotFound(url),
                        code@_ => StepResult::OtherURLStatus(url, code),
                    }
                },
                Err(err) => StepResult::OtherDownloadError(err.into()),
            };

            (site, model, init_time, download_result)
        })
        // Add the successful downloads to the archive
        .filter_map(|(site, model, init_time, res)|{
            match res {
                StepResult::BufkitFileAsString(data) => {
                    match arch.add_file(site, model, &init_time, &data) {
                        Ok(_) => None,
                        Err(err) => Some((site, model, init_time, StepResult::ArhciveError(err.into()))),
                    }
                },
                _ => Some((site, model, init_time, res))
            }
        })
        // Print out the error messages
        .for_each(|(site, model, init_time, res)|{
            use StepResult::*;

            println!("Error with {} {} for {}.", init_time, model, site);
            match res {
                URLNotFound(url) => println!("  URL: {} does not exist.", url),
                OtherURLStatus(url, code) => println!("  HTTP error ({}): {}.", code, url),
                OtherDownloadError(err)  | ArhciveError(err) => {
                    println!("  {}", err);

                    let mut fail: &Fail = err.as_fail();

                    while let Some(cause) = fail.cause() {
                        println!("  caused by: {}", cause);
                        fail = cause;
                    }
                },
                _ => {},
            }
        });

    Ok(())
}

fn build_download_list<'a>(
    common_args: &'a CommonCmdLineArgs,
) -> impl Iterator<Item = (&'a String, Model, NaiveDateTime)> {
    let sites = common_args.sites().iter();
    let models = common_args.models().iter();

    let end = Utc::now().naive_utc();
    let start = Utc::now().naive_utc() - Duration::days(common_args.days_back());

    iproduct!(sites, models).flat_map(move |(site, model)| {
        model
            .all_runs(&(start - Duration::hours(model.hours_between_runs())), &end)
            .map(move |init_time| (site, *model, init_time))
    })
}

fn invalid_combination(site: &str, model: Model) -> bool {
    match site {
        "lrr" | "c17" => model == Model::NAM || model == Model::NAM4KM,
        "mrp" | "hmm" | "bon" => model == Model::GFS,
        _ => false, // All other combinations are OK
    }
}

fn translate_sites(site: &str, model: Model) -> &str {
    match (site, model) {
        ("kgpi", Model::GFS) => "kfca",
        _ => site,
    }
}

fn build_url(site: &str, model: Model, init_time: &NaiveDateTime) -> String {
    let year = init_time.year();
    let month = init_time.month();
    let day = init_time.day();
    let hour = init_time.hour();
    let remote_model = match (model, hour) {
        (Model::GFS, _) => "gfs3",
        (Model::NAM, 6) | (Model::NAM, 18) => "namm",
        (Model::NAM, _) => "nam",
        (Model::NAM4KM, _) => "nam4km",
    };

    let remote_site = translate_sites(site, model);

    let remote_file_name = remote_model.to_string() + "_" + remote_site + ".buf";

    format!(
        "{}{}/{:02}/{:02}/bufkit/{:02}/{}/{}",
        HOST_URL,
        year,
        month,
        day,
        hour,
        model.to_string().to_lowercase(),
        remote_file_name
    )
}
