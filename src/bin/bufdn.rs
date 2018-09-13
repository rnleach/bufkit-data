//! Bufkit Downloader.
//!
//! Downloads Bufkit files and stores them in your archive.

extern crate bufkit_data;
extern crate chrono;
extern crate crossbeam_channel;
#[macro_use]
extern crate itertools;
extern crate failure;
extern crate reqwest;

use bufkit_data::{Archive, CommonCmdLineArgs, Model};
use chrono::{Datelike, Duration, NaiveDateTime, Timelike, Utc};
use crossbeam_channel as channel;
use failure::{Error, Fail};
use reqwest::{Client, StatusCode};
use std::io::{Read, Write};
use std::thread::{spawn, JoinHandle};

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
    Success,                            // File added to the archive
}

fn run() -> Result<(), Error> {
    let app = CommonCmdLineArgs::new_app("bufdn", "Download data into your archive.");

    let (common_args, _matches) = CommonCmdLineArgs::matches(app, false)?;
    let root_clone = common_args.root().to_path_buf();

    let arch = Archive::connect(common_args.root())?;

    let main_tx: channel::Sender<(String, Model, NaiveDateTime, String)>;
    let dl_rx: channel::Receiver<(String, Model, NaiveDateTime, String)>;
    let tx_rx = channel::bounded(10);
    main_tx = tx_rx.0;
    dl_rx = tx_rx.1;

    let dl_tx: channel::Sender<(String, Model, NaiveDateTime, StepResult)>;
    let save_rx: channel::Receiver<(String, Model, NaiveDateTime, StepResult)>;
    let tx_rx = channel::bounded(10);
    dl_tx = tx_rx.0;
    save_rx = tx_rx.1;

    let save_tx: channel::Sender<(String, Model, NaiveDateTime, StepResult)>;
    let print_rx: channel::Receiver<(String, Model, NaiveDateTime, StepResult)>;
    let tx_rx = channel::bounded(10);
    save_tx = tx_rx.0;
    print_rx = tx_rx.1;

    // The file download thread
    let download_handle: JoinHandle<Result<(), Error>> = spawn(move || {
        let client = Client::new();

        for vals in dl_rx {
            let (site, model, init_time, url) = vals;

            let download_result = match client.get(&url).send() {
                Ok(ref mut response) => match response.status() {
                    StatusCode::Ok => {
                        let mut buffer = String::new();
                        match response.read_to_string(&mut buffer) {
                            Ok(_) => StepResult::BufkitFileAsString(buffer),
                            Err(err) => StepResult::OtherDownloadError(err.into()),
                        }
                    }
                    StatusCode::NotFound => StepResult::URLNotFound(url),
                    code @ _ => StepResult::OtherURLStatus(url, code),
                },
                Err(err) => StepResult::OtherDownloadError(err.into()),
            };

            dl_tx.send((site, model, init_time, download_result));
        }

        Ok(())
    });

    // The db writer thread
    let writer_handle: JoinHandle<Result<(), Error>> = spawn(move || {
        let arch = Archive::connect(root_clone)?;

        for (site, model, init_time, download_res) in save_rx {
            let save_res = match download_res {
                StepResult::BufkitFileAsString(data) => {
                    match arch.add_file(&site, model, &init_time, &data) {
                        Ok(_) => StepResult::Success,
                        Err(err) => StepResult::ArhciveError(err.into()),
                    }
                }
                _ => download_res,
            };

            save_tx.send((site, model, init_time, save_res));
        }

        Ok(())
    });

    // The printer thread
    let printer_handle: JoinHandle<Result<(), Error>> = spawn(move || {
        let stdout = ::std::io::stdout();
        let mut lock = stdout.lock();

        for (site, model, init_time, save_res) in print_rx {
            use StepResult::*;

            match save_res {
                URLNotFound(url) => writeln!(lock, "URL does not exist: {}", url)?,
                OtherURLStatus(url, code) => writeln!(lock, "  HTTP error ({}): {}.", code, url)?,
                OtherDownloadError(err) | ArhciveError(err) => {
                    writeln!(lock, "  {}", err)?;

                    let mut fail: &Fail = err.as_fail();

                    while let Some(cause) = fail.cause() {
                        writeln!(lock, "  caused by: {}", cause)?;
                        fail = cause;
                    }
                }
                Success => writeln!(lock, "Success for {:>4} {:^6} {}.", site, model, init_time)?,
                _ => {}
            }
        }

        Ok(())
    });

    // Start processing
    build_download_list(&common_args)
        // Filter out known bad combinations
        .filter(|(site, model, _)| !invalid_combination(site, *model))
        // Filter out data already in the databse
        .filter(|(site, model, init_time)| !arch.exists(site, *model, init_time).unwrap_or(false))
        // Add the url
        .map(|(site, model, init_time)| {
            let url = build_url(&site, model, &init_time);
            (site, model, init_time, url)
        })
        .for_each(move |list_val: (String, Model, NaiveDateTime, String)|{
            main_tx.send(list_val);
        });

    download_handle.join().unwrap()?;
    writer_handle.join().unwrap()?;
    printer_handle.join().unwrap()?;

    Ok(())
}

fn build_download_list<'a>(
    common_args: &'a CommonCmdLineArgs,
) -> impl Iterator<Item = (String, Model, NaiveDateTime)> + 'a {
    let sites = common_args.sites().iter();
    let models = common_args.models().iter();

    let end = Utc::now().naive_utc() - Duration::hours(2);
    let start = Utc::now().naive_utc() - Duration::days(common_args.days_back());

    iproduct!(sites, models).flat_map(move |(site, model)| {
        model
            .all_runs(&(start - Duration::hours(model.hours_between_runs())), &end)
            .map(move |init_time| (site.to_lowercase(), *model, init_time))
    })
}

fn invalid_combination(site: &str, model: Model) -> bool {
    match site {
        "lrr" | "c17" | "s06" => model == Model::NAM || model == Model::NAM4KM,
        "mrp" | "hmm" | "bon" => model == Model::GFS,
        "kfca" => model == Model::NAM || model == Model::NAM4KM,
        _ => false, // All other combinations are OK
    }
}

fn build_url(site: &str, model: Model, init_time: &NaiveDateTime) -> String {
    let site = site.to_lowercase();

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

    let remote_site = translate_sites(&site, model);

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

fn translate_sites(site: &str, model: Model) -> &str {
    match (site, model) {
        ("kgpi", Model::GFS) => "kfca",
        _ => site,
    }
}
