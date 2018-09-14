//! BufKit Archive Manager

extern crate clap;
extern crate failure;
extern crate strum;

extern crate bufkit_data;

use bufkit_data::{Archive, CommonCmdLineArgs, Site};
use clap::{Arg, ArgMatches, SubCommand};
use failure::{err_msg, Error, Fail};
use strum::AsStaticRef;

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
    let app = CommonCmdLineArgs::new_app("bkam", "Manage a Bufkit file archive.")
        .subcommand(
            SubCommand::with_name("create")
                .about("Create a new archive. Ignores all options except --root and --force.")
                .arg(
                    Arg::with_name("force")
                        .long("force")
                        .help("Overwrite any existing archive at `root`."),
                ),
        ).subcommand(
            SubCommand::with_name("sites")
                .about("View and manipulate site data.")
                .subcommand(
                    SubCommand::with_name("list")
                        .about("List sites in the data base.")
                        .arg(
                            Arg::with_name("missing-data")
                                .short("m")
                                .long("missing-data")
                                .help("Sites with any missing info."),
                        ).arg(
                            Arg::with_name("missing-state")
                                .long("missing-state")
                                .help("Only sites missing state/providence."),
                        ),
                ).subcommand(SubCommand::with_name("modify").about("Modify the entry for a site."))
                .subcommand(
                    SubCommand::with_name("inv")
                        .about("Get the inventory of soundings for a site."),
                ).subcommand(
                    SubCommand::with_name("export")
                        .about("Export a sounding from the archive in its raw format."),
                ),
        );

    let (common_args, matches) = CommonCmdLineArgs::matches(app, true)?;

    match matches.subcommand() {
        ("create", Some(sub_args)) => create(common_args, sub_args)?,
        ("sites", Some(sub_args)) => sites(common_args, sub_args)?,
        _ => unreachable!(),
    }

    Ok(())
}

fn create(common_args: CommonCmdLineArgs, sub_args: &ArgMatches) -> Result<(), Error> {
    // Check if the archive already exists. (try connecting to it)
    let already_exists: bool = Archive::connect(common_args.root()).is_ok();

    if already_exists && sub_args.is_present("force") {
        ::std::fs::remove_dir_all(common_args.root())?;
    } else if already_exists {
        return Err(err_msg(
            "Archive already exists, must use --force to overwrite.",
        ));
    }

    Archive::create_new(common_args.root())?;

    Ok(())
}

fn sites(common_args: CommonCmdLineArgs, sub_args: &ArgMatches) -> Result<(), Error> {
    match sub_args.subcommand() {
        ("list", Some(sub_sub_args)) => sites_list(common_args, sub_args, &sub_sub_args),
        ("modify", Some(sub_sub_args)) => unimplemented!(),
        ("inv", Some(sub_sub_args)) => unimplemented!(),
        ("export", Some(sub_sub_args)) => unimplemented!(),
        _ => unreachable!(),
    }
}

fn sites_list(
    common_args: CommonCmdLineArgs,
    _sub_args: &ArgMatches,
    sub_sub_args: &ArgMatches,
) -> Result<(), Error> {
    let arch = Archive::connect(common_args.root())?;

    let pass = &|_site: &Site| -> bool { true };

    let missing_any = &|site: &Site| -> bool {
        site.lat.is_none()
            || site.lon.is_none()
            || site.elev_m.is_none()
            || site.name.is_none()
            || site.state.is_none()
            || site.notes.is_none()
    };

    let missing_state = &|site: &Site| -> bool { site.state.is_none() };

    let missing_any_pred: &Fn(&Site) -> bool = if sub_sub_args.is_present("missing-data") {
        missing_any
    } else {
        pass
    };

    let missing_state_pred: &Fn(&Site) -> bool = if sub_sub_args.is_present("missing-state") {
        missing_state
    } else {
        pass
    };

    let master_list = arch.get_sites()?;
    let sites_iter = || {
        master_list
            .iter()
            .filter(|s| missing_any_pred(s))
            .filter(|s| missing_state_pred(s))
    };

    if sites_iter().count() == 0 {
        println!("No sites matched criteria.");
    } else {
        println!(
            "{:^4} {:^6} {:^7} {:^7} {:^5} {:^20} : {}",
            "ID", "LAT", "LON", "ELEV(m)", "STATE", "NAME", "NOTES"
        );
    }

    let blank = "-".to_owned();

    for site in sites_iter() {
        let id = &site.id;
        let lat = site
            .lat
            .map(|lat| format!("{:6.2}", lat))
            .unwrap_or_else(|| "-".to_owned());
        let lon = site
            .lon
            .map(|lon| format!("{:7.2}", lon))
            .unwrap_or_else(|| "-".to_owned());
        let elev = site
            .elev_m
            .map(|elev| format!("{:7.0}", elev))
            .unwrap_or_else(|| "-".to_owned());
        let state = site.state.map(|st| st.as_static()).unwrap_or(&"-");
        let name = site.name.as_ref().unwrap_or(&blank);
        let notes = site.notes.as_ref().unwrap_or(&blank);
        println!(
            "{:<4} {:>} {:>} {:>} {:>5} {:>20} : {}",
            id, lat, lon, elev, state, name, notes
        );
    }

    Ok(())
}
