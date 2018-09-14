//! BufKit Archive Manager

extern crate clap;
extern crate failure;
extern crate strum;

extern crate bufkit_data;

use bufkit_data::{Archive, CommonCmdLineArgs, Model, Site, StateProv};
use clap::{Arg, ArgMatches, SubCommand};
use failure::{err_msg, Error, Fail};
use std::str::FromStr;
use strum::{AsStaticRef, IntoEnumIterator};

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
                .about("Create a new archive. Ignores all global options except --root.")
                .arg(
                    Arg::with_name("force")
                        .long("force")
                        .help("Overwrite any existing archive at `root`."),
                ),
        ).subcommand(
            SubCommand::with_name("sites")
                .about("View and manipulate site data. Ignores all global options except --root.")
                .subcommand(
                    SubCommand::with_name("list")
                        .about(concat!(
                            "List sites in the data base. Ignores all global options",
                            " except --root."
                        )).arg(
                            Arg::with_name("missing-data")
                                .short("m")
                                .long("missing-data")
                                .help("Sites with any missing info."),
                        ).arg(
                            Arg::with_name("missing-state")
                                .long("missing-state")
                                .help("Only sites missing state/providence."),
                        ).arg(
                            Arg::with_name("state")
                                .long("state")
                                .help("Only sites in the given state.")
                                .takes_value(true),
                        ),
                ).subcommand(
                    SubCommand::with_name("modify")
                        .about(concat!(
                            "Modify the entry for a site. Ignores all global options ",
                            "except --root."
                        )).arg(
                            Arg::with_name("site")
                                .index(1)
                                .required(true)
                                .takes_value(true)
                                .help("The site to modify."),
                        ).arg(
                            Arg::with_name("state")
                                .long("state")
                                .takes_value(true)
                                .help("Set the state field to this value."),
                        ).arg(
                            Arg::with_name("name")
                                .long("name")
                                .takes_value(true)
                                .help("Set the name field to this value."),
                        ).arg(
                            Arg::with_name("notes")
                                .long("notes")
                                .takes_value(true)
                                .help("Set the name field to this value."),
                        ),
                ).subcommand(
                    SubCommand::with_name("inv")
                        .about(concat!(
                            "Get the inventory of soundings for a site. Ignores all ",
                            "global options except --root."
                        )).arg(
                            Arg::with_name("site")
                                .index(1)
                                .required(true)
                                .takes_value(true)
                                .help("The site to get the inventory for."),
                        ),
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
        ("modify", Some(sub_sub_args)) => sites_modify(common_args, sub_args, &sub_sub_args),
        ("inv", Some(sub_sub_args)) => sites_inventory(common_args, sub_args, &sub_sub_args),
        _ => unreachable!(),
    }
}

fn sites_list(
    common_args: CommonCmdLineArgs,
    _sub_args: &ArgMatches,
    sub_sub_args: &ArgMatches,
) -> Result<(), Error> {
    let arch = Archive::connect(common_args.root())?;

    //
    // This filter lets all sites pass
    //
    let pass = &|_site: &Site| -> bool { true };

    //
    // Filter based on state
    //
    let state_value = if let Some(st) = sub_sub_args.value_of("state") {
        StateProv::from_str(&st.to_uppercase()).unwrap_or(StateProv::AL)
    } else {
        StateProv::AL
    };

    let state_filter = &|site: &Site| -> bool {
        match site.state {
            Some(st) => st == state_value,
            None => false,
        }
    };
    let in_state_pred: &Fn(&Site) -> bool = if sub_sub_args.is_present("state") {
        state_filter
    } else {
        pass
    };

    //
    // Filter for missing any data
    //
    let missing_any = &|site: &Site| -> bool {
        site.name.is_none() || site.state.is_none() || site.notes.is_none()
    };
    let missing_any_pred: &Fn(&Site) -> bool = if sub_sub_args.is_present("missing-data") {
        missing_any
    } else {
        pass
    };

    //
    // Filter for missing state
    //
    let missing_state = &|site: &Site| -> bool { site.state.is_none() };
    let missing_state_pred: &Fn(&Site) -> bool = if sub_sub_args.is_present("missing-state") {
        missing_state
    } else {
        pass
    };

    //
    // Combine filters to make an iterator over the sites.
    //
    let master_list = arch.get_sites()?;
    let sites_iter = || {
        master_list
            .iter()
            .filter(|s| missing_any_pred(s))
            .filter(|s| missing_state_pred(s))
            .filter(|s| in_state_pred(s))
    };

    if sites_iter().count() == 0 {
        println!("No sites matched criteria.");
    } else {
        println!("{:^4} {:^5} {:<20} : {}", "ID", "STATE", "NAME", "NOTES");
    }

    let blank = "-".to_owned();

    for site in sites_iter() {
        let id = &site.id;
        let state = site.state.map(|st| st.as_static()).unwrap_or(&"-");
        let name = site.name.as_ref().unwrap_or(&blank);
        let notes = site.notes.as_ref().unwrap_or(&blank);
        println!("{:<4} {:^5} {:<20} : {}", id, state, name, notes);
    }

    Ok(())
}

fn sites_modify(
    common_args: CommonCmdLineArgs,
    _sub_args: &ArgMatches,
    sub_sub_args: &ArgMatches,
) -> Result<(), Error> {
    let arch = Archive::connect(common_args.root())?;

    // Safe to unwrap because the argument is required.
    let site = sub_sub_args.value_of("site").unwrap();
    let mut site = arch.get_site_info(site)?;

    if let Some(new_state) = sub_sub_args.value_of("state") {
        match StateProv::from_str(&new_state.to_uppercase()) {
            Ok(new_state) => site.state = Some(new_state),
            Err(_) => println!("Unable to parse state/providence: {}", new_state),
        }
    }

    if let Some(new_name) = sub_sub_args.value_of("name") {
        site.name = Some(new_name.to_owned());
    }

    if let Some(new_notes) = sub_sub_args.value_of("notes") {
        site.notes = Some(new_notes.to_owned())
    }

    arch.set_site_info(&site)?;
    Ok(())
}

fn sites_inventory(
    common_args: CommonCmdLineArgs,
    _sub_args: &ArgMatches,
    sub_sub_args: &ArgMatches,
) -> Result<(), Error> {
    let arch = Archive::connect(common_args.root())?;

    // Safe to unwrap because the argument is required.
    let site = sub_sub_args.value_of("site").unwrap();

    for model in Model::iter() {
        let inv = arch.get_inventory(site, model)?;

        println!("\nInventory for {} at {}.", model, site.to_uppercase());
        println!("   start: {}", inv.first);
        println!("     end: {}", inv.last);

        if inv.missing.is_empty() {
            println!("\n   No missing runs!");
        } else {
            println!("Missing:");
            println!("{:^19} -> {:^19} : {:6}", "From", "To", "Cycles");
            for missing in inv.missing.iter() {
                let start = missing.0;
                let end = missing.1;
                let cycles = (end - start).num_hours() / model.hours_between_runs() + 1;
                println!("{} -> {} : {:6}", start, end, cycles);
            }
        }
    }

    Ok(())
}
