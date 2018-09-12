//! BufKit Archive Manager

extern crate clap;
extern crate failure;

extern crate bufkit_data;

use bufkit_data::{CommonCmdLineArgs, Archive};
use clap::{SubCommand, Arg, ArgMatches};
use failure::{Error, Fail, err_msg};

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
                        .help("Overwrite any existing archive at `root`.")
                )
        );

    let (common_args, matches) = CommonCmdLineArgs::matches(app, true)?;
    
    match matches.subcommand() {
        ("create", Some(sub_args)) => create(common_args, sub_args)?,
        _                          => unreachable!(),
    }

    Ok(())
}

fn create(common_args: CommonCmdLineArgs, sub_args: &ArgMatches) -> Result<(), Error> {

    // Check if the archive already exists. (try connecting to it)
    let already_exists: bool = Archive::connect(common_args.root()).is_ok();

    if already_exists && sub_args.is_present("force") {
        ::std::fs::remove_dir_all(common_args.root())?;
    } else if already_exists {
        return Err(err_msg("Archive already exists, must use --force to overwrite."));
    }
    
    Archive::create_new(common_args.root())?;

    Ok(())
}
