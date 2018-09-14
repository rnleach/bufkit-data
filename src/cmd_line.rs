//! Command line options that are used across applications.

use std::path::{Path, PathBuf};
use std::str::FromStr;

use clap::{App, Arg, ArgMatches};
use dirs::home_dir;
use strum::IntoEnumIterator;

use archive::Archive;
use errors::BufkitDataErr;
use models::Model;

/// Struct to package up command line arguments.
#[derive(Clone, Debug)]
pub struct CommonCmdLineArgs {
    // List of sites, e.g. 3 or 4 letter identifiers kmso, kbtm, kord
    sites: Vec<String>,
    // Models to use
    models: Vec<Model>,
    // Path to the root of the archive
    root: PathBuf,
    // Number of days back to consider when running a command.
    days_back: i64,
}

impl<'a, 'b> CommonCmdLineArgs {
    const DEFAULT_DAYS_BACK: &'static str = "0";

    /// Create a new set of args.
    pub fn new_app(app_name: &'static str, about: &'static str) -> App<'a, 'b> {
        App::new(app_name)
            .author("Ryan Leach <clumsycodemonkey@gmail.com>")
            .about(about)
            .version(crate_version!())
            .arg(
                Arg::with_name("sites")
                    .multiple(true)
                    .short("s")
                    .long("sites")
                    .takes_value(true)
                    .help("Site identifiers (e.g. kord, katl, smn)."),
            ).arg(
                Arg::with_name("models")
                    .multiple(true)
                    .short("m")
                    .long("models")
                    .takes_value(true)
                    .help("Allowable models for this operation/program.")
                    .long_help("Allowable models for this operation/program. Case insensitive."),
            ).arg(
                Arg::with_name("root")
                    .short("r")
                    .long("root")
                    .takes_value(true)
                    .help("Path to the archive.")
                    .long_help("Path to the archive. Defaults to '${HOME}/bufkit/'"),
            ).arg(
                Arg::with_name("days-back")
                    .short("d")
                    .long("days-back")
                    .takes_value(true)
                    .default_value(Self::DEFAULT_DAYS_BACK)
                    .help("Number of days back to consider.")
                    .long_help("The number of days back to consider."),
            ).after_help(concat!(
                "For sites and models, if none are provided then the default is to",
                " use all available ones in the database.\n\n",
                "If days back is 0 (the default), then only process the latest model runs, ",
                "whatever they are."
            ))
    }

    /// Process an `App` to get the parsed values out of it and the matches object so an application
    /// can continue with further argument parsing.
    pub fn matches(
        app: App<'a, 'b>,
        allow_empty_lists: bool,
    ) -> Result<(Self, ArgMatches<'a>), BufkitDataErr> {
        let matches = app.get_matches();

        let cmd_line_opts = {
            let mut sites: Vec<String> = matches
                .values_of("sites")
                .into_iter()
                .flat_map(|site_iter| site_iter.map(|arg_val| arg_val.to_owned()))
                .collect();

            let mut models: Vec<Model> = matches
                .values_of("models")
                .into_iter()
                .flat_map(|model_iter| model_iter.map(Model::from_str))
                .filter_map(|res| res.ok())
                .collect();

            let root = matches
                .value_of("root")
                .map(PathBuf::from)
                .or_else(|| home_dir().and_then(|hd| Some(hd.join("bufkit"))))
                .expect("Invalid root.");

            let days_back = matches
                .value_of("days-back")
                .and_then(|val| val.parse::<i64>().ok())
                .expect("Invalid days-back, not parseable as an integer.");

            if !allow_empty_lists && sites.is_empty() {
                let arch = Archive::connect(&root)?;
                sites = arch.get_sites()?.into_iter().map(|site| site.id).collect();
            }

            if !allow_empty_lists && models.is_empty() {
                models = Model::iter().collect();
            }

            CommonCmdLineArgs {
                sites,
                models,
                root,
                days_back,
            }
        };

        let usage = matches.usage().to_owned();
        let print_usage_message = |msg: &str| -> ! {
            println!("\n{}\n\n{}\n", msg, usage);
            println!("Try the -h or --help option for more instructions.");
            ::std::process::exit(1);
        };

        if cmd_line_opts.days_back < 0 {
            print_usage_message("Invalid days-back, it must be 0 or a positive value!");
        }

        Ok((cmd_line_opts, matches))
    }

    /// Get the sites
    pub fn sites(&self) -> &[String] {
        &self.sites
    }

    /// Get the models
    pub fn models(&self) -> &[Model] {
        &self.models
    }

    /// Get the root of the archive
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Get the number of days back for this operation.
    pub fn days_back(&self) -> i64 {
        self.days_back
    }
}
