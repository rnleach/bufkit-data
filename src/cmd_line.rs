//! Command line options that are used across applications.

use std::path::{Path, PathBuf};

use clap::{App, Arg, ArgMatches};
use dirs::home_dir;

use errors::BufkitDataErr;

/// Struct to package up command line arguments.
#[derive(Clone, Debug)]
pub struct CommonCmdLineArgs {
    // Path to the root of the archive
    root: PathBuf,
}

impl<'a, 'b> CommonCmdLineArgs {
    /// Create a new set of args.
    pub fn new_app(app_name: &'static str, about: &'static str) -> App<'a, 'b> {
        App::new(app_name)
            .author("Ryan Leach <clumsycodemonkey@gmail.com>")
            .about(about)
            .version(crate_version!())
            .arg(
                Arg::with_name("root")
                    .short("r")
                    .long("root")
                    .takes_value(true)
                    .global(true)
                    .help("Path to the archive.")
                    .long_help("Path to the archive. Defaults to '${HOME}/bufkit/'"),
            )
    }

    /// Process an `App` to get the parsed values out of it and the matches object so an application
    /// can continue with further argument parsing.
    pub fn matches(app: App<'a, 'b>) -> Result<(Self, ArgMatches<'a>), BufkitDataErr> {
        let matches = app.get_matches();

        let cmd_line_opts = {
            let root = matches
                .value_of("root")
                .map(PathBuf::from)
                .or_else(|| home_dir().and_then(|hd| Some(hd.join("bufkit"))))
                .expect("Invalid root.");

            CommonCmdLineArgs { root }
        };

        Ok((cmd_line_opts, matches))
    }

    /// Get the root of the archive
    pub fn root(&self) -> &Path {
        &self.root
    }
}
