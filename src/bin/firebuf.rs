//! firebuf - Calculate fire weather indicies from soundings in your Bufkit Archive.
extern crate bufkit_data;
extern crate chrono;
extern crate clap;
extern crate failure;
extern crate sounding_analysis;
extern crate sounding_bufkit;
extern crate strum;
#[macro_use]
extern crate strum_macros;
extern crate textplots;
extern crate unicode_width;

use bufkit_data::{Archive, BufkitDataErr, CommonCmdLineArgs, Model};
use chrono::{NaiveDate, NaiveDateTime};
use clap::Arg;
use failure::{Error, Fail};
use std::path::PathBuf;
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
    let args = parse_args()?;

    println!("{:#?}", args);

    Ok(())
}

fn parse_args() -> Result<CmdLineArgs, Error> {
    let app = CommonCmdLineArgs::new_app("firebuf", "Fire weather analysis & summary.")
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
                .possible_values(
                    &Model::iter()
                        .map(|val| val.as_static())
                        .collect::<Vec<&str>>(),
                ).help("Allowable models for this operation/program.")
                .long_help(concat!(
                    "Allowable models for this operation/program.",
                    " Default is to use all possible values."
                )),
        ).arg(
            Arg::with_name("table-stats")
                .multiple(true)
                .short("t")
                .long("table-stats")
                .takes_value(true)
                .possible_values(
                    &Stat::iter()
                        .map(|val| val.as_static())
                        .collect::<Vec<&str>>(),
                ).help("Which statistics to show in the table.")
                .long_help("Which statistics to show in the table. Defaults to all possible."),
        ).arg(
            Arg::with_name("graph-stats")
                .multiple(true)
                .short("g")
                .long("graph-stats")
                .takes_value(true)
                .possible_values(
                    &Stat::iter()
                        .map(|val| val.as_static())
                        .collect::<Vec<&str>>(),
                ).help("Which statistics to plot make a graph for.")
                .long_help(concat!(
                    "Which statistics to plot make a graph for.",
                    " Defaults to all possible.",
                    " All graphs plot all available data."
                )),
        ).arg(
            Arg::with_name("mode")
                .takes_value(true)
                .multiple(false)
                .possible_values(
                    &Mode::iter()
                        .map(|val| val.as_static())
                        .collect::<Vec<&str>>(),
                ).long("mode")
                .help("Which daily value to put in the table.")
                .default_value("00Z"),
        ).arg(
            Arg::with_name("init-time")
                .long("init-time")
                .short("i")
                .takes_value(true)
                .help("The model inititialization time. YYYY-MM-DD-HH")
                .long_help(concat!(
                    "The initialization time of the model run to analyze.",
                    " Format is YYYY-MM-DD-HH. If not specified then the model run is assumed to",
                    " be the last available run in the  archive."
                )),
        );

    let (common_args, matches) = CommonCmdLineArgs::matches(app)?;

    let bail = |msg: &str| -> ! {
        println!("{}", msg);
        ::std::process::exit(1);
    };

    let arch = Archive::connect(common_args.root())?;

    let root = common_args.root().to_path_buf();
    let mut sites: Vec<String> = matches
        .values_of("sites")
        .into_iter()
        .flat_map(|site_iter| site_iter.map(|arg_val| arg_val.to_owned()))
        .collect();

    if sites.is_empty() {
        sites = arch.get_sites()?.into_iter().map(|site| site.id).collect();
    }

    let mut models: Vec<Model> = matches
        .values_of("models")
        .into_iter()
        .flat_map(|model_iter| model_iter.map(Model::from_str))
        .filter_map(|res| res.ok())
        .collect();

    if models.is_empty() {
        models = Model::iter().collect();
    }

    let mut table_stats: Vec<Stat> = matches
        .values_of("table-stats")
        .into_iter()
        .flat_map(|stat_iter| stat_iter.map(Stat::from_str))
        .filter_map(|res| res.ok())
        .collect();

    if table_stats.is_empty() {
        table_stats = Stat::iter().collect();
    }

    let mut graph_stats: Vec<Stat> = matches
        .values_of("graph-stats")
        .into_iter()
        .flat_map(|stat_iter| stat_iter.map(Stat::from_str))
        .filter_map(|res| res.ok())
        .collect();

    if graph_stats.is_empty() {
        graph_stats = Stat::iter().collect();
    }

    let parse_date_string = |dt_str: &str| -> NaiveDateTime {
        let hour: u32 = match dt_str[11..].parse() {
            Ok(hour) => hour,
            Err(_) => bail(&format!("Could not parse date: {}", dt_str)),
        };

        let date = match NaiveDate::parse_from_str(&dt_str[..10], "%Y-%m-%d") {
            Ok(date) => date,
            Err(_) => bail(&format!("Could not parse date: {}", dt_str)),
        };

        date.and_hms(hour, 0, 0)
    };

    let init_time = if let Some(start_date) = matches.value_of("start") {
        parse_date_string(start_date)
    } else {
        // Search and hunt for a combination that works!
        let mut res = Err(BufkitDataErr::NotEnoughData);
        for &model in &models {
            for site in &sites {
                res = arch.get_most_recent_valid_time(&site, model);
                if res.is_ok() {
                    break;
                }
            }
        }
        match res {
            Ok(init_time) => init_time,
            Err(_) => bail(&format!(
                concat!(
                    "Unable to find model run for any site-model ",
                    "combination provided: {:#?} {:#?}"
                ),
                sites,
                models
            )),
        }
    };

    let mode: Mode = matches
        .value_of("mode")
        .and_then(|m| Mode::from_str(m).ok())
        .unwrap_or(Mode::ZeroZ);

    Ok(CmdLineArgs {
        root,
        sites,
        models,
        init_time,
        table_stats,
        graph_stats,
        mode,
    })
}

#[derive(Debug)]
struct CmdLineArgs {
    root: PathBuf,
    sites: Vec<String>,
    models: Vec<Model>,
    init_time: NaiveDateTime,
    table_stats: Vec<Stat>,
    graph_stats: Vec<Stat>,
    mode: Mode,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, EnumString, AsStaticStr, EnumIter)]
enum Stat {
    #[strum(serialize = "HDW")]
    Hdw,
    HainesLow,
    HainesMid,
    HainesHigh,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, EnumString, AsStaticStr, EnumIter)]
enum Mode {
    #[strum(serialize = "00Z")]
    ZeroZ,
    DailyMax,
}

#[allow(dead_code)]
mod table_printer {
    use failure::Error;
    use std::fmt::{Display, Write};
    use unicode_width::UnicodeWidthStr;

    #[derive(Default, Debug)]
    pub struct TablePrinter {
        title: Option<String>,
        header: Option<String>,
        footer: Option<String>,
        column_names: Vec<String>,
        columns: Vec<Vec<String>>,
        fill: String,
    }

    impl TablePrinter {
        pub fn new() -> Self {
            TablePrinter::default()
        }

        pub fn with_title<T>(self, title: T) -> Self
        where
            Option<String>: From<T>,
        {
            TablePrinter {
                title: Option::from(title),
                ..self
            }
        }

        pub fn with_header<T>(self, header: T) -> Self
        where
            Option<String>: From<T>,
        {
            TablePrinter {
                header: Option::from(header),
                ..self
            }
        }

        pub fn with_footer<T>(self, footer: T) -> Self
        where
            Option<String>: From<T>,
        {
            TablePrinter {
                footer: Option::from(footer),
                ..self
            }
        }

        pub fn with_fill<T: AsRef<str>>(self, fill_string: T) -> Self {
            TablePrinter {
                fill: fill_string.as_ref().to_owned(),
                ..self
            }
        }

        pub fn with_column<T, V>(self, col_name: T, col_vals: &[V]) -> Self
        where
            T: Display,
            V: Display,
        {
            let mut column_names = self.column_names;
            let mut columns = self.columns;

            column_names.push(format!("{}", col_name));

            let col_vals: Vec<String> = col_vals.iter().map(|v| format!("{}", v)).collect();

            columns.push(col_vals);

            TablePrinter {
                column_names,
                columns,
                ..self
            }
        }

        pub fn print(self) -> Result<(), Error> {
            //
            // Calculate widths
            //
            let mut table_width = self
                .title
                .as_ref()
                .and_then(|title| Some(UnicodeWidthStr::width(title.as_str())))
                .unwrap_or(0);
            let mut col_widths = vec![0; self.columns.len()];

            for (i, col_name) in self.column_names.iter().enumerate() {
                let mut width = UnicodeWidthStr::width(col_name.as_str());
                if col_widths[i] < width {
                    col_widths[i] = width;
                }

                for row in self.columns[i].iter() {
                    width = UnicodeWidthStr::width(row.as_str());
                    if col_widths[i] < width {
                        col_widths[i] = width;
                    }
                }
            }

            debug_assert!(self.columns.len() > 0, "Must add a column.");
            let all_cols_width: usize =
                col_widths.iter().map(|x| *x).sum::<usize>() + col_widths.len() - 1;
            if all_cols_width > table_width {
                table_width = all_cols_width;
            }

            //
            // Function to split the header/footers into lines
            //
            fn wrapper<'a>(text: &'a str, table_width: usize) -> Vec<&'a str> {
                let mut to_ret: Vec<&str> = vec![];

                let mut remaining = &text[..];
                while remaining.len() > table_width {
                    let guess = &remaining[..table_width];

                    let right_edge = guess
                        .find(|c| c == '\n')
                        .or_else(|| guess.rfind(char::is_whitespace))
                        .unwrap_or(table_width);
                    to_ret.push(&remaining[..right_edge]);
                    remaining = remaining[right_edge..].trim();
                }
                to_ret.push(remaining);

                to_ret
            };

            //
            // Build the string.
            //
            let mut builder = String::with_capacity(2000); // This should be enough
            write!(&mut builder, "\n")?;

            //
            // Print the title
            //
            let mut left_char: char;
            let mut right_char: char;
            if let Some(title) = self.title {
                // print top border
                write!(
                    &mut builder,
                    "\u{250c}{}\u{2510}\n",
                    "\u{2500}".repeat(table_width)
                )?;
                // print title
                write!(
                    &mut builder,
                    "\u{2502}{0:^1$}\u{2502}\n",
                    title, table_width
                )?;

                // set up the border type for the next line.
                left_char = '\u{251c}';
                right_char = '\u{2524}';
            } else {
                left_char = '\u{250c}';
                right_char = '\u{2510}';
            }

            //
            // Print the header
            //
            if let Some(header) = self.header {
                // print top border -  or a horizontal line
                write!(
                    &mut builder,
                    "{}{}{}\n",
                    left_char,
                    "\u{2500}".repeat(table_width),
                    right_char
                )?;
                for line in wrapper(&header, table_width) {
                    write!(&mut builder, "\u{2502}{0:<1$}\u{2502}\n", line, table_width)?;
                }

                // set up the border type for the next line.
                left_char = '\u{251c}';
                right_char = '\u{2524}';
            }

            //
            // Print the column names
            //

            // print top border above columns
            write!(&mut builder, "{}", left_char)?;
            for &width in &col_widths[..(col_widths.len() - 1)] {
                write!(&mut builder, "{}\u{252C}", "\u{2500}".repeat(width))?;
            }
            write!(
                &mut builder,
                "{}{}\n",
                "\u{2500}".repeat(col_widths[col_widths.len() - 1]),
                right_char
            )?;

            // print column names
            for i in 0..self.column_names.len() {
                write!(
                    &mut builder,
                    "\u{2502}{0:^1$}",
                    self.column_names[i], col_widths[i]
                )?;
            }
            write!(&mut builder, "\u{2502}\n")?;

            //
            // Print the data rows
            //

            // print border below column names
            write!(&mut builder, "\u{251C}")?;
            for &width in &col_widths[..(col_widths.len() - 1)] {
                write!(&mut builder, "{}\u{253C}", "\u{2500}".repeat(width))?;
            }
            write!(
                &mut builder,
                "{}\u{2524}\n",
                "\u{2500}".repeat(col_widths[col_widths.len() - 1])
            )?;

            // print rows
            let num_rows = self.columns.iter().map(|col| col.len()).max().unwrap_or(0);
            for i in 0..num_rows {
                for j in 0..self.columns.len() {
                    let val = self.columns[j].get(i).unwrap_or(&self.fill);
                    write!(&mut builder, "\u{2502}{0:>1$}", val, col_widths[j])?;
                }
                write!(&mut builder, "\u{2502}\n")?;
            }

            //
            // Print the footer
            //

            // print border below data
            if self.footer.is_some() {
                left_char = '\u{251c}';
                right_char = '\u{2524}';
            } else {
                left_char = '\u{2514}';
                right_char = '\u{2518}';
            }
            write!(&mut builder, "{}", left_char)?;
            for &width in &col_widths[..(col_widths.len() - 1)] {
                write!(&mut builder, "{}\u{2534}", "\u{2500}".repeat(width))?;
            }
            write!(
                &mut builder,
                "{}{}\n",
                "\u{2500}".repeat(col_widths[col_widths.len() - 1]),
                right_char
            )?;

            if let Some(footer) = self.footer {
                for line in wrapper(&footer, table_width) {
                    write!(&mut builder, "\u{2502}{0:<1$}\u{2502}\n", line, table_width)?;
                }

                // print very bottom border -  or a horizontal line
                write!(
                    &mut builder,
                    "\u{2514}{}\u{2518}\n",
                    "\u{2500}".repeat(table_width)
                )?;
            }

            print!("{}", builder);

            Ok(())
        }
    }
}
