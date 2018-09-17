//! firebuf - Calculate fire weather indicies from soundings in your Bufkit Archive.
extern crate bufkit_data;
extern crate chrono;
extern crate clap;
extern crate failure;
extern crate sounding_analysis;
extern crate sounding_base;
extern crate sounding_bufkit;
extern crate strum;
#[macro_use]
extern crate strum_macros;
extern crate textplots;
extern crate unicode_width;

use bufkit_data::{Archive, CommonCmdLineArgs, Model};
use chrono::{Duration, NaiveDate, NaiveDateTime, Timelike};
use clap::Arg;
use failure::{Error, Fail};
use sounding_base::Sounding;
use sounding_bufkit::BufkitData;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use strum::{AsStaticRef, IntoEnumIterator};
use textplots::{Chart, Plot, Shape};

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
    let args = &parse_args()?;

    #[cfg(debug_assertions)]
    println!("{:#?}", args);

    let arch = &Archive::connect(&args.root)?;

    for site in args.sites.iter() {
        let stats = &calculate_stats(args, arch, site)?;

        if args.print {
            print_stats(args, site, stats)?;
        }

        if args.save_dir.is_some() {
            save_stats(args, site, stats)?;
        }
    }

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
                    &TableStatArg::iter()
                        .map(|val| val.as_static())
                        .collect::<Vec<&str>>(),
                ).help("Which statistics to show in the table.")
                .long_help(concat!(
                    "Which statistics to show in the table.",
                    " Defaults to HDW,  MaxHDW, HainesLow, HainesMid, and HainesHigh"
                )),
        ).arg(
            Arg::with_name("graph-stats")
                .multiple(true)
                .short("g")
                .long("graph-stats")
                .takes_value(true)
                .possible_values(
                    &GraphStatArg::iter()
                        .map(|val| val.as_static())
                        .collect::<Vec<&str>>(),
                ).help("Which statistics to plot make a graph for.")
                .long_help(concat!(
                    "Which statistics to plot a graph for.",
                    " Defaults to HDW.",
                    " All graphs plot all available data, but each model is on an individual axis."
                )),
        ).arg(
            Arg::with_name("init-time")
                .long("init-time")
                .short("i")
                .takes_value(true)
                .help("The model inititialization time. YYYY-MM-DD-HH")
                .long_help(concat!(
                    "The initialization time of the model run to analyze.",
                    " Format is YYYY-MM-DD-HH. If not specified then the model run is assumed to",
                    " be the last available run in the archive."
                )),
        ).arg(
            Arg::with_name("save-dir")
                .long("save-dir")
                .takes_value(true)
                .help("Directory to save .csv files to.")
                .long_help(concat!(
                    "Directory to save .csv files to. If this is specified then a file 'site.csv'",
                    " is created for each site in that directory with data for all models and",
                    " and all statistics."
                )),
        ).arg(
            Arg::with_name("print")
                .long("print")
                .short("p")
                .possible_values(&["Y", "N", "y", "n"])
                .default_value("y")
                .takes_value(true)
                .help("Print the results to the terminal."),
        );

    let (common_args, matches) = CommonCmdLineArgs::matches(app)?;

    let bail = |msg: &str| -> ! {
        println!("{}", msg);
        ::std::process::exit(1);
    };

    let arch = match Archive::connect(common_args.root()) {
        arch @ Ok(_) => arch,
        err @ Err(_) => {
            println!("Unable to connect to db, printing error and exiting.");
            err
        }
    }?;

    let root = common_args.root().to_path_buf();
    let mut sites: Vec<String> = matches
        .values_of("sites")
        .into_iter()
        .flat_map(|site_iter| site_iter.map(|arg_val| arg_val.to_owned()))
        .collect();

    if sites.is_empty() {
        sites = arch.get_sites()?.into_iter().map(|site| site.id).collect();
    }

    for site in sites.iter() {
        if !arch.site_exists(site)? {
            println!("Site {} not in the archive, skipping.", site);
        }
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

    let mut table_stats: Vec<TableStatArg> = matches
        .values_of("table-stats")
        .into_iter()
        .flat_map(|stat_iter| stat_iter.map(TableStatArg::from_str))
        .filter_map(|res| res.ok())
        .collect();

    if table_stats.is_empty() {
        use TableStatArg::{HainesHigh, HainesLow, HainesMid, Hdw, MaxHdw};
        table_stats = vec![Hdw, MaxHdw, HainesLow, HainesMid, HainesHigh];
    }

    let mut graph_stats: Vec<GraphStatArg> = matches
        .values_of("graph-stats")
        .into_iter()
        .flat_map(|stat_iter| stat_iter.map(GraphStatArg::from_str))
        .filter_map(|res| res.ok())
        .collect();

    if graph_stats.is_empty() {
        use GraphStatArg::Hdw;
        graph_stats = vec![Hdw];
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

    let init_time = matches.value_of("init-time").map(parse_date_string);

    let print: bool = {
        let arg_val = matches.value_of("print").unwrap(); // Safe, this is a required argument.

        arg_val == "Y" || arg_val == "y"
    };

    let save_dir: Option<PathBuf> = matches
        .value_of("save-dir")
        .map(str::to_owned)
        .map(PathBuf::from);

    save_dir.as_ref().and_then(|path| {
        if !path.is_dir() {
            bail(&format!("save-dir path {} does not exist.", path.display()));
        } else {
            Some(())
        }
    });

    Ok(CmdLineArgs {
        root,
        sites,
        models,
        init_time,
        table_stats,
        graph_stats,
        print,
        save_dir,
    })
}

fn calculate_stats(args: &CmdLineArgs, arch: &Archive, site: &str) -> Result<CalcStats, Error> {
    let mut to_return: CalcStats = CalcStats::new();

    for &model in args.models.iter() {
        let analysis = if let Some(ref init_time) = args.init_time {
            arch.get_file(site, model, init_time)
        } else {
            arch.get_most_recent_file(site, model)
        };
        let analysis = match analysis {
            Ok(analysis) => analysis,
            Err(_) => continue,
        };
        let analysis = BufkitData::new(&analysis)?;

        let mut model_stats = to_return.stats.entry(model).or_insert(ModelStats::new());

        let mut curr_time: Option<NaiveDateTime> = None;
        for anal in analysis.into_iter() {
            let sounding = anal.sounding();

            let valid_time = if let Some(valid_time) = sounding.get_valid_time() {
                valid_time
            } else {
                continue;
            };

            if curr_time.is_none() {
                model_stats.init_time = Some(valid_time);
            }
            curr_time = Some(valid_time);

            let cal_day = (valid_time - Duration::hours(12)).date(); // Daily stats from 12Z to 12Z

            // Build the graph stats
            for &graph_stat in args.graph_stats.iter() {
                use GraphStatArg::*;

                let mut graph_stats = model_stats
                    .graph_stats
                    .entry(graph_stat)
                    .or_insert(Vec::new());

                let stat = match graph_stat {
                    Hdw => sounding_analysis::hot_dry_windy(sounding),
                    HainesLow => sounding_analysis::haines_low(sounding),
                    HainesMid => sounding_analysis::haines_mid(sounding),
                    HainesHigh => sounding_analysis::haines_high(sounding),
                    AutoHaines => sounding_analysis::haines(sounding),
                    None => continue,
                };
                let stat = match stat {
                    Ok(stat) => stat,
                    Err(_) => continue,
                };

                graph_stats.push((valid_time, stat));
            }

            // Build the daily stats
            for &table_stat in args.table_stats.iter() {
                use TableStatArg::*;

                let zero_z = |old_val: (f64, u32), new_val: (f64, u32)| -> (f64, u32) {
                    if valid_time.hour() == 0 {
                        new_val
                    } else {
                        old_val
                    }
                };

                let max = |old_val: (f64, u32), new_val: (f64, u32)| -> (f64, u32) {
                    if new_val.0 > old_val.0 {
                        new_val
                    } else {
                        old_val
                    }
                };

                let mut table_stats = model_stats
                    .table_stats
                    .entry(table_stat)
                    .or_insert(HashMap::new());

                let stat_func: &Fn(&Sounding) -> Result<f64, _> = match table_stat {
                    Hdw => &sounding_analysis::hot_dry_windy,
                    MaxHdw => &sounding_analysis::hot_dry_windy,
                    HainesLow => &sounding_analysis::haines_low,
                    MaxHainesLow => &sounding_analysis::haines_low,
                    HainesMid => &sounding_analysis::haines_mid,
                    MaxHainesMid => &sounding_analysis::haines_mid,
                    HainesHigh => &sounding_analysis::haines_high,
                    MaxHainesHigh => &sounding_analysis::haines_high,
                    AutoHaines => &sounding_analysis::haines,
                    MaxAutoHaines => &sounding_analysis::haines,
                    None => continue,
                };
                let stat = match stat_func(sounding) {
                    Ok(stat) => stat,
                    Err(_) => continue,
                };

                let selector: &Fn((f64, u32), (f64, u32)) -> (f64, u32) = match table_stat {
                    Hdw => &zero_z,
                    MaxHdw => &max,
                    HainesLow => &zero_z,
                    MaxHainesLow => &max,
                    HainesMid => &zero_z,
                    MaxHainesMid => &max,
                    HainesHigh => &zero_z,
                    MaxHainesHigh => &max,
                    AutoHaines => &zero_z,
                    MaxAutoHaines => &max,
                    None => unreachable!(),
                };

                let mut day_entry = table_stats.entry(cal_day).or_insert((0.0, 12));
                let hour = valid_time.hour();

                *day_entry = selector(*day_entry, (stat, hour));
            }
        }

        model_stats.end_time = curr_time;
    }

    Ok(to_return)
}

fn print_stats(args: &CmdLineArgs, site: &str, stats: &CalcStats) -> Result<(), Error> {
    use table_printer::TablePrinter;

    for model in args.models.iter() {
        let stats = match stats.stats.get(model) {
            Some(stats) => stats,
            None => continue,
        };

        //
        // Table
        //
        if args
            .table_stats
            .iter()
            .all(|&stat| stat != TableStatArg::None)
        {
            let table_stats = &stats.table_stats;
            let vals = match table_stats.get(&args.table_stats[0]) {
                Some(vals) => vals,
                None => continue,
            };

            let mut days: Vec<NaiveDate> = vals.keys().cloned().collect();
            days.sort();

            let title = format!("Fire Indexes for {}.", site.to_uppercase());
            let header = format!(
                "{} data from {} to {}.",
                model,
                stats
                    .init_time
                    .map(|dt| dt.to_string())
                    .unwrap_or("unknown".to_owned()),
                stats
                    .end_time
                    .map(|dt| dt.to_string())
                    .unwrap_or("unknown".to_owned())
            );
            let footer = concat!(
                "For daily maximum values, first and last days may be partial. ",
                "Days run from 12Z on the date listed until 12Z the next day."
            ).to_owned();

            let mut tp = TablePrinter::new()
                .with_title(title)
                .with_header(header)
                .with_footer(footer)
                .with_column("Date", &days);

            for table_stat in args.table_stats.iter() {
                use TableStatArg::*;

                let vals = match table_stats.get(table_stat) {
                    Some(vals) => vals,
                    Option::None => continue,
                };

                let mut days: Vec<NaiveDate> = vals.keys().cloned().collect();
                days.sort();

                let daily_stat_values = days.iter().map(|d| vals[d]);
                let daily_stat_values: Vec<String> = match *table_stat {
                    Hdw | HainesLow | HainesMid | HainesHigh | AutoHaines => daily_stat_values
                        .map(|(val, _)| format!("{:.0}", val))
                        .collect(),
                    _ => daily_stat_values
                        .map(|(val, hour)| format!("{:.0} ({:02}Z)", val, hour))
                        .collect(),
                };

                tp = tp.with_column(table_stat.as_static(), &daily_stat_values);
            }

            tp.print_with_min_width(78)?;
        }

        //
        // END TABLE
        //

        //
        // GRAPHS
        //
        let graph_stats = &stats.graph_stats;
        for graph_stat in args.graph_stats.iter() {
            let vals = match graph_stats.get(graph_stat) {
                Some(vals) => vals,
                None => continue,
            };
            let base_time = if let Some(first) = vals.get(0) {
                first.0
            } else {
                continue;
            };

            let base_hour = if base_time.hour() == 0 {
                24f32
            } else {
                base_time.hour() as f32
            };

            let values_start = [
                (0.0, graph_stat.default_max_y()),
                (1.0 / 24.0, graph_stat.default_min_y()),
                ((base_hour - 1.0) / 24.0, graph_stat.default_min_y()),
            ];
            let values_iter = vals.iter().map(|&(v_time, val)| {
                (
                    ((v_time - base_time).num_hours() as f32 + base_hour) / 24.0,
                    val as f32,
                )
            });

            let values_plot: Vec<(f32, f32)> =
                values_start.iter().cloned().chain(values_iter).collect();

            println!(
                "{:^78}",
                format!(
                    "{} {} for {}",
                    model,
                    graph_stat.as_static(),
                    site.to_uppercase()
                )
            );

            Chart::new(160, 45, 0.0, 9.0)
                .lineplot(Shape::Steps(values_plot.as_slice()))
                .nice();
        }
        //
        // END GRAPHS
        //
    }

    Ok(())
}

fn save_stats(args: &CmdLineArgs, site: &str, stats: &CalcStats) -> Result<(), Error> {
    // Print the stats to the screen
    unimplemented!()
}

#[derive(Debug)]
struct CmdLineArgs {
    root: PathBuf,
    sites: Vec<String>,
    models: Vec<Model>,
    init_time: Option<NaiveDateTime>,
    table_stats: Vec<TableStatArg>,
    graph_stats: Vec<GraphStatArg>,
    print: bool,
    save_dir: Option<PathBuf>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, EnumString, AsStaticStr, EnumIter, Hash)]
enum GraphStatArg {
    #[strum(serialize = "HDW")]
    Hdw,
    HainesLow,
    HainesMid,
    HainesHigh,
    AutoHaines,
    None,
}

impl GraphStatArg {
    fn default_min_y(self) -> f32 {
        match self {
            _ => 0.0,
        }
    }

    fn default_max_y(self) -> f32 {
        use GraphStatArg::*;

        match self {
            Hdw => 500.0,
            _ => 6.0,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, EnumString, AsStaticStr, EnumIter, Hash)]
enum TableStatArg {
    #[strum(serialize = "HDW")]
    Hdw,
    #[strum(serialize = "MaxHDW")]
    MaxHdw,
    HainesLow,
    MaxHainesLow,
    HainesMid,
    MaxHainesMid,
    HainesHigh,
    MaxHainesHigh,
    AutoHaines,
    MaxAutoHaines,
    None,
}

#[derive(Debug)]
struct CalcStats {
    stats: HashMap<Model, ModelStats>,
}

impl CalcStats {
    fn new() -> Self {
        CalcStats {
            stats: HashMap::new(),
        }
    }
}

#[derive(Debug)]
struct ModelStats {
    graph_stats: HashMap<GraphStatArg, Vec<(NaiveDateTime, f64)>>,
    table_stats: HashMap<TableStatArg, HashMap<NaiveDate, (f64, u32)>>,
    init_time: Option<NaiveDateTime>,
    end_time: Option<NaiveDateTime>,
}

impl ModelStats {
    fn new() -> Self {
        ModelStats {
            graph_stats: HashMap::new(),
            table_stats: HashMap::new(),
            init_time: None,
            end_time: None,
        }
    }
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
            self.print_with_min_width(0)
        }

        pub fn print_with_min_width(self, min_width: usize) -> Result<(), Error> {
            //
            // Calculate widths
            //
            let title_width = self
                .title
                .as_ref()
                .and_then(|title| Some(UnicodeWidthStr::width(title.as_str()) + 2))
                .unwrap_or(0);

            let mut table_width = if min_width > title_width {
                min_width
            } else {
                title_width
            };
            let mut col_widths = vec![0; self.columns.len()];

            for (i, col_name) in self.column_names.iter().enumerate() {
                let mut width = UnicodeWidthStr::width(col_name.as_str()) + 2;
                if col_widths[i] < width {
                    col_widths[i] = width;
                }

                for row in self.columns[i].iter() {
                    width = UnicodeWidthStr::width(row.as_str()) + 2;
                    if col_widths[i] < width {
                        col_widths[i] = width;
                    }
                }
            }

            debug_assert!(self.columns.len() > 0, "Must add a column.");
            let mut all_cols_width: usize =
                col_widths.iter().cloned().sum::<usize>() + col_widths.len() - 1;

            //
            // Widen columns until they add to the width of the table (for really long titles)
            //
            while all_cols_width < table_width {
                let min = col_widths.iter().cloned().min().unwrap();
                for width in &mut col_widths {
                    if *width == min {
                        *width += 1;
                    }
                }
                all_cols_width = col_widths.iter().cloned().sum::<usize>() + col_widths.len() - 1;
            }

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
                    "\u{2502} {0:^1$} ",
                    self.column_names[i],
                    col_widths[i] - 2
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
                    write!(&mut builder, "\u{2502} {0:>1$} ", val, col_widths[j] - 2)?;
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
