//! firebuf - Calculate fire weather indicies from soundings in your Bufkit Archive.
extern crate bufkit_data;
extern crate chrono;
extern crate failure;
extern crate sounding_analysis;
extern crate sounding_bufkit;
extern crate textplots;
extern crate unicode_width;

use failure::{Fail, Error};
use table_printer::TablePrinter;

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
    unimplemented!()
}

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
