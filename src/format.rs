use crate::{Table, DataType};
use std::fmt::{Display, Formatter, Result as FmtResult};
use prettytable::{Attr, Cell as PCell, Row as PRow, Table as PTable};

impl Display for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let mut display_table = PTable::new();
        display_table.set_format(*prettytable::format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

        // Create the header row first
        let header_row = PRow::new(
            self.columns
                .keys()
                .map(|identifier| {
                    PCell::new(identifier)
                        .with_style(Attr::Bold)
                        .with_style(Attr::ForegroundColor(prettytable::color::GREEN))
                })
                .collect(),
        );
        display_table.set_titles(header_row);

        // Fill in the values
        self.keys.iter().for_each(|(_, index)| {
            let mut row = PRow::empty();
            self.columns.iter().for_each(|(_, column)| {
                let val = column.value_at(*index).unwrap();
                println!("{:?}", val);
                row.add_cell(PCell::new(&String::from(val)))
            });

            display_table.add_row(row);
        });

        display_table.fmt(f)
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            DataType::Integer => f.write_str("INTEGER"),
            DataType::String => f.write_str("STRING"),
            DataType::Uuid => f.write_str("UUID"),
        }
    }
}