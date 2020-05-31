use crate::{Index, DataType, PrimaryKey};
use std::fmt::{Formatter, Display, Result as FmtResult};

#[derive(Eq, PartialEq)]
pub enum VirtualTableError {
    InvalidRowIndex(Index),
    InvalidDataType(String, DataType, DataType),
    DuplicateColumnInRow(String),
    DuplicatePrimaryKey(PrimaryKey),
    UnknownColumn(String),
    UnknownPrimaryKey(PrimaryKey),
    InvalidNullValue(String),
}

impl Display for VirtualTableError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            VirtualTableError::InvalidRowIndex(index) => f.write_str(&format!(
                "Unable to find row with specified index of '{}'",
                index
            )),
            VirtualTableError::InvalidDataType(identifier, required_type, provided_type) => f
                .write_str(&format!(
                    "Invalid data type for column {}. Required is {} but {} was provided.",
                    identifier, required_type, provided_type
                )),
            VirtualTableError::DuplicateColumnInRow(column_identifier) => f.write_str(&format!(
                "A cell for column {} is already in this row.",
                column_identifier
            )),
            VirtualTableError::DuplicatePrimaryKey(primary_key) => f.write_str(&format!(
                "Can't create a new row with primary key {} since a row with this key already exists.",
                primary_key
            )),
            VirtualTableError::UnknownColumn(column_identifier) => f.write_str(&format!(
                "Didn't find a column with name {}",
                column_identifier
            )),
            VirtualTableError::InvalidNullValue(column_identifier) => f.write_str(&format!(
                "Column {} does not accept NULL values.",
                column_identifier
            )),
            VirtualTableError::UnknownPrimaryKey(key) => f.write_str(&format!(
                "Did not find a row with the primary key of {}",
                key
            )),
        }
    }
}