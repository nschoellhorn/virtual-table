mod error;

use crate::error::VirtualTableError;
use linked_hash_map::LinkedHashMap;
use prettytable::{Attr, Cell as PCell, Row as PRow, Table as PTable};
use std::collections::HashMap;
use std::fmt::{Display, Formatter, Result as FmtResult};
use uuid::Uuid;

#[derive(Debug, Eq, PartialEq)]
pub struct Column {
    // The column has a name that needs to be unique inside of the table
    identifier: String,
    // The data type must be enforced over the whole column
    data_type: DataType,
    is_nullable: bool,

    // The values are stored in a vec, so its only accessible via its index.
    // This implies, that one can only effectively access a column value via the table,
    //  since the table stores a mapping between PK and Index
    values: Vec<Cell>,
}

impl Column {
    pub fn create(identifier: String, data_type: DataType, is_nullable: bool) -> Self {
        Column {
            identifier,
            data_type,
            is_nullable,
            values: Vec::new(),
        }
    }

    pub(crate) fn set_cell(&mut self, index: Index, cell: Cell) -> Result<(), VirtualTableError> {
        if self.data_type != cell.data_type {
            return Result::Err(VirtualTableError::InvalidDataType(
                self.identifier.clone(),
                self.data_type,
                cell.data_type,
            ));
        }

        // If we don't allow null values in this column, we need to throw an error
        if !self.is_nullable && cell.inner == TableValue::Null {
            return Result::Err(VirtualTableError::InvalidNullValue(self.identifier.clone()));
        }

        self.values.insert(index, cell);
        Result::Ok(())
    }

    pub(crate) fn destroy_cell(&mut self, index: Index) -> Result<Cell, VirtualTableError> {
        if index >= self.values.len() {
            // We got an invalid index, so we can't do anything at this point.
            return Result::Err(VirtualTableError::InvalidRowIndex(index));
        }

        Result::Ok(self.values.remove(index))
    }

    pub fn value_at(&self, index: Index) -> Option<&TableValue> {
        self.values.get(index).map(|cell| &cell.inner)
    }
}

pub type Index = usize;

pub struct Table {
    identifier: String,
    columns: LinkedHashMap<String, Column>,
    keys: HashMap<PrimaryKey, Index>,
}

impl Table {
    pub fn create(identifier: String, columns: Vec<ColumnDefinition>) -> Self {
        Table {
            identifier,
            columns: Table::create_columns_from_definition(columns),
            keys: HashMap::new(),
        }
    }

    // TODO: This should be "transactional" I guess.
    pub fn create_row(&mut self, row: Row) -> Result<(), Vec<VirtualTableError>> {
        if self.keys.contains_key(&row.primary_key) {
            return Result::Err(vec![VirtualTableError::DuplicatePrimaryKey(
                row.primary_key,
            )]);
        }

        let new_index = self.keys.len();
        self.keys.insert(row.primary_key, new_index);
        let errors = row
            .cells
            .into_iter()
            .map(|(identifier, cell_option)| {
                let column_option = self.columns.get_mut(&identifier);
                if column_option.is_none() {
                    return Some(VirtualTableError::UnknownColumn(String::from(identifier)));
                }

                let col = column_option.unwrap();

                // if we see a None cell in the row, we handle it as a NULL value
                let cell = match cell_option {
                    Some(c) => c,
                    None => Cell {
                        data_type: col.data_type,
                        inner: TableValue::Null,
                    },
                };

                col.set_cell(new_index, cell).err()
            })
            .flatten()
            .collect::<Vec<_>>();

        if !errors.is_empty() {
            // If we experienced any errors, we should reject all values from this column to avoid subsequent panics
            self.rollback_at_index(&row.primary_key, new_index);
            return Result::Err(errors);
        }

        Result::Ok(())
    }

    fn create_columns_from_definition(
        mut definitions: Vec<ColumnDefinition>,
    ) -> LinkedHashMap<String, Column> {
        // Extend the definitions by a first column "ID" which contains the PK
        definitions.insert(
            0,
            ColumnDefinition {
                data_type: DataType::Uuid,
                is_nullable: false,
                identifier: String::from("ID"),
            },
        );

        definitions
            .into_iter()
            .map(|def| {
                (
                    def.identifier.clone(),
                    Column::create(def.identifier, def.data_type, def.is_nullable),
                )
            })
            .collect()
    }

    pub fn update_row(&mut self, update_row: Row) -> Result<(), Vec<VirtualTableError>> {
        if !self.keys.contains_key(&update_row.primary_key) {
            return Result::Err(vec![VirtualTableError::UnknownPrimaryKey(
                update_row.primary_key,
            )]);
        }

        let row_index = self.keys.get(&update_row.primary_key).unwrap().clone();

        let errors = update_row
            .cells
            .into_iter()
            .map(|(identifier, cell_option)| {
                // if we see a None cell in the update row, we ignore it since that means the cell should not be updated (= partial update)
                if cell_option.is_none() {
                    return None;
                }

                let column_option = self.columns.get_mut(&identifier);
                if column_option.is_none() {
                    return Some(VirtualTableError::UnknownColumn(String::from(identifier)));
                }

                let col = column_option.unwrap();

                let cell = cell_option.unwrap();
                col.set_cell(row_index, cell).err()
            })
            .flatten()
            .collect::<Vec<_>>();

        if !errors.is_empty() {
            // If we experienced any errors, we should reject all values from this column to avoid subsequent panics
            self.rollback_at_index(&update_row.primary_key, row_index);
            return Result::Err(errors);
        }

        Result::Ok(())
    }

    fn rollback_at_index(&mut self, key: &PrimaryKey, index: Index) {
        self.columns.iter_mut().for_each(|(_, col)| {
            col.destroy_cell(index);
        });
        self.keys.remove(key);
    }
}

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

pub struct ColumnDefinition {
    pub identifier: String,
    pub data_type: DataType,
    pub is_nullable: bool,
}

#[derive(Debug)]
pub struct Row {
    primary_key: PrimaryKey,
    cells: HashMap<String, Option<Cell>>,
}

impl Row {
    pub fn create(table: &Table, primary_key: PrimaryKey) -> Self {
        Row {
            primary_key,
            cells: table
                .columns
                .iter()
                .map(|(identifier, column)| {
                    let val = if identifier == "ID" {
                        // TODO: This is not very nice, should redo this.
                        Some(Cell {
                            data_type: column.data_type,
                            inner: TableValue::Uuid(primary_key),
                        })
                    } else {
                        None
                    };

                    (identifier.clone(), val)
                })
                .collect(),
        }
    }

    pub fn set_cell(&mut self, column_identifier: String, cell: Cell) {
        self.cells.insert(column_identifier, Some(cell));
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum TableValue {
    Null,
    Integer(i64),
    String(String),
    Uuid(Uuid),
}

impl From<&TableValue> for String {
    fn from(value: &TableValue) -> Self {
        match value {
            TableValue::Null => String::from("*NULL*"),
            TableValue::Integer(i) => format!("{}", i),
            TableValue::String(str) => str.clone(),
            TableValue::Uuid(uuid) => format!("{}", uuid),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum DataType {
    Integer,
    String,
    Uuid,
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

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Cell {
    data_type: DataType,
    inner: TableValue,
}

impl From<i64> for TableValue {
    fn from(i: i64) -> TableValue {
        TableValue::Integer(i)
    }
}

impl From<String> for TableValue {
    fn from(str: String) -> TableValue {
        TableValue::String(str)
    }
}

impl From<&str> for TableValue {
    fn from(str: &str) -> TableValue {
        TableValue::String(String::from(str))
    }
}

pub trait IntoCell
where
    Self: Clone,
{
    fn into_cell(self) -> Cell;
}

impl IntoCell for i64 {
    fn into_cell(self) -> Cell {
        Cell {
            data_type: DataType::Integer,
            inner: TableValue::Integer(self),
        }
    }
}

impl IntoCell for String {
    fn into_cell(self) -> Cell {
        Cell {
            data_type: DataType::String,
            inner: TableValue::String(self),
        }
    }
}

impl IntoCell for &str {
    fn into_cell(self) -> Cell {
        Cell {
            data_type: DataType::String,
            inner: TableValue::String(self.to_owned()),
        }
    }
}

type PrimaryKey = Uuid;

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn create_demo_table() -> Table {
        Table::create(
            String::from("user"),
            vec![
                ColumnDefinition {
                    identifier: String::from("first_name"),
                    data_type: DataType::String,
                    is_nullable: false,
                },
                ColumnDefinition {
                    identifier: String::from("last_name"),
                    data_type: DataType::String,
                    is_nullable: false,
                },
                ColumnDefinition {
                    identifier: String::from("age"),
                    data_type: DataType::Integer,
                    is_nullable: true,
                },
            ],
        )
    }

    #[test]
    fn can_create_table() {
        let table = create_demo_table();

        let expected = "\
+----+------------+-----------+-----+
| ID | first_name | last_name | age |
+----+------------+-----------+-----+
+----+------------+-----------+-----+
";

        assert_eq!(expected, table.to_string().replace("\r\n", "\n"));
    }

    #[test]
    fn can_create_row() {
        let mut table = create_demo_table();
        let pk = Uuid::from_str("797724d9-491c-46ac-981c-566d6d65b199").unwrap();

        let mut row = Row::create(&table, pk);
        row.set_cell(String::from("first_name"), "first".into_cell());
        row.set_cell(String::from("last_name"), "last".into_cell());
        row.set_cell(String::from("age"), 69.into_cell());

        table.create_row(row);

        let expected = "\
+--------------------------------------+------------+-----------+-----+
| ID                                   | first_name | last_name | age |
+--------------------------------------+------------+-----------+-----+
| 797724d9-491c-46ac-981c-566d6d65b199 | first      | last      | 69  |
+--------------------------------------+------------+-----------+-----+
";

        assert_eq!(expected, table.to_string().replace("\r\n", "\n"));
    }

    #[test]
    fn can_partially_update_row() {
        let mut table = create_demo_table();
        let pk = Uuid::from_str("797724d9-491c-46ac-981c-566d6d65b199").unwrap();

        // Create the initial state of the row
        let mut row = Row::create(&table, pk);
        row.set_cell(String::from("first_name"), "first".into_cell());
        row.set_cell(String::from("last_name"), "last".into_cell());
        row.set_cell(String::from("age"), 69.into_cell());

        table.create_row(row);

        // Update only the first_name field of the row, then we expect everything else to stay the same
        let mut update_row = Row::create(&table, pk);
        update_row.set_cell(String::from("first_name"), "changed first name".into_cell());
        assert!(table.update_row(update_row).is_ok());

        let expected = "\
+--------------------------------------+--------------------+-----------+-----+
| ID                                   | first_name         | last_name | age |
+--------------------------------------+--------------------+-----------+-----+
| 797724d9-491c-46ac-981c-566d6d65b199 | changed first name | last      | 69  |
+--------------------------------------+--------------------+-----------+-----+
";

        assert_eq!(expected, table.to_string().replace("\r\n", "\n"));
    }

    #[test]
    fn it_rejects_values_with_different_data_types_than_the_column_definition() {
        let mut table = create_demo_table();
        let mut empty_row = Row::create(&table, Uuid::new_v4());

        // We try to set an integer value into the first_name cell which expects String values
        empty_row.set_cell(String::from("first_name"), 64.into_cell());

        let result = table.create_row(empty_row);
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs.contains(&VirtualTableError::InvalidDataType(
            String::from("first_name"),
            DataType::String,
            DataType::Integer
        )))
    }

    #[test]
    fn it_rejects_nulled_values_that_are_defined_as_not_nullable_in_the_column() {
        let mut table = create_demo_table();
        let empty_row = Row::create(&table, Uuid::new_v4());

        let result = table.create_row(empty_row);

        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(
            errs.contains(&VirtualTableError::InvalidNullValue(String::from(
                "first_name"
            )))
        );
        assert!(
            errs.contains(&VirtualTableError::InvalidNullValue(String::from(
                "last_name"
            )))
        );
    }
}
