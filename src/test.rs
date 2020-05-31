use std::str::FromStr;
use uuid::Uuid;
use virtual_table::error::VirtualTableError;
use virtual_table::*;
use virtual_table::query::ColumnSpecification;

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
        DataType::Integer,
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

#[test]
fn it_can_fetch_rows_with_all_columns_via_primary_key() {
    let mut table = create_demo_table();

    let pk = Uuid::from_str("797724d9-491c-46ac-981c-566d6d65b199").unwrap();
    let mut row = Row::create(&table, pk);
    row.set_cell(String::from("first_name"), "first".into_cell());
    row.set_cell(String::from("last_name"), "last".into_cell());
    row.set_cell(String::from("age"), 69.into_cell());

    table.create_row(row.clone());

    assert_eq!(row, table.find_row(&pk, ColumnSpecification::All).expect("Expected a value here."));
}

#[test]
fn it_can_fetch_rows_with_selected_columns_via_primary_key() {
    let mut table = create_demo_table();

    let pk = Uuid::from_str("797724d9-491c-46ac-981c-566d6d65b199").unwrap();
    let mut row = Row::create(&table, pk);
    row.set_cell(String::from("first_name"), "first".into_cell());
    row.set_cell(String::from("last_name"), "last".into_cell());
    row.set_cell(String::from("age"), 69.into_cell());

    table.create_row(row.clone());

    let mut expected_row = Row::create(&table, pk);
    expected_row.set_cell(String::from("age"), 69.into_cell());

    assert_eq!(expected_row, table.find_row(&pk, ColumnSpecification::Some(vec![String::from("age")])).expect("Expected a value here."));
}
