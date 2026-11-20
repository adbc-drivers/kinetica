// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The CRUD round trip a tool exercises on any database, and the transaction
//! surface every ADBC client probes. live.rs reaches both in pieces, around
//! what it is really testing; here they are the subject.
//! The instance and its login come from KI_URL, KI_USER and KI_PWD.

use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::sync::{Connection, Database, Driver, Statement};
use adbc_core::Optionable;
use adbc_driver_kinetica::KineticaDriver;
use arrow_array::Array;

fn connect() -> adbc_driver_kinetica::KineticaConnection {
    connect_with([])
}

/// Opens a connection, with any extra database options the caller needs.
/// @return A connection to the instance under test
fn connect_with<I>(extra: I) -> adbc_driver_kinetica::KineticaConnection
where
    I: IntoIterator<Item = (OptionDatabase, OptionValue)>,
{
    let kinetica_adbc_tests::Login {
        url,
        user,
        password,
    } = kinetica_adbc_tests::login();

    let mut options = vec![
        (OptionDatabase::Uri, OptionValue::String(url)),
        (OptionDatabase::Username, OptionValue::String(user)),
        (OptionDatabase::Password, OptionValue::String(password)),
    ];
    options.extend(extra);

    let mut driver = KineticaDriver::default();

    driver
        .new_database_with_opts(options)
        .expect("database")
        .new_connection()
        .expect("connection")
}

/// Runs a statement that changes something.
/// @return The number of rows affected, if the server reported one
fn run(connection: &mut adbc_driver_kinetica::KineticaConnection, sql: &str) -> Option<i64> {
    let mut statement = connection.new_statement().expect("statement");
    statement.set_sql_query(sql).expect("query");
    statement.execute_update().expect("run")
}

/// Reads a query returning one integer in one row. COUNT comes back as a
/// BIGINT and an INTEGER column as an INT, so both are accepted rather than
/// making every caller cast.
/// @return That value
fn scalar(connection: &mut adbc_driver_kinetica::KineticaConnection, sql: &str) -> i64 {
    let mut statement = connection.new_statement().expect("statement");
    statement.set_sql_query(sql).expect("query");

    let batches: Vec<_> = statement
        .execute()
        .expect("execute")
        .map(|b| b.expect("batch"))
        .collect();

    let column = batches[0].column(0);

    if let Some(values) = column.as_any().downcast_ref::<arrow_array::Int64Array>() {
        return values.value(0);
    }

    i64::from(
        column
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap_or_else(|| panic!("an integer, got {}", column.data_type()))
            .value(0),
    )
}

// --------------------------------------------------------------------------
// Create, read, update, delete.
// --------------------------------------------------------------------------

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_table_goes_through_create_insert_read_update_delete_and_drop() {
    let mut connection = connect();
    let table = "adbc_crud_round_trip";

    run(&mut connection, &format!("DROP TABLE IF EXISTS {table}"));

    // Create.
    run(
        &mut connection,
        &format!("CREATE TABLE {table} (id INTEGER, name VARCHAR, amount DOUBLE)"),
    );
    assert_eq!(
        scalar(&mut connection, &format!("SELECT COUNT(*) FROM {table}")),
        0
    );

    // Insert. The server reports what it wrote, which is what a tool shows.
    assert_eq!(
        run(
            &mut connection,
            &format!(
                "INSERT INTO {table} VALUES (1, 'one', 1.5), (2, 'two', 2.5), \
                      (3, 'three', 3.5)"
            )
        ),
        Some(3)
    );

    // Read, including a predicate and an aggregate, not just a bare scan.
    assert_eq!(
        scalar(&mut connection, &format!("SELECT COUNT(*) FROM {table}")),
        3
    );
    assert_eq!(
        scalar(
            &mut connection,
            &format!("SELECT id FROM {table} WHERE name = 'two'")
        ),
        2
    );

    // Update.
    assert_eq!(
        run(
            &mut connection,
            &format!("UPDATE {table} SET amount = 99.5 WHERE id = 2")
        ),
        Some(1)
    );
    assert_eq!(
        scalar(
            &mut connection,
            &format!("SELECT COUNT(*) FROM {table} WHERE amount = 99.5")
        ),
        1
    );

    // An update that matches nothing reports nothing rather than failing.
    assert_eq!(
        run(
            &mut connection,
            &format!("UPDATE {table} SET amount = 0 WHERE id = 9999")
        ),
        Some(0)
    );

    // Delete.
    assert_eq!(
        run(
            &mut connection,
            &format!("DELETE FROM {table} WHERE id = 1")
        ),
        Some(1)
    );
    assert_eq!(
        scalar(&mut connection, &format!("SELECT COUNT(*) FROM {table}")),
        2
    );

    // Drop, and the table is gone rather than empty.
    run(&mut connection, &format!("DROP TABLE {table}"));

    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_sql_query(format!("SELECT COUNT(*) FROM {table}"))
        .expect("query");
    assert!(
        statement.execute().is_err(),
        "the dropped table still answers"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_column_added_after_the_rows_reads_back_null() {
    let mut connection = connect();
    let table = "adbc_crud_alter";

    run(&mut connection, &format!("DROP TABLE IF EXISTS {table}"));
    run(
        &mut connection,
        &format!("CREATE TABLE {table} (id INTEGER)"),
    );
    run(
        &mut connection,
        &format!("INSERT INTO {table} VALUES (1), (2)"),
    );

    run(
        &mut connection,
        &format!("ALTER TABLE {table} ADD note VARCHAR"),
    );

    // The new column exists, and the rows that predate it hold nulls.
    assert_eq!(
        scalar(
            &mut connection,
            &format!("SELECT COUNT(*) FROM {table} WHERE note IS NULL")
        ),
        2
    );

    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_sql_query(format!("SELECT id, note FROM {table}"))
        .expect("query");

    let reader = statement.execute().expect("execute");
    assert_eq!(reader.schema().fields().len(), 2);

    run(&mut connection, &format!("DROP TABLE {table}"));
}

// --------------------------------------------------------------------------
// Transactions. Kinetica has none, so what matters is that the driver says so
// in the way an ADBC client can act on, and that a client can ask for the
// transaction calls to be accepted and ignored instead.
// --------------------------------------------------------------------------

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn autocommit_reads_back_on_and_asking_for_it_again_is_accepted() {
    let mut connection = connect();

    assert_eq!(
        connection
            .get_option_string(OptionConnection::AutoCommit)
            .expect("autocommit"),
        "true"
    );

    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("true".into()),
        )
        .expect("asking for autocommit that is already on");
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn turning_autocommit_off_is_refused_and_so_are_commit_and_rollback() {
    let mut connection = connect();

    // Not implemented rather than a hard error, so a client that asks on
    // principle degrades instead of stopping.
    let refused = connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect_err("turning autocommit off should be refused");
    assert_eq!(refused.status, adbc_core::error::Status::NotImplemented);

    // Nothing was held back, so there is nothing to commit or undo; a caller
    // that rolls back after an error should learn that rather than believe it.
    assert!(connection.commit().is_err(), "commit should be refused");
    assert!(connection.rollback().is_err(), "rollback should be refused");
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_caller_that_asks_for_fake_transactions_gets_commit_and_rollback() {
    let mut connection = connect_with([(
        OptionDatabase::Other(adbc_driver_kinetica::options::FAKE_TRANSACTIONS.into()),
        OptionValue::String("true".into()),
    )]);

    assert_eq!(
        connection
            .get_option_string(OptionConnection::Other(
                adbc_driver_kinetica::options::FAKE_TRANSACTIONS.into()
            ))
            .expect("fake transactions"),
        "true"
    );

    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("turning autocommit off with fake transactions");

    // Accepted, but they still do not make an earlier statement undoable.
    connection.commit().expect("commit with fake transactions");
    connection
        .rollback()
        .expect("rollback with fake transactions");
}
