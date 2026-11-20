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

//! Server sessions, which are what a connection's SQL `#name` temp tables
//! belong to. The client puts its session id in every request; the server
//! scopes the tables to it and says when one is held, and /drop/session is
//! what lets go. The instance and its login come from KI_URL, KI_USER and KI_PWD.
//!
//! Each test names its tables after a suffix of its own, so they do not
//! collide with each other or with anything else using the server.

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::sync::{Connection, Database, Driver, Statement};
use adbc_driver_kinetica::KineticaDriver;
use arrow_array::Array;
use kinetica_adbc_tests::{clear_session_tables, drop_session, session_tables, unique_suffix};

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

/// @return A connection with a session of its own
fn connect() -> adbc_driver_kinetica::KineticaConnection {
    connect_with([])
}

/// @return A connection sharing the session the caller names
fn connect_to_session(session_id: &str) -> adbc_driver_kinetica::KineticaConnection {
    connect_with([(
        OptionDatabase::Other(adbc_driver_kinetica::options::SESSION_ID.to_string()),
        OptionValue::String(session_id.to_string()),
    )])
}

/// Runs a statement that changes something.
fn run(connection: &mut adbc_driver_kinetica::KineticaConnection, sql: &str) {
    let mut statement = connection.new_statement().expect("statement");
    statement.set_sql_query(sql).expect("query");
    statement.execute_update().expect("run");
}

/// Reads an INTEGER column of a query, in the order the query gave it.
/// @return Those values
fn ints(connection: &mut adbc_driver_kinetica::KineticaConnection, sql: &str) -> Vec<i32> {
    let mut statement = connection.new_statement().expect("statement");
    statement.set_sql_query(sql).expect("query");

    statement
        .execute()
        .expect("execute")
        .flat_map(|batch| {
            let batch = batch.expect("batch");
            let column = batch.column(0);
            let values = column
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap_or_else(|| panic!("an INTEGER, got {}", column.data_type()))
                .clone();

            (0..values.len()).map(move |row| values.value(row))
        })
        .collect()
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn each_connection_has_its_own_session() {
    // Two connections creating the same #name must not collide: before
    // sessions both were <user>_foo, so the second CREATE found the first.
    let suffix = unique_suffix();
    let table = format!("#own_{suffix}");

    let mut a = connect();
    let mut b = connect();

    run(&mut a, &format!("CREATE TABLE {table} (a INT)"));
    run(&mut b, &format!("CREATE TABLE {table} (a INT)"));
    run(&mut a, &format!("INSERT INTO {table} VALUES (1)"));
    run(&mut b, &format!("INSERT INTO {table} VALUES (2)"));

    assert_eq!(ints(&mut a, &format!("SELECT a FROM {table}")), vec![1]);
    assert_eq!(ints(&mut b, &format!("SELECT a FROM {table}")), vec![2]);

    // A table each, under the mangled names the sessions give them
    assert_eq!(
        session_tables(&format!("own_{suffix}")).len(),
        2,
        "a physical table per session"
    );

    drop(a);
    drop(b);
    clear_session_tables(&format!("own_{suffix}"));
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn closing_a_connection_drops_its_temp_tables() {
    // Without this the tables sit there until the server's idle sweep takes
    // them, an hour later by default and never where the timeout is 0.
    let suffix = unique_suffix();
    let table = format!("#closed_{suffix}");
    let ending = format!("closed_{suffix}");

    let mut connection = connect();
    run(&mut connection, &format!("CREATE TABLE {table} (a INT)"));
    run(&mut connection, &format!("INSERT INTO {table} VALUES (1)"));

    assert_eq!(session_tables(&ending).len(), 1, "the table while open");

    drop(connection);

    assert!(
        session_tables(&ending).is_empty(),
        "closing drops the session's tables, found {:?}",
        session_tables(&ending)
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_named_session_is_shared_and_left_to_the_caller() {
    // What adbc.kinetica.session_id is for: connections that share temp
    // tables, and a session the caller drops itself.
    let suffix = unique_suffix();
    let session_id = format!("adbc-{suffix}");
    let table = format!("#named_{suffix}");
    let ending = format!("named_{suffix}");

    let mut a = connect_to_session(&session_id);
    run(&mut a, &format!("CREATE TABLE {table} (a INT)"));
    run(&mut a, &format!("INSERT INTO {table} VALUES (1), (2)"));

    // The second connection reaches the first one's table, which is the point
    let mut b = connect_to_session(&session_id);
    assert_eq!(
        ints(&mut b, &format!("SELECT a FROM {table} ORDER BY a")),
        vec![1, 2],
        "a shared session's table"
    );

    // One physical table, not one per connection
    assert_eq!(
        session_tables(&ending).len(),
        1,
        "one table for the session"
    );

    // Closing them leaves it: the session is the caller's to drop
    drop(a);
    drop(b);
    assert_eq!(
        session_tables(&ending).len(),
        1,
        "a named session survives the connections that used it"
    );

    clear_session_tables(&ending);
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_named_session_is_dropped_by_the_caller() {
    // The other half of leaving a named session alone: what the caller is
    // expected to do instead has to actually clear the tables the driver
    // made, which is the only way they ever go.
    let suffix = unique_suffix();
    let session_id = format!("adbc-{suffix}");
    let table = format!("#dropped_{suffix}");
    let ending = format!("dropped_{suffix}");

    let mut connection = connect_to_session(&session_id);
    run(&mut connection, &format!("CREATE TABLE {table} (a INT)"));
    run(&mut connection, &format!("INSERT INTO {table} VALUES (9)"));
    drop(connection);

    assert_eq!(
        session_tables(&ending).len(),
        1,
        "closing leaves a named session's table"
    );

    assert!(
        drop_session(&session_id),
        "the caller drops its own session"
    );

    assert!(
        session_tables(&ending).is_empty(),
        "/drop/session clears what the driver made, found {:?}",
        session_tables(&ending)
    );

    // And the name is free again, rather than the table lingering invisibly
    let mut again = connect_to_session(&session_id);
    run(&mut again, &format!("CREATE TABLE {table} (a INT)"));
    assert_eq!(
        ints(&mut again, &format!("SELECT a FROM {table}")),
        Vec::<i32>::new(),
        "a fresh table under the same name"
    );
    drop(again);

    assert!(drop_session(&session_id), "drop the session again");
    clear_session_tables(&ending);
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_named_session_is_read_back() {
    let suffix = unique_suffix();
    let session_id = format!("adbc-{suffix}");

    let url = kinetica_adbc_tests::url();
    let mut driver = KineticaDriver::default();
    let database = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(url)),
            (
                OptionDatabase::Other(adbc_driver_kinetica::options::SESSION_ID.to_string()),
                OptionValue::String(session_id.clone()),
            ),
        ])
        .expect("database");

    use adbc_core::Optionable;
    assert_eq!(
        database
            .get_option_string(OptionDatabase::Other(
                adbc_driver_kinetica::options::SESSION_ID.to_string()
            ))
            .expect("the session id"),
        session_id
    );
}
