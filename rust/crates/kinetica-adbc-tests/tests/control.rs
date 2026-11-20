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

//! The statements the connection carries out itself: who it acts as, and
//! where its unqualified names are looked for.
//!
//! Each is checked by what the server then says, rather than by what the
//! driver recorded: the user by `CURRENT_USER()`, the schema by resolving a
//! bare table name. A driver that kept the state and never sent it would pass
//! the second kind of test and fail these.

use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::sync::{Connection, Database, Driver, Statement};
use adbc_core::Optionable;
use adbc_driver_kinetica::options::IMPERSONATE_USER;
use adbc_driver_kinetica::{KineticaConnection, KineticaDriver};
use arrow_array::{Array, StringArray};
use kinetica_adbc_tests::unique_suffix;

/// @return A connection to the instance under test
fn connect() -> KineticaConnection {
    connect_as(None)
}

/// @return A connection acting as the given user from the start
fn connect_as(user: Option<&str>) -> KineticaConnection {
    let kinetica_adbc_tests::Login {
        url,
        user: name,
        password,
    } = kinetica_adbc_tests::login();

    let mut options = vec![
        (OptionDatabase::Uri, OptionValue::String(url)),
        (OptionDatabase::Username, OptionValue::String(name)),
        (OptionDatabase::Password, OptionValue::String(password)),
    ];

    if let Some(user) = user {
        options.push((
            OptionDatabase::Other(IMPERSONATE_USER.into()),
            OptionValue::String(user.into()),
        ));
    }

    KineticaDriver::default()
        .new_database_with_opts(options)
        .expect("database")
        .new_connection()
        .expect("connection")
}

/// Runs a statement for its effect.
fn run(connection: &mut KineticaConnection, sql: &str) {
    try_run(connection, sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
}

/// Runs a statement that may be refused.
/// @return What it reported, or the refusal
fn try_run(connection: &mut KineticaConnection, sql: &str) -> Result<i64, String> {
    let mut statement = connection.new_statement().map_err(|e| e.to_string())?;

    statement.set_sql_query(sql).map_err(|e| e.to_string())?;
    statement
        .execute_update()
        .map(|rows| rows.unwrap_or(-1))
        .map_err(|e| e.to_string())
}

/// @return The one string the query answers with
fn one(connection: &mut KineticaConnection, sql: &str) -> String {
    let mut statement = connection.new_statement().expect("statement");

    statement.set_sql_query(sql).expect("set query");

    let reader = statement
        .execute()
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let mut found = String::new();

    for batch in reader {
        let batch = batch.expect("batch");

        for row in 0..batch.num_rows() {
            for column in batch.columns() {
                if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
                    found = values.value(row).to_string();
                }
            }
        }
    }

    found
}

/// @return Who the server says is asking
fn whoami(connection: &mut KineticaConnection) -> String {
    one(connection, "SELECT CURRENT_USER() AS u")
}

/// Makes a user to act as, and gives it back with the statement that drops it.
/// @return The user's name
fn make_user(connection: &mut KineticaConnection, suffix: &str) -> String {
    let user = format!("adbc_ctl_{suffix}");

    run(
        connection,
        &format!("CREATE USER {user} WITH PASSWORD 'Probe1!'"),
    );

    user
}

/// Dropping happens on a connection of its own, since the one under test may
/// be left acting as somebody who cannot drop anything.
fn drop_user(user: &str) {
    let mut clean = connect();

    let _ = try_run(&mut clean, &format!("DROP USER {user}"));
}

/// Stepping into a user and back out again, which is what the server sees
/// rather than what the driver remembers.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_user_is_stepped_into_and_back_out_of() {
    let mut connection = connect();
    let user = make_user(&mut connection, &unique_suffix());
    let me = whoami(&mut connection);

    assert_ne!(me, user, "the test would prove nothing otherwise");

    run(&mut connection, &format!("EXECUTE AS USER {user}"));
    assert_eq!(whoami(&mut connection), user, "the server sees the change");

    run(&mut connection, "REVERT");
    assert_eq!(whoami(&mut connection), me);

    // SET USER steps in the same way, and with no name steps back out.
    run(&mut connection, &format!("SET USER {user}"));
    assert_eq!(whoami(&mut connection), user);

    run(&mut connection, "SET USER");
    assert_eq!(whoami(&mut connection), me);

    drop_user(&user);
}

/// The stack nests: two steps in need two steps back.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn stepping_in_twice_needs_two_steps_back() {
    let mut connection = connect();
    let suffix = unique_suffix();
    let first = make_user(&mut connection, &format!("{suffix}a"));
    let second = make_user(&mut connection, &format!("{suffix}b"));
    let me = whoami(&mut connection);

    run(&mut connection, &format!("EXECUTE AS USER {first}"));
    run(&mut connection, &format!("EXECUTE AS USER {second}"));
    assert_eq!(whoami(&mut connection), second);

    run(&mut connection, "REVERT");
    assert_eq!(whoami(&mut connection), first, "back one, not all the way");

    run(&mut connection, "REVERT");
    assert_eq!(whoami(&mut connection), me);

    drop_user(&first);
    drop_user(&second);
}

/// WITH NO REVERT gives up the right, and the refusal says so rather than
/// quietly leaving the connection where it was.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn with_no_revert_gives_up_the_right_to_come_back() {
    let mut connection = connect();
    let user = make_user(&mut connection, &unique_suffix());

    run(
        &mut connection,
        &format!("EXECUTE AS USER {user} WITH NO REVERT"),
    );
    assert_eq!(whoami(&mut connection), user);

    let refused = try_run(&mut connection, "REVERT").expect_err("REVERT should be refused");

    assert!(refused.contains("NO REVERT"), "{refused}");
    assert_eq!(whoami(&mut connection), user, "still acting as them");

    drop_user(&user);
}

/// A user named before anything runs is in force from the first statement,
/// and is where REVERT comes back to.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_connection_can_start_as_another_user() {
    let mut setup = connect();
    let suffix = unique_suffix();
    let base = make_user(&mut setup, &format!("{suffix}base"));
    let other = make_user(&mut setup, &format!("{suffix}other"));

    let mut connection = connect_as(Some(&base));

    assert_eq!(whoami(&mut connection), base, "from the first statement");

    run(&mut connection, &format!("EXECUTE AS USER {other}"));
    assert_eq!(whoami(&mut connection), other);

    run(&mut connection, "REVERT");
    assert_eq!(
        whoami(&mut connection),
        base,
        "REVERT comes back to the user the connection started as"
    );

    drop_user(&base);
    drop_user(&other);
}

/// SET SCHEMA changes where a bare table name is looked for, which is what
/// the server does with it rather than what the driver records.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_schema_statement_changes_where_names_are_looked_for() {
    let mut connection = connect();
    let schema = format!("adbc_ctl_s_{}", unique_suffix());
    let table = "in_the_schema";

    run(&mut connection, &format!("CREATE SCHEMA {schema}"));
    run(
        &mut connection,
        &format!("CREATE TABLE {schema}.{table} (id INT)"),
    );
    run(
        &mut connection,
        &format!("INSERT INTO {schema}.{table} VALUES (7)"),
    );

    // Unqualified, it is not there yet.
    assert!(
        try_run(&mut connection, &format!("SELECT * FROM {table}")).is_err(),
        "the bare name should not resolve before the schema is set"
    );

    run(&mut connection, &format!("SET SCHEMA {schema}"));

    assert_eq!(
        connection
            .get_option_string(OptionConnection::CurrentSchema)
            .expect("the schema"),
        schema
    );
    assert_eq!(
        one(
            &mut connection,
            &format!("SELECT CAST(id AS VARCHAR) AS v FROM {table}")
        ),
        "7",
        "the bare name resolves in the schema that was set"
    );

    // Clearing it puts the name back out of reach.
    run(&mut connection, "SET SCHEMA");
    assert!(
        try_run(&mut connection, &format!("SELECT * FROM {table}")).is_err(),
        "cleared, so the bare name should not resolve again"
    );

    run(&mut connection, &format!("DROP TABLE {schema}.{table}"));
    run(&mut connection, &format!("DROP SCHEMA {schema}"));
}

/// SET CATALOG is remembered the way the option is, and cleared the same way.
/// Only a federated deployment could show it resolving anything, so what is
/// checked here is that it reaches the connection and is given back.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_catalog_statement_is_remembered_and_cleared() {
    let mut connection = connect();

    assert!(
        connection
            .get_option_string(OptionConnection::CurrentCatalog)
            .is_err(),
        "nobody has named one yet"
    );

    run(&mut connection, "SET CATALOG remote_one");

    assert_eq!(
        connection
            .get_option_string(OptionConnection::CurrentCatalog)
            .expect("the catalog"),
        "remote_one"
    );

    run(&mut connection, "SET CATALOG");

    assert!(
        connection
            .get_option_string(OptionConnection::CurrentCatalog)
            .is_err(),
        "cleared again"
    );
}
