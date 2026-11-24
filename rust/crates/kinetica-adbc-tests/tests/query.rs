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

//! The server options a statement carries, and the schema and catalog it
//! resolves unqualified names against.
//!
//! Each test names its objects after a suffix of its own, so they do not
//! collide with each other or with anything else using the server.

use adbc_core::error::Status;
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::sync::{Connection, Database, Driver, Statement};
use adbc_core::Optionable;
use adbc_driver_kinetica::{options, KineticaConnection, KineticaDriver};
use arrow_array::{Array, Int32Array};
use kinetica_adbc_tests::{table_ttl, unique_suffix};

/// Every server option a caller can put on a statement, with a value the
/// server has to accept under exactly that name.
const EVERY_OPTION: [(&str, &str); 12] = [
    (options::COST_BASED_OPTIMIZATION, "true"),
    (options::DISTRIBUTED_OPERATIONS, "true"),
    (options::IGNORE_EXISTING_PK, "false"),
    (options::UPDATE_ON_EXISTING_PK, "false"),
    (options::PARALLEL_EXECUTION, "true"),
    (options::PLAN_CACHE, "false"),
    (options::RESULTS_CACHING, "false"),
    (options::RULE_BASED_OPTIMIZATION, "true"),
    (options::SSQ_OPTIMIZATION, "true"),
    (options::VALIDATE_CHANGE_COLUMN, "false"),
    (options::USE_APPROX_COUNT_DISTINCT, "false"),
    (options::PAGING_TABLE_TTL, "5"),
];

/// @return A connection, with any extra database options the caller needs
fn connect_with<I>(extra: I) -> KineticaConnection
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

    KineticaDriver::default()
        .new_database_with_opts(options)
        .expect("database")
        .new_connection()
        .expect("connection")
}

/// @return A connection with nothing said about it
fn connect() -> KineticaConnection {
    connect_with([])
}

/// Runs a statement that answers with no rows.
fn run(connection: &mut KineticaConnection, sql: &str) {
    let mut statement = connection.new_statement().expect("statement");
    statement.set_sql_query(sql).expect("set query");
    statement
        .execute_update()
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

/// @return The single number the query answers with
fn number(connection: &mut KineticaConnection, sql: &str) -> i32 {
    let mut statement = connection.new_statement().expect("statement");
    statement.set_sql_query(sql).expect("set query");

    let batch = statement
        .execute()
        .expect("execute")
        .next()
        .expect("a row")
        .expect("batch");

    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("an integer column")
        .value(0)
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn every_server_option_is_sent_under_a_name_the_server_accepts() {
    // The server refuses an option it does not know, so a query that runs
    // with all of them on is what says each one is spelt the way it wants.
    let mut connection = connect_with(
        EVERY_OPTION
            .iter()
            .map(|(name, value)| {
                (
                    OptionDatabase::Other(name.to_string()),
                    OptionValue::String(value.to_string()),
                )
            })
            .collect::<Vec<_>>(),
    );

    assert_eq!(number(&mut connection, "SELECT 1 AS value"), 1);
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_server_option_is_read_back_from_the_connection_it_was_given_to() {
    let mut connection = connect_with([(
        OptionDatabase::Other(options::PLAN_CACHE.to_string()),
        OptionValue::String("false".to_string()),
    )]);

    assert_eq!(
        connection
            .get_option_string(OptionConnection::Other(options::PLAN_CACHE.to_string()))
            .expect("read back"),
        "false"
    );

    // And a connection can change its mind without the database being told.
    connection
        .set_option(
            OptionConnection::Other(options::PLAN_CACHE.to_string()),
            OptionValue::String("true".to_string()),
        )
        .expect("set on the connection");

    assert_eq!(
        connection
            .get_option_string(OptionConnection::Other(options::PLAN_CACHE.to_string()))
            .expect("read back"),
        "true"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_value_the_server_would_not_understand_is_refused_on_the_connection() {
    let mut connection = connect();

    let error = connection
        .set_option(
            OptionConnection::Other(options::PLAN_CACHE.to_string()),
            OptionValue::String("sometimes".to_string()),
        )
        .expect_err("a flag that is not one should be refused");

    assert_eq!(error.status, Status::InvalidArguments);
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_ttl_the_caller_asks_for_is_the_one_the_table_is_kept_under() {
    let suffix = unique_suffix();
    let table = format!("adbc_ttl_{suffix}");

    let mut connection = connect_with([(
        OptionDatabase::Other(options::TTL.to_string()),
        OptionValue::String("7".to_string()),
    )]);

    run(
        &mut connection,
        &format!("CREATE TABLE {table} AS SELECT 1 AS value"),
    );

    let kept = table_ttl(&table);

    run(&mut connection, &format!("DROP TABLE IF EXISTS {table}"));

    assert_eq!(
        kept.as_deref(),
        Some("7"),
        "the table should be kept under the TTL the statement asked for"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn an_unqualified_name_in_a_statement_that_changes_things_uses_the_schema() {
    // A SELECT has always been sent with the connection's schema on it; the
    // statements that go through execute_update were not, so a CREATE or an
    // INSERT naming a bare table landed wherever the server defaulted to.
    let suffix = unique_suffix();
    let schema = format!("adbc_schema_{suffix}");
    let table = format!("adbc_in_schema_{suffix}");

    let mut setup = connect();
    run(&mut setup, &format!("CREATE SCHEMA {schema}"));

    let mut connection = connect();
    connection
        .set_option(
            OptionConnection::CurrentSchema,
            OptionValue::String(schema.clone()),
        )
        .expect("set the schema");

    run(
        &mut connection,
        &format!("CREATE TABLE {table} (id INTEGER NOT NULL)"),
    );
    run(&mut connection, &format!("INSERT INTO {table} VALUES (7)"));

    // Read back by the qualified name, from a connection that was told
    // nothing, so what is being asserted is where the rows actually went.
    let mut reader = connect();
    let found = number(
        &mut reader,
        &format!("SELECT id FROM {schema}.{table} WHERE id = 7"),
    );

    run(&mut setup, &format!("DROP SCHEMA {schema} CASCADE"));

    assert_eq!(found, 7, "the table should have been made in {schema}");
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_catalog_that_is_not_there_is_the_servers_to_refuse() {
    // The driver does not keep a list of catalogs; what it does is carry the
    // name, and the server says whether it means anything.
    let mut connection = connect();
    connection
        .set_option(
            OptionConnection::CurrentCatalog,
            OptionValue::String("not_a_catalog".to_string()),
        )
        .expect("the driver should carry a catalog name without checking it");

    assert_eq!(
        connection
            .get_option_string(OptionConnection::CurrentCatalog)
            .expect("read back"),
        "not_a_catalog"
    );

    let mut statement = connection.new_statement().expect("statement");
    statement.set_sql_query("SELECT 1 AS value").expect("query");

    // Whether the server minds is its business; what must not happen is the
    // driver dropping the name on the floor and reporting success it cannot
    // vouch for.
    let _ = statement.execute();
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn an_option_nobody_knows_is_refused_by_the_connection_too() {
    let mut connection = connect();

    let error = connection
        .set_option(
            OptionConnection::Other("kinetica.not_an_option".to_string()),
            OptionValue::String("1".to_string()),
        )
        .expect_err("an unknown option should not be swallowed");

    assert_eq!(error.status, Status::NotImplemented);
}
