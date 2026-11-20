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

//! How an ingest behaves when it is told to: how many rows go at a time,
//! whether they go to the workers, what happens to a row that will not fit,
//! and how many rows a read hands back.

use adbc_core::error::Status;
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::sync::{Connection, Database, Driver, Statement};
use adbc_core::Optionable;
use adbc_driver_kinetica::{options, KineticaConnection, KineticaDriver};
use arrow_array::{RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema};
use kinetica_adbc_tests::unique_suffix;
use std::sync::Arc;

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

/// @return The option, named the way a database option is named
fn option(name: &str, value: &str) -> (OptionDatabase, OptionValue) {
    (
        OptionDatabase::Other(name.to_string()),
        OptionValue::String(value.to_string()),
    )
}

/// Runs a statement for its effect.
fn run(connection: &mut KineticaConnection, sql: &str) {
    let mut statement = connection.new_statement().expect("statement");
    statement.set_sql_query(sql).expect("set query");
    statement
        .execute_update()
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

/// @return How many rows the query answers with, read to the end
fn rows(connection: &mut KineticaConnection, sql: &str) -> usize {
    let mut statement = connection.new_statement().expect("statement");
    statement.set_sql_query(sql).expect("set query");

    statement
        .execute()
        .expect("execute")
        .map(|batch| batch.expect("batch").num_rows())
        .sum()
}

/// @return A batch of strings, for the column named
fn strings(column: &str, values: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(column, DataType::Utf8, true)]));

    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values.to_vec()))])
        .expect("a batch")
}

/// Reads one batch as a stream, which is what ingest takes.
struct One(Option<RecordBatch>, Arc<Schema>);

impl Iterator for One {
    type Item = Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.take().map(Ok)
    }
}

impl RecordBatchReader for One {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.1)
    }
}

/// Loads the batch into the table through bulk ingest.
/// @return What the ingest reported, so a refusal can be asserted on
fn load(
    connection: &mut KineticaConnection,
    table: &str,
    batch: RecordBatch,
) -> adbc_core::error::Result<Option<i64>> {
    let schema = batch.schema();
    let mut statement = connection.new_statement().expect("statement");

    statement.set_option(
        adbc_core::options::OptionStatement::TargetTable,
        OptionValue::String(table.to_string()),
    )?;
    statement.bind_stream(Box::new(One(Some(batch), schema)))?;
    statement.execute_update()
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_read_stops_at_the_cap_the_caller_set() {
    let mut connection = connect_with([option(options::MAX_ROWS, "3")]);

    // The server holds far more than three of these, so a cap that is not
    // applied would show up at once.
    assert_eq!(
        rows(
            &mut connection,
            "SELECT table_name FROM information_schema.tables"
        ),
        3
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_cap_on_the_statement_beats_the_one_on_the_connection() {
    let mut connection = connect_with([option(options::MAX_ROWS, "3")]);

    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_option(
            adbc_core::options::OptionStatement::Other(options::MAX_ROWS.to_string()),
            OptionValue::String("1".to_string()),
        )
        .expect("cap the statement");
    statement
        .set_sql_query("SELECT table_name FROM information_schema.tables")
        .expect("set query");

    let read: usize = statement
        .execute()
        .expect("execute")
        .map(|batch| batch.expect("batch").num_rows())
        .sum();

    assert_eq!(read, 1);
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn no_cap_hands_back_everything() {
    let mut capped = connect_with([option(options::MAX_ROWS, "2")]);
    let mut uncapped = connect();

    let query = "SELECT table_name FROM information_schema.tables";

    assert_eq!(rows(&mut capped, query), 2);
    assert!(
        rows(&mut uncapped, query) > 2,
        "the same query uncapped should hand back more than the cap did"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_string_too_long_is_refused_unless_the_caller_said_to_cut_it() {
    let suffix = unique_suffix();
    let table = format!("adbc_trunc_{suffix}");

    let mut setup = connect();
    run(
        &mut setup,
        &format!("CREATE TABLE ki_home.{table} (word VARCHAR(4))"),
    );

    // Five characters into a column that holds four.
    let too_long = strings("word", &["abcde"]);

    let mut strict = connect();
    let refused = load(&mut strict, &format!("ki_home.{table}"), too_long.clone());

    let mut lenient = connect_with([option(options::TRUNCATE_STRINGS, "true")]);
    let taken = load(&mut lenient, &format!("ki_home.{table}"), too_long);

    let held = rows(&mut setup, &format!("SELECT word FROM ki_home.{table}"));
    run(&mut setup, &format!("DROP TABLE IF EXISTS ki_home.{table}"));

    assert!(
        refused.is_err(),
        "a string too long should be refused by default, got {refused:?}"
    );
    assert!(
        taken.is_ok(),
        "and taken when the caller asked for it to be cut: {taken:?}"
    );
    assert_eq!(held, 1, "exactly the one row should have landed");
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn rows_still_arrive_with_the_workers_left_out() {
    let suffix = unique_suffix();
    let table = format!("adbc_head_{suffix}");

    let mut setup = connect();
    run(
        &mut setup,
        &format!("CREATE TABLE ki_home.{table} (word VARCHAR(16))"),
    );

    let mut connection = connect_with([
        option(options::MULTIHEAD_INSERT, "false"),
        option(options::ROWS_PER_INSERTION, "2"),
    ]);

    let loaded = load(
        &mut connection,
        &format!("ki_home.{table}"),
        strings("word", &["one", "two", "three"]),
    );

    let held = rows(&mut setup, &format!("SELECT word FROM ki_home.{table}"));
    run(&mut setup, &format!("DROP TABLE IF EXISTS ki_home.{table}"));

    assert!(loaded.is_ok(), "the head rank should take them: {loaded:?}");
    assert_eq!(held, 3, "all three rows should have landed");
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn changing_how_rows_load_takes_effect_on_the_next_ingest() {
    // The inserter is cached per table, so a caller that changes its mind
    // after loading once has to get an inserter built again rather than the
    // held one going on as it was.
    let suffix = unique_suffix();
    let table = format!("adbc_rebuilt_{suffix}");

    let mut setup = connect();
    run(
        &mut setup,
        &format!("CREATE TABLE ki_home.{table} (word VARCHAR(4))"),
    );

    let mut connection = connect();
    let qualified = format!("ki_home.{table}");

    let refused = load(&mut connection, &qualified, strings("word", &["abcde"]));

    connection
        .set_option(
            OptionConnection::Other(options::TRUNCATE_STRINGS.to_string()),
            OptionValue::String("true".to_string()),
        )
        .expect("ask for strings to be cut");

    let taken = load(&mut connection, &qualified, strings("word", &["abcde"]));

    let held = rows(&mut setup, &format!("SELECT word FROM {qualified}"));
    run(&mut setup, &format!("DROP TABLE IF EXISTS {qualified}"));

    assert!(refused.is_err(), "the first load should be refused");
    assert!(
        taken.is_ok(),
        "and the second, after changing the option, taken: {taken:?}"
    );
    assert_eq!(held, 1);
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_loading_option_the_driver_cannot_use_is_refused_where_it_is_said() {
    let url = kinetica_adbc_tests::url();

    let error = KineticaDriver::default()
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(url)),
            option(options::ERROR_MODE, "ignore"),
        ])
        .expect_err("an error mode outside the three should be refused");

    assert_eq!(error.status, Status::InvalidArguments);
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn read_only_is_recorded_and_reported_back() {
    let mut connection = connect();

    assert_eq!(
        connection
            .get_option_string(OptionConnection::ReadOnly)
            .expect("read back"),
        "false"
    );

    connection
        .set_option(
            OptionConnection::ReadOnly,
            OptionValue::String("true".to_string()),
        )
        .expect("a read-only hint should be accepted");

    assert_eq!(
        connection
            .get_option_string(OptionConnection::ReadOnly)
            .expect("read back"),
        "true"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn the_log_is_written_where_the_caller_asked_for_it() {
    let path = std::env::temp_dir().join(format!("adbc-loading-{}.log", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let mut connection = connect_with([
        option(options::LOG_PATH, path.to_str().expect("a path")),
        option(options::LOG_LEVEL, "debug"),
    ]);

    // Something that certainly talks to the server.
    let _ = rows(&mut connection, "SELECT 1 AS value");

    let written = std::fs::read_to_string(&path).unwrap_or_default();

    // Put back, so the rest of the suite is not logging to a file.
    let _ = connect_with([
        option(options::LOG_LEVEL, "off"),
        option(options::LOG_PATH, ""),
    ]);
    let _ = std::fs::remove_file(&path);

    assert!(
        !written.is_empty(),
        "asking for a debug log should have put something in {path:?}"
    );
}
