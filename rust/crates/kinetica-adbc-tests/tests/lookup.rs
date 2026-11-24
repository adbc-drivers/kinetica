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

//! Reading a keyed row from the rank that holds it.
//!
//! Two things are asserted of every query here: the rows, which must be the
//! rows the SQL engine would have given, and which way it was routed, which
//! the driver reports under `kinetica.key_lookup`. Rows alone would pass
//! whether or not the lookup was ever used; routing alone would not notice it
//! answering with the wrong rows.

use adbc_core::options::{OptionConnection, OptionDatabase, OptionStatement, OptionValue};
use adbc_core::sync::{Connection, Database, Driver, Statement};
use adbc_core::Optionable;
use adbc_driver_kinetica::options::{KEY_LOOKUP, MAX_ROWS, REPLICATION, USE_KEY_LOOKUP};
use adbc_driver_kinetica::{KineticaConnection, KineticaDriver};
use arrow_array::{Array, Int32Array, Int64Array, RecordBatchReader, StringArray};
use kinetica_adbc_tests::unique_suffix;

/// A connection with key lookup asked for, which is how every test here but
/// one wants it: the driver leaves it off until it is.
/// @return A connection to the instance under test
fn connect() -> KineticaConnection {
    connect_with(&[(USE_KEY_LOOKUP, "true")])
}

/// @return A connection to the instance under test, with the given options
fn connect_with(options: &[(&str, &str)]) -> KineticaConnection {
    let kinetica_adbc_tests::Login {
        url,
        user,
        password,
    } = kinetica_adbc_tests::login();

    let mut said = vec![
        (OptionDatabase::Uri, OptionValue::String(url)),
        (OptionDatabase::Username, OptionValue::String(user)),
        (OptionDatabase::Password, OptionValue::String(password)),
    ];

    for (name, value) in options {
        said.push((
            OptionDatabase::Other((*name).into()),
            OptionValue::String((*value).into()),
        ));
    }

    KineticaDriver::default()
        .new_database_with_opts(said)
        .expect("database")
        .new_connection()
        .expect("connection")
}

/// Runs a statement for its effect.
fn run(connection: &mut KineticaConnection, sql: &str) {
    let mut statement = connection.new_statement().expect("statement");
    statement.set_sql_query(sql).expect("set query");
    statement
        .execute_update()
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

/// What a read came back with, and how it was routed.
struct Read {
    /// Every row as its cells joined by '|', sorted, since neither route
    /// promises an order without an ORDER BY.
    rows: Vec<String>,
    columns: Vec<String>,
    /// Whether the rank holding the row answered, rather than the SQL engine.
    by_key: bool,
}

/// Reads a query through the driver.
/// @return Its rows, its columns and which way it was routed
fn read(connection: &mut KineticaConnection, sql: &str) -> Read {
    read_capped(connection, sql, None)
}

/// Reads a query, optionally capping how many rows it may hand back.
/// @return Its rows, its columns and which way it was routed
fn read_capped(connection: &mut KineticaConnection, sql: &str, cap: Option<i64>) -> Read {
    let mut statement = connection.new_statement().expect("statement");

    if let Some(rows) = cap {
        statement
            .set_option(
                OptionStatement::Other(MAX_ROWS.into()),
                OptionValue::Int(rows),
            )
            .expect("set max rows");
    }

    statement.set_sql_query(sql).expect("set query");

    let reader = statement
        .execute()
        .unwrap_or_else(|error| panic!("{sql}: {error}"));

    let columns = reader
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();

    let mut rows = Vec::new();

    for batch in reader {
        let batch = batch.expect("batch");

        for row in 0..batch.num_rows() {
            let cells: Vec<String> = batch.columns().iter().map(|c| cell(c, row)).collect();

            rows.push(cells.join("|"));
        }
    }

    rows.sort();

    let by_key = statement
        .get_option_int(OptionStatement::Other(KEY_LOOKUP.into()))
        .expect("the driver reports how it routed the read")
        == 1;

    Read {
        rows,
        columns,
        by_key,
    }
}

/// @return One cell as text, for the few types these tables use
fn cell(column: &std::sync::Arc<dyn Array>, row: usize) -> String {
    if column.is_null(row) {
        return "null".to_string();
    }

    if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
        return values.value(row).to_string();
    }

    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        return values.value(row).to_string();
    }

    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return values.value(row).to_string();
    }

    panic!("a test table should hold no {}", column.data_type());
}

/// A statement pinning the whole shard key is answered by the rank that holds
/// the row, and the result is the one the SQL engine would have given.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_pinned_key_is_read_from_the_rank_that_holds_it() {
    let mut connection = connect();
    let table = format!("adbc_lookup_{}", unique_suffix());

    run(
        &mut connection,
        &format!("CREATE TABLE {table} (id INT, grp INT, name VARCHAR(32), SHARD KEY (grp))"),
    );

    for (id, grp, name) in [(1, 10, "a"), (2, 10, "b"), (3, 20, "c")] {
        run(
            &mut connection,
            &format!("INSERT INTO {table} VALUES ({id}, {grp}, '{name}')"),
        );
    }

    let found = read(
        &mut connection,
        &format!("SELECT * FROM {table} WHERE grp = 10"),
    );

    assert!(
        found.by_key,
        "the rank holding the key should have answered"
    );
    assert_eq!(found.columns, ["id", "grp", "name"]);
    assert_eq!(found.rows, ["1|10|a", "2|10|b"]);

    // The columns come back in the order the statement asked for them.
    let projected = read(
        &mut connection,
        &format!("SELECT name, id FROM {table} WHERE grp = 20"),
    );

    assert!(projected.by_key);
    assert_eq!(projected.columns, ["name", "id"]);
    assert_eq!(projected.rows, ["c|3"]);

    // A statement may spell a column in any case.
    let shouted = read(
        &mut connection,
        &format!("SELECT * FROM {table} WHERE GRP = 20"),
    );

    assert!(shouted.by_key);
    assert_eq!(shouted.rows, ["3|20|c"]);

    run(&mut connection, &format!("DROP TABLE {table}"));
}

/// What the key does not account for is filtered by the worker, over its own
/// data, and the rows are the same ones.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_leftover_comparison_is_filtered_on_the_worker() {
    let mut connection = connect();
    let table = format!("adbc_lookup_rest_{}", unique_suffix());

    run(
        &mut connection,
        &format!("CREATE TABLE {table} (id INT, grp INT, name VARCHAR(32), SHARD KEY (grp))"),
    );

    for (id, grp, name) in [(1, 5, "a"), (2, 5, "b"), (3, 5, "c"), (4, 6, "d")] {
        run(
            &mut connection,
            &format!("INSERT INTO {table} VALUES ({id}, {grp}, '{name}')"),
        );
    }

    let found = read(
        &mut connection,
        &format!("SELECT * FROM {table} WHERE grp = 5 AND id > 1"),
    );

    assert!(found.by_key);
    assert_eq!(found.rows, ["2|5|b", "3|5|c"]);

    let on_text = read(
        &mut connection,
        &format!("SELECT id FROM {table} WHERE grp = 5 AND name = 'c'"),
    );

    assert!(on_text.by_key);
    assert_eq!(on_text.rows, ["3"]);

    run(&mut connection, &format!("DROP TABLE {table}"));
}

/// A key of several columns is matched whatever order the statement names
/// them in, since the key's own order is the table's.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_composite_key_is_matched_in_any_order() {
    let mut connection = connect();
    let table = format!("adbc_lookup_pair_{}", unique_suffix());

    run(
        &mut connection,
        &format!("CREATE TABLE {table} (a INT, b VARCHAR(16), v INT, SHARD KEY (a, b))"),
    );

    for (a, b, v) in [(1, "x", 100), (1, "y", 200), (2, "x", 300)] {
        run(
            &mut connection,
            &format!("INSERT INTO {table} VALUES ({a}, '{b}', {v})"),
        );
    }

    let in_order = read(
        &mut connection,
        &format!("SELECT * FROM {table} WHERE a = 1 AND b = 'y'"),
    );

    assert!(in_order.by_key);
    assert_eq!(in_order.rows, ["1|y|200"]);

    let reversed = read(
        &mut connection,
        &format!("SELECT * FROM {table} WHERE b = 'x' AND a = 2"),
    );

    assert!(reversed.by_key);
    assert_eq!(reversed.rows, ["2|x|300"]);

    // Half a key does not say which rank holds the row.
    let partial = read(
        &mut connection,
        &format!("SELECT * FROM {table} WHERE a = 1"),
    );

    assert!(!partial.by_key, "half a key is not a key");
    assert_eq!(partial.rows, ["1|x|100", "1|y|200"]);

    run(&mut connection, &format!("DROP TABLE {table}"));
}

/// With no shard key the server shards on the primary key, and that is the
/// key a lookup uses. A replicated table is held whole by every worker, so
/// any of them can answer.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_table_keyed_by_its_primary_key_is_looked_up() {
    let mut connection = connect();
    let sharded = format!("adbc_lookup_pk_{}", unique_suffix());
    let replicated = format!("adbc_lookup_rep_{}", unique_suffix());

    run(
        &mut connection,
        &format!("CREATE TABLE {sharded} (k VARCHAR(16), v INT, PRIMARY KEY (k))"),
    );
    run(
        &mut connection,
        &format!("INSERT INTO {sharded} VALUES ('one', 1)"),
    );
    run(
        &mut connection,
        &format!("INSERT INTO {sharded} VALUES ('two', 2)"),
    );

    let found = read(
        &mut connection,
        &format!("SELECT v FROM {sharded} WHERE k = 'two'"),
    );

    assert!(found.by_key);
    assert_eq!(found.rows, ["2"]);

    run(
        &mut connection,
        &format!("CREATE REPLICATED TABLE {replicated} (k INT, v INT, PRIMARY KEY (k))"),
    );
    run(
        &mut connection,
        &format!("INSERT INTO {replicated} VALUES (2, 20)"),
    );

    let copied = read(
        &mut connection,
        &format!("SELECT * FROM {replicated} WHERE k = 2"),
    );

    assert!(copied.by_key);
    assert_eq!(copied.rows, ["2|20"]);

    run(&mut connection, &format!("DROP TABLE {sharded}"));
    run(&mut connection, &format!("DROP TABLE {replicated}"));
}

/// Everything the rank holding one row cannot answer by itself goes to the
/// server, and still comes back right.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn anything_else_goes_to_the_server() {
    let mut connection = connect();
    let keyed = format!("adbc_lookup_other_{}", unique_suffix());
    let unkeyed = format!("adbc_lookup_none_{}", unique_suffix());

    run(
        &mut connection,
        &format!("CREATE TABLE {keyed} (id INT, grp INT, SHARD KEY (grp))"),
    );
    run(
        &mut connection,
        &format!("INSERT INTO {keyed} VALUES (1, 9)"),
    );
    run(
        &mut connection,
        &format!("INSERT INTO {keyed} VALUES (2, 9)"),
    );

    run(
        &mut connection,
        &format!("CREATE TABLE {unkeyed} (a INT, b INT)"),
    );
    run(
        &mut connection,
        &format!("INSERT INTO {unkeyed} VALUES (1, 2)"),
    );

    // An ORDER BY, a cap, a disjunction and a column that is not the key are
    // all things the SQL engine does and a single rank does not.
    for (sql, rows) in [
        (
            format!("SELECT * FROM {keyed} WHERE grp = 9 ORDER BY id"),
            vec!["1|9", "2|9"],
        ),
        (format!("SELECT * FROM {keyed} WHERE id = 1"), vec!["1|9"]),
        (
            format!("SELECT * FROM {keyed} WHERE grp = 9 OR grp = 8"),
            vec!["1|9", "2|9"],
        ),
        (format!("SELECT * FROM {keyed}"), vec!["1|9", "2|9"]),
        (format!("SELECT * FROM {unkeyed} WHERE a = 1"), vec!["1|2"]),
    ] {
        let found = read(&mut connection, &sql);

        assert!(!found.by_key, "{sql} should go to the server");
        assert_eq!(found.rows, rows, "{sql}");
    }

    run(&mut connection, &format!("DROP TABLE {keyed}"));
    run(&mut connection, &format!("DROP TABLE {unkeyed}"));
}

/// A key may hold more rows than one response can carry, so a lookup pages
/// the way a read through the head rank does, and a cap still caps it.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_result_larger_than_one_response_is_paged_and_capped() {
    let mut connection = connect();
    let table = format!("adbc_lookup_many_{}", unique_suffix());

    run(
        &mut connection,
        &format!("CREATE TABLE {table} (g INT, i INT, SHARD KEY (g))"),
    );
    run(
        &mut connection,
        &format!(
            "INSERT INTO {table} SELECT 7, generate_series FROM TABLE(GENERATE_SERIES(1, 50000))"
        ),
    );

    let whole = read(
        &mut connection,
        &format!("SELECT * FROM {table} WHERE g = 7"),
    );

    assert!(whole.by_key);
    assert_eq!(
        whole.rows.len(),
        50_000,
        "every row, over several responses"
    );

    let capped = read_capped(
        &mut connection,
        &format!("SELECT * FROM {table} WHERE g = 7"),
        Some(10),
    );

    assert!(capped.by_key);
    assert_eq!(capped.rows.len(), 10);

    run(&mut connection, &format!("DROP TABLE {table}"));
}

/// What a connection has worked out about a table is only true while the
/// table is. Reading it with the key it used to have would route to the rank
/// that used to hold the row.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_table_rebuilt_with_another_key_is_not_read_with_the_old_one() {
    let mut connection = connect();
    let table = format!("adbc_lookup_rebuilt_{}", unique_suffix());

    run(
        &mut connection,
        &format!("CREATE TABLE {table} (a INT, b INT, SHARD KEY (a))"),
    );
    run(
        &mut connection,
        &format!("INSERT INTO {table} VALUES (1, 1)"),
    );

    let first = read(
        &mut connection,
        &format!("SELECT * FROM {table} WHERE a = 1"),
    );

    assert!(first.by_key, "keyed on a, so a lookup");
    assert_eq!(first.rows, ["1|1"]);

    run(&mut connection, &format!("DROP TABLE {table}"));
    run(
        &mut connection,
        &format!("CREATE TABLE {table} (a INT, b INT, SHARD KEY (b))"),
    );

    for (a, b) in [(1, 7), (2, 8), (3, 7)] {
        run(
            &mut connection,
            &format!("INSERT INTO {table} VALUES ({a}, {b})"),
        );
    }

    let rekeyed = read(
        &mut connection,
        &format!("SELECT * FROM {table} WHERE b = 7"),
    );

    assert!(rekeyed.by_key, "keyed on b now");
    assert_eq!(rekeyed.rows, ["1|7", "3|7"]);

    // `a` is no longer the key, so this is no longer a lookup.
    let old_key = read(
        &mut connection,
        &format!("SELECT * FROM {table} WHERE a = 1"),
    );

    assert!(!old_key.by_key);
    assert_eq!(old_key.rows, ["1|7"]);

    run(&mut connection, &format!("DROP TABLE {table}"));
}

/// The lookup is off until a caller asks for it, with
/// `kinetica.query.use_key_lookup`, and either route answers the same.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_lookup_is_off_until_it_is_asked_for() {
    let mut connection = connect_with(&[]);
    let table = format!("adbc_lookup_off_{}", unique_suffix());

    run(
        &mut connection,
        &format!("CREATE TABLE {table} (id INT, grp INT, SHARD KEY (grp))"),
    );
    run(
        &mut connection,
        &format!("INSERT INTO {table} VALUES (1, 4)"),
    );
    run(
        &mut connection,
        &format!("INSERT INTO {table} VALUES (2, 4)"),
    );

    let query = format!("SELECT * FROM {table} WHERE grp = 4");
    let off = read(&mut connection, &query);

    assert!(!off.by_key, "off until it is asked for");
    assert_eq!(off.rows, ["1|4", "2|4"]);

    // On, on the connection.
    connection
        .set_option(
            OptionConnection::Other(USE_KEY_LOOKUP.into()),
            OptionValue::String("true".into()),
        )
        .expect("turn it on");

    let on = read(&mut connection, &query);

    assert!(on.by_key, "the connection asked for it");
    assert_eq!(on.rows, off.rows, "the same rows, by the other route");

    // A statement's own answer beats the connection's, either way round.
    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_option(
            OptionStatement::Other(USE_KEY_LOOKUP.into()),
            OptionValue::String("false".into()),
        )
        .expect("turn it off here");
    statement.set_sql_query(&query).expect("set query");
    let reader = statement.execute().expect("read");
    let rows: usize = reader.map(|batch| batch.expect("batch").num_rows()).sum();

    assert_eq!(rows, 2);
    assert_eq!(
        statement
            .get_option_int(OptionStatement::Other(KEY_LOOKUP.into()))
            .expect("routing"),
        0,
        "the statement overrode the connection"
    );

    run(&mut connection, &format!("DROP TABLE {table}"));
}

/// A replication mode is sent with every request, so a connection that names
/// one goes on working rather than being refused at connect time.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_connection_can_name_a_replication_mode() {
    let kinetica_adbc_tests::Login {
        url,
        user,
        password,
    } = kinetica_adbc_tests::login();

    for mode in ["sync", "async", "none"] {
        let mut connection = KineticaDriver::default()
            .new_database_with_opts([
                (OptionDatabase::Uri, OptionValue::String(url.clone())),
                (OptionDatabase::Username, OptionValue::String(user.clone())),
                (
                    OptionDatabase::Password,
                    OptionValue::String(password.clone()),
                ),
                (
                    OptionDatabase::Other(REPLICATION.into()),
                    OptionValue::String(mode.into()),
                ),
            ])
            .unwrap_or_else(|e| panic!("database with replication {mode}: {e}"))
            .new_connection()
            .unwrap_or_else(|e| panic!("connection with replication {mode}: {e}"));

        let table = format!("adbc_repl_{}_{}", mode, unique_suffix());

        run(&mut connection, &format!("CREATE TABLE {table} (id INT)"));
        run(&mut connection, &format!("INSERT INTO {table} VALUES (1)"));

        let found = read(
            &mut connection,
            &format!("SELECT * FROM {table} WHERE id = 1"),
        );

        assert_eq!(found.rows, ["1"], "replication {mode}");

        run(&mut connection, &format!("DROP TABLE {table}"));
    }
}
