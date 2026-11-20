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

//! Loading a local file into a table, which the driver reads itself because
//! the file is here rather than on the server.
//!
//! What is checked is the rows that end up in the table, read back through
//! the server: the file is only loaded correctly if the server agrees.

use std::io::Write;
use std::path::PathBuf;

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::sync::{Connection, Database, Driver, Statement};
use adbc_driver_kinetica::options::{FILE_READ_HAS_HEADER, FILE_READ_LIMIT, FILE_READ_NULL_STRING};
use adbc_driver_kinetica::{KineticaConnection, KineticaDriver};
use arrow_array::{Array, StringArray};
use kinetica_adbc_tests::unique_suffix;

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

/// @return A connection to the instance under test
fn connect() -> KineticaConnection {
    connect_with(&[])
}

/// Runs a statement for its effect.
/// @return The rows it reported
fn run(connection: &mut KineticaConnection, sql: &str) -> i64 {
    try_run(connection, sql).unwrap_or_else(|error| panic!("{sql}: {error}"))
}

/// @return What the statement reported, or its refusal
fn try_run(connection: &mut KineticaConnection, sql: &str) -> Result<i64, String> {
    let mut statement = connection.new_statement().map_err(|e| e.to_string())?;

    statement.set_sql_query(sql).map_err(|e| e.to_string())?;
    statement
        .execute_update()
        .map(|rows| rows.unwrap_or(-1))
        .map_err(|e| e.to_string())
}

/// Reads a query back, every cell as text and a null as `~`.
/// @return Its rows, sorted, each as its cells joined by '|'
fn rows(connection: &mut KineticaConnection, sql: &str) -> Vec<String> {
    let mut statement = connection.new_statement().expect("statement");

    statement.set_sql_query(sql).expect("set query");

    let reader = statement
        .execute()
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let mut found = Vec::new();

    for batch in reader {
        let batch = batch.expect("batch");

        for row in 0..batch.num_rows() {
            let cells: Vec<String> = batch
                .columns()
                .iter()
                .map(
                    |column| match column.as_any().downcast_ref::<StringArray>() {
                        Some(values) if !values.is_null(row) => values.value(row).to_string(),
                        _ => "~".to_string(),
                    },
                )
                .collect();

            found.push(cells.join("|"));
        }
    }

    found.sort();
    found
}

/// A directory of its own, so a glob in one test cannot match another's file.
struct Files {
    dir: PathBuf,
}

impl Files {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("adbc_file_{name}_{}", unique_suffix()));

        std::fs::create_dir_all(&dir).expect("a directory to write into");

        Self { dir }
    }

    /// @return The path of a file written with the given text
    fn write(&self, name: &str, text: &str) -> String {
        let path = self.dir.join(name);
        let mut file = std::fs::File::create(&path).expect("create");

        file.write_all(text.as_bytes()).expect("write");

        path.to_string_lossy().into_owned()
    }

    /// @return A pattern matching every file here with that extension
    fn glob(&self, extension: &str) -> String {
        self.dir
            .join(format!("*.{extension}"))
            .to_string_lossy()
            .into_owned()
    }
}

impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A file is read here and its rows reach the table, each field converted to
/// whatever its column holds and an empty one left null.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_file_is_read_here_and_its_rows_reach_the_table() {
    let mut connection = connect();
    let files = Files::new("plain");
    let table = format!("adbc_file_{}", unique_suffix());
    let csv = files.write("a.csv", "id,name,score\n1,alice,9.5\n2,bob,8\n3,carol,\n");

    run(
        &mut connection,
        &format!("CREATE TABLE {table} (id INT, name VARCHAR(32), score DOUBLE)"),
    );

    let loaded = run(
        &mut connection,
        &format!("INSERT INTO {table} SELECT * FROM FILE.\"{csv}\""),
    );

    assert_eq!(loaded, 3, "every row of the file");
    assert_eq!(
        rows(
            &mut connection,
            &format!(
                "SELECT CAST(id AS VARCHAR) AS a, name, CAST(score AS VARCHAR) AS c FROM {table}"
            )
        ),
        ["1|alice|9.5", "2|bob|8", "3|carol|~"],
        "an empty field is a null, and the rest are converted"
    );

    run(&mut connection, &format!("DROP TABLE {table}"));
}

/// The INSERT names the table's columns and the SELECT the file's, so the two
/// lists together say which field goes where.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn the_two_column_lists_say_which_field_goes_where() {
    let mut connection = connect();
    let files = Files::new("columns");
    let table = format!("adbc_file_cols_{}", unique_suffix());
    let csv = files.write("a.csv", "id,name,extra\n1,alice,x\n2,bob,y\n");

    run(
        &mut connection,
        &format!("CREATE TABLE {table} (who VARCHAR(32), num INT)"),
    );

    let loaded = run(
        &mut connection,
        &format!("INSERT INTO {table} (num, who) SELECT id, name FROM FILE.\"{csv}\""),
    );

    assert_eq!(loaded, 2);
    assert_eq!(
        rows(
            &mut connection,
            &format!("SELECT who, CAST(num AS VARCHAR) AS n FROM {table}")
        ),
        ["alice|1", "bob|2"],
        "the file's columns went where the statement said, extra left out"
    );

    run(&mut connection, &format!("DROP TABLE {table}"));
}

/// A glob reads every file it matches, and in a settled order so that two
/// runs of the same statement load the same rows.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_glob_reads_every_file_it_matches() {
    let mut connection = connect();
    let files = Files::new("glob");
    let table = format!("adbc_file_glob_{}", unique_suffix());

    files.write("one.csv", "id,name\n1,alice\n");
    files.write("two.csv", "id,name\n2,bob\n");
    files.write("other.txt", "id,name\n3,carol\n");

    run(
        &mut connection,
        &format!("CREATE TABLE {table} (id INT, name VARCHAR(32))"),
    );

    let loaded = run(
        &mut connection,
        &format!(
            "INSERT INTO {table} SELECT * FROM FILE.\"{}\"",
            files.glob("csv")
        ),
    );

    assert_eq!(loaded, 2, "the two csv files, not the txt one");
    assert_eq!(
        rows(
            &mut connection,
            &format!("SELECT CAST(id AS VARCHAR) AS a, name FROM {table}")
        ),
        ["1|alice", "2|bob"]
    );

    run(&mut connection, &format!("DROP TABLE {table}"));
}

/// How the file is read can be said on the connection and said again on the
/// statement, where it wins.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_statements_options_beat_the_connections() {
    let files = Files::new("options");
    let table = format!("adbc_file_opt_{}", unique_suffix());

    // No header, and the separator taken from the extension.
    let psv = files.write("a.psv", "1|alice\n2|bob\n3|carol\n");

    let mut connection = connect_with(&[(FILE_READ_HAS_HEADER, "false"), (FILE_READ_LIMIT, "2")]);

    run(
        &mut connection,
        &format!("CREATE TABLE {table} (id INT, name VARCHAR(32))"),
    );

    let loaded = run(
        &mut connection,
        &format!("INSERT INTO {table} SELECT * FROM FILE.\"{psv}\""),
    );

    assert_eq!(loaded, 2, "the connection capped it at two");

    // The statement asks for all three instead, into an emptied table.
    let again = run(
        &mut connection,
        &format!(
            "INSERT INTO {table} SELECT * FROM FILE.\"{psv}\" \
             WITH OPTIONS (file read limit = 3, file read initial clear = true)"
        ),
    );

    assert_eq!(again, 3, "the statement said three");
    assert_eq!(
        rows(
            &mut connection,
            &format!("SELECT CAST(id AS VARCHAR) AS a, name FROM {table}")
        ),
        ["1|alice", "2|bob", "3|carol"],
        "and emptied the table first, so the first two are not there twice"
    );

    run(&mut connection, &format!("DROP TABLE {table}"));
}

/// The text that stands for a null is a setting, since an empty field is not
/// always how a file writes one.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn the_text_that_stands_for_a_null_can_be_said() {
    let mut connection = connect_with(&[(FILE_READ_NULL_STRING, "NULL")]);
    let files = Files::new("nulls");
    let table = format!("adbc_file_null_{}", unique_suffix());
    let csv = files.write("a.csv", "id,name\n1,NULL\n2,bob\n");

    run(
        &mut connection,
        &format!("CREATE TABLE {table} (id INT, name VARCHAR(32))"),
    );
    run(
        &mut connection,
        &format!("INSERT INTO {table} SELECT * FROM FILE.\"{csv}\""),
    );

    assert_eq!(
        rows(
            &mut connection,
            &format!("SELECT CAST(id AS VARCHAR) AS a, name FROM {table}")
        ),
        ["1|~", "2|bob"]
    );

    run(&mut connection, &format!("DROP TABLE {table}"));
}

/// A path that matches nothing is refused here, rather than being sent to a
/// server that could not have read it anyway.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_path_matching_nothing_is_refused() {
    let mut connection = connect();
    let files = Files::new("missing");
    let table = format!("adbc_file_none_{}", unique_suffix());

    run(&mut connection, &format!("CREATE TABLE {table} (id INT)"));

    let refused = try_run(
        &mut connection,
        &format!(
            "INSERT INTO {table} SELECT * FROM FILE.\"{}\"",
            files.glob("nothing")
        ),
    )
    .expect_err("there is no such file");

    assert!(refused.contains("matches no file"), "{refused}");

    run(&mut connection, &format!("DROP TABLE {table}"));
}
