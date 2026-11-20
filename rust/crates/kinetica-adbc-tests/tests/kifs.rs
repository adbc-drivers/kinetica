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

//! Moving files to and from the server's own file system, which the driver
//! carries out rather than sending: the files are here, not there.
//!
//! Each test works in a directory of its own, both here and on the server,
//! and takes both away afterwards.

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::sync::{Connection, Database, Driver, Statement};
use adbc_driver_kinetica::{KineticaConnection, KineticaDriver};
use arrow_array::{Array, Int64Array};
use kinetica_adbc_tests::unique_suffix;

/// @return A connection to the instance under test
fn connect() -> KineticaConnection {
    try_connect().expect("connection")
}

/// @return A connection to the instance under test, or why there is none
fn try_connect() -> adbc_core::error::Result<KineticaConnection> {
    let kinetica_adbc_tests::Login {
        url,
        user,
        password,
    } = kinetica_adbc_tests::login();

    KineticaDriver::default()
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(url)),
            (OptionDatabase::Username, OptionValue::String(user)),
            (OptionDatabase::Password, OptionValue::String(password)),
        ])?
        .new_connection()
}

/// Runs a statement for its effect.
/// @return What it reported changing, which for a transfer is files moved
fn run(connection: &mut KineticaConnection, sql: &str) -> adbc_core::error::Result<Option<i64>> {
    let mut statement = connection.new_statement().expect("statement");
    statement.set_sql_query(sql)?;
    statement.execute_update()
}

/// A directory of this test's own, taken away when it is done with.
struct Sandbox {
    local: std::path::PathBuf,
    remote: String,
}

impl Sandbox {
    fn new(name: &str) -> Self {
        let suffix = unique_suffix();
        let local = std::env::temp_dir().join(format!("adbc_{name}_{suffix}"));

        std::fs::create_dir_all(&local).expect("a directory to work in");

        Self {
            local,
            remote: format!("adbc_{name}_{suffix}"),
        }
    }

    /// @return The path of a file written with the given contents
    fn wrote(&self, name: &str, contents: &str) -> std::path::PathBuf {
        let path = self.local.join(name);
        std::fs::write(&path, contents).expect("a file to send");
        path
    }
}

impl Drop for Sandbox {
    /// Nothing here may panic. A destructor that panics while its test is
    /// already failing aborts the process, and every test after it in this
    /// file goes unreported, so each step that can fail is let go instead; a
    /// server that cannot be reached keeps the directory.
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.local);

        let Ok(mut connection) = try_connect() else {
            return;
        };
        let Ok(mut statement) = connection.new_statement() else {
            return;
        };

        let remove = format!("DROP DIRECTORY '{}' WITH DELETE FILES = TRUE", self.remote);

        if statement.set_sql_query(remove).is_ok() {
            let _ = statement.execute_update();
        }
    }
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_file_goes_up_and_comes_back_the_same() {
    let sandbox = Sandbox::new("round");
    let sent = sandbox.wrote("one.csv", "id,name\n1,first\n");

    let mut connection = connect();

    let up = run(
        &mut connection,
        &format!("UPLOAD FILE '{}' INTO '{}'", sent.display(), sandbox.remote),
    )
    .expect("the upload should be carried out");

    // Taken away here, so what comes back can only have come from the server.
    std::fs::remove_file(&sent).expect("the local copy goes");

    let down = run(
        &mut connection,
        &format!(
            "DOWNLOAD FILE '{}/one.csv' INTO '{}'",
            sandbox.remote,
            sandbox.local.display()
        ),
    )
    .expect("the download should be carried out");

    let back = std::fs::read_to_string(&sent).unwrap_or_default();

    assert_eq!(up, Some(1), "one file should have gone up");
    assert_eq!(down, Some(1), "and one should have come back");
    assert_eq!(back, "id,name\n1,first\n", "unchanged by the round trip");
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn several_files_go_in_one_statement() {
    let sandbox = Sandbox::new("several");
    let first = sandbox.wrote("a.csv", "a\n");
    let second = sandbox.wrote("b.csv", "b\n");

    let mut connection = connect();

    let moved = run(
        &mut connection,
        &format!(
            "UPLOAD FILES '{}', '{}' INTO '{}'",
            first.display(),
            second.display(),
            sandbox.remote
        ),
    )
    .expect("the upload should be carried out");

    assert_eq!(moved, Some(2), "both files should have gone up");
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_glob_is_resolved_where_the_files_are() {
    let sandbox = Sandbox::new("glob");
    sandbox.wrote("one.csv", "1\n");
    sandbox.wrote("two.csv", "2\n");
    // Not a .csv, so the pattern should leave it behind.
    sandbox.wrote("notes.txt", "ignore me\n");

    let mut connection = connect();

    let moved = run(
        &mut connection,
        &format!(
            "UPLOAD FILES '{}/*.csv' INTO '{}'",
            sandbox.local.display(),
            sandbox.remote
        ),
    )
    .expect("the upload should be carried out");

    assert_eq!(
        moved,
        Some(2),
        "only the two matching files should have gone"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_transfer_run_as_a_query_answers_with_the_count() {
    let sandbox = Sandbox::new("asquery");
    let sent = sandbox.wrote("one.csv", "1\n");

    let mut connection = connect();
    let mut statement = connection.new_statement().expect("statement");

    statement
        .set_sql_query(format!(
            "UPLOAD FILE '{}' INTO '{}'",
            sent.display(),
            sandbox.remote
        ))
        .expect("set query");

    let batch = statement
        .execute()
        .expect("the upload should be carried out")
        .next()
        .expect("a row")
        .expect("a batch");

    let moved = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("a count");

    assert_eq!(moved.value(0), 1);
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_file_that_is_not_there_is_reported_rather_than_counted() {
    let sandbox = Sandbox::new("missing");

    let mut connection = connect();

    let failed = run(
        &mut connection,
        &format!(
            "UPLOAD FILE '{}/not-here.csv' INTO '{}'",
            sandbox.local.display(),
            sandbox.remote
        ),
    );

    assert!(
        failed.is_err(),
        "a file that is not there should be an error, got {failed:?}"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn an_ordinary_statement_still_goes_to_the_server() {
    // The recognizer must not take a statement that merely mentions a table
    // called upload, which would stop it ever reaching the server.
    let suffix = unique_suffix();
    let table = format!("upload_{suffix}");

    let mut connection = connect();

    run(
        &mut connection,
        &format!("CREATE TABLE ki_home.{table} (value INTEGER)"),
    )
    .expect("the server should make the table");

    let inserted = run(
        &mut connection,
        &format!("INSERT INTO ki_home.{table} VALUES (1)"),
    )
    .expect("the server should take the row");

    run(
        &mut connection,
        &format!("DROP TABLE IF EXISTS ki_home.{table}"),
    )
    .expect("and drop it again");

    assert_eq!(inserted, Some(1));
}
