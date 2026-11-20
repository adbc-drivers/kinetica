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

//! What `get_objects` says is promised about a table: its primary key, and
//! the foreign keys it holds with where each of them points.
//!
//! A tool drawing the relationships between tables reads exactly this, so the
//! assertions follow the shape ADBC prescribes rather than the queries the
//! driver happens to run.

use adbc_core::options::{ObjectDepth, OptionDatabase, OptionValue};
use adbc_core::sync::{Connection, Database, Driver, Statement};
use adbc_driver_kinetica::{KineticaConnection, KineticaDriver};
use arrow_array::{Array, ListArray, RecordBatch, StringArray, StructArray};
use kinetica_adbc_tests::unique_suffix;

/// One constraint, flattened out of the nested shape get_objects returns.
#[derive(Debug, PartialEq, Eq)]
struct Constraint {
    kind: String,
    columns: Vec<String>,
    /// Schema, table and column, for each place a foreign key points.
    references: Vec<(String, String, String)>,
}

/// @return A connection to the instance under test
fn connect() -> KineticaConnection {
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
        ])
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

/// @return The named field of the struct, as a list
fn list(parent: &StructArray, field: &str) -> ListArray {
    parent
        .column_by_name(field)
        .unwrap_or_else(|| panic!("{field} should be there"))
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap_or_else(|| panic!("{field} should be a list"))
        .clone()
}

/// @return The struct the list holds at that position
fn element(values: &ListArray, index: usize) -> StructArray {
    values
        .value(index)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("a struct")
        .clone()
}

/// @return The named string field, at that position
fn text(parent: &StructArray, field: &str, index: usize) -> String {
    parent
        .column_by_name(field)
        .unwrap_or_else(|| panic!("{field} should be there"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("{field} should be a string"))
        .value(index)
        .to_string()
}

/// Walks the shape get_objects returns down to the one table asked for.
/// @return Its constraints, or an empty list if the table is not there
fn constraints_of(batch: &RecordBatch, schema: &str, table: &str) -> Vec<Constraint> {
    let catalogs = batch
        .column_by_name("catalog_db_schemas")
        .expect("catalog_db_schemas")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("a list of schemas");

    let mut found = Vec::new();

    for catalog in 0..catalogs.len() {
        let schemas = element(catalogs, catalog);

        for i in 0..schemas.len() {
            if text(&schemas, "db_schema_name", i) != schema {
                continue;
            }

            let tables = element(&list(&schemas, "db_schema_tables"), i);

            for t in 0..tables.len() {
                if text(&tables, "table_name", t) != table {
                    continue;
                }

                let held = list(&tables, "table_constraints");
                if held.is_null(t) {
                    continue;
                }

                let constraints = element(&held, t);

                for c in 0..constraints.len() {
                    let names = list(&constraints, "constraint_column_names");
                    let columns = names
                        .value(c)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("column names")
                        .iter()
                        .flatten()
                        .map(str::to_string)
                        .collect();

                    let usage = list(&constraints, "constraint_column_usage");
                    let mut references = Vec::new();

                    if !usage.is_null(c) {
                        let points_at = usage.value(c);
                        let points_at = points_at
                            .as_any()
                            .downcast_ref::<StructArray>()
                            .expect("usage");

                        for u in 0..points_at.len() {
                            references.push((
                                text(points_at, "fk_db_schema", u),
                                text(points_at, "fk_table", u),
                                text(points_at, "fk_column_name", u),
                            ));
                        }
                    }

                    found.push(Constraint {
                        kind: text(&constraints, "constraint_type", c),
                        columns,
                        references,
                    });
                }
            }
        }
    }

    found
}

/// @return What get_objects says about the one table, at full depth
fn asked_about(connection: &mut KineticaConnection, schema: &str, table: &str) -> Vec<Constraint> {
    let mut reader = connection
        .get_objects(
            ObjectDepth::All,
            None,
            Some(schema),
            Some(table),
            None,
            None,
        )
        .expect("get_objects");

    let batch = reader.next().expect("a batch").expect("a batch");

    constraints_of(&batch, schema, table)
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_primary_key_and_the_foreign_key_pointing_at_it_are_both_reported() {
    let suffix = unique_suffix();
    let parent = format!("adbc_parent_{suffix}");
    let child = format!("adbc_child_{suffix}");
    let schema = "ki_home";

    let mut connection = connect();

    run(
        &mut connection,
        &format!(
            "CREATE TABLE {schema}.{parent} \
             (id INTEGER NOT NULL, name VARCHAR(32), PRIMARY KEY (id))"
        ),
    );
    // A foreign key needs the two shard keys to line up, so the child is
    // sharded on the column that points at the parent's key.
    run(
        &mut connection,
        &format!(
            "CREATE TABLE {schema}.{child} \
             (cid INTEGER NOT NULL, pid INTEGER NOT NULL, SHARD KEY (pid))"
        ),
    );
    run(
        &mut connection,
        &format!(
            "ALTER TABLE {schema}.{child} ADD FOREIGN KEY (pid) \
             REFERENCES {schema}.{parent}(id) AS fk_{suffix}"
        ),
    );

    let on_parent = asked_about(&mut connection, schema, &parent);
    let on_child = asked_about(&mut connection, schema, &child);

    run(
        &mut connection,
        &format!("DROP TABLE IF EXISTS {schema}.{child}"),
    );
    run(
        &mut connection,
        &format!("DROP TABLE IF EXISTS {schema}.{parent}"),
    );

    assert_eq!(
        on_parent,
        vec![Constraint {
            kind: "PRIMARY KEY".to_string(),
            columns: vec!["id".to_string()],
            references: Vec::new(),
        }],
        "the parent's primary key should be reported, and point at nothing"
    );

    assert_eq!(
        on_child,
        vec![Constraint {
            kind: "FOREIGN KEY".to_string(),
            columns: vec!["pid".to_string()],
            references: vec![(schema.to_string(), parent.clone(), "id".to_string())],
        }],
        "the child's foreign key should say where it points"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_table_promising_nothing_reports_nothing() {
    let suffix = unique_suffix();
    let table = format!("adbc_plain_{suffix}");

    let mut connection = connect();
    run(
        &mut connection,
        &format!("CREATE TABLE ki_home.{table} (value INTEGER)"),
    );

    let found = asked_about(&mut connection, "ki_home", &table);

    run(
        &mut connection,
        &format!("DROP TABLE IF EXISTS ki_home.{table}"),
    );

    assert!(
        found.is_empty(),
        "a table with no keys should report no constraints, found {found:?}"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn constraints_are_left_out_below_the_depth_that_asks_for_them() {
    let suffix = unique_suffix();
    let table = format!("adbc_depth_{suffix}");

    let mut connection = connect();
    run(
        &mut connection,
        &format!("CREATE TABLE ki_home.{table} (id INTEGER NOT NULL, PRIMARY KEY (id))"),
    );

    let mut reader = connection
        .get_objects(
            ObjectDepth::Tables,
            None,
            Some("ki_home"),
            Some(&table),
            None,
            None,
        )
        .expect("get_objects");
    let batch = reader.next().expect("a batch").expect("a batch");
    let shallow = constraints_of(&batch, "ki_home", &table);

    let deep = asked_about(&mut connection, "ki_home", &table);

    run(
        &mut connection,
        &format!("DROP TABLE IF EXISTS ki_home.{table}"),
    );

    assert!(
        shallow.is_empty(),
        "a table-depth read should not go looking for constraints"
    );
    assert_eq!(
        deep.len(),
        1,
        "and the same table at full depth should report its primary key"
    );
}
