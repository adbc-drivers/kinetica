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

//! Runs against a live instance, because the encodings the server actually
//! sends are the thing worth testing. The instance and its login come from KI_URL, KI_USER and KI_PWD.

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::sync::{Connection, Database, Driver, Statement};
use adbc_driver_kinetica::KineticaDriver;
use arrow_array::Array;

fn connect() -> adbc_driver_kinetica::KineticaConnection {
    let kinetica_adbc_tests::Login {
        url,
        user,
        password,
    } = kinetica_adbc_tests::login();

    let mut driver = KineticaDriver::default();
    let database = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(url)),
            (OptionDatabase::Username, OptionValue::String(user)),
            (OptionDatabase::Password, OptionValue::String(password)),
        ])
        .expect("database");

    let mut connection = database.new_connection().expect("connection");

    fixtures(&mut connection);

    connection
}

/// The tables the read-only tests select from. They are made once for the run
/// rather than assumed to be there, so the suite works against an instance it
/// has never seen. The Python suite makes the same ones, in tests/conftest.py.
fn fixtures(connection: &mut adbc_driver_kinetica::KineticaConnection) {
    static ONCE: std::sync::Once = std::sync::Once::new();

    ONCE.call_once(|| {
        // One column of every type the driver maps, in the order the tests
        // index them by; its column count is asserted, so adding one means
        // updating the test that counts them.
        let statements = [
            "CREATE OR REPLACE TABLE adbc_types (\
                 c_int INTEGER, c_long BIGINT, c_ulong UNSIGNED BIGINT, c_double DOUBLE, \
                 c_dec DECIMAL(19,4), c_char32 CHAR(32), c_str VARCHAR, c_ts TIMESTAMP, \
                 c_date DATE, c_time TIME, c_bool BOOLEAN, c_bytes BLOB)",
            "INSERT INTO adbc_types VALUES (42, 9000000000, 3, 2.5, 12345.6789, \
                 'a char32 value', 'a string', 1700000000000, '2024-10-04', '12:34:56.789', \
                 TRUE, x'0102030405')",
            // Larger than one page, so paging is visible to a test.
            "CREATE OR REPLACE TABLE adbc_pg AS SELECT generate_series AS id, \
                 generate_series % 7 AS grp, generate_series / 2.0 AS val \
                 FROM TABLE(GENERATE_SERIES(1, 50001))",
        ];

        for sql in statements {
            let mut statement = connection.new_statement().expect("statement");
            statement.set_sql_query(sql).expect("fixture query");
            statement.execute_update().expect("fixture");
        }
    });
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn reads_a_scalar() {
    let _running = kinetica_adbc_tests::shared();
    let mut statement = connect().new_statement().expect("statement");
    statement.set_sql_query("SELECT 1 AS n").expect("query");

    let reader = statement.execute().expect("execute");
    let schema = reader.schema();
    let batches: Vec<_> = reader.map(|b| b.expect("batch")).collect();

    assert_eq!(schema.field(0).name(), "n");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn maps_every_column_type() {
    let _running = kinetica_adbc_tests::shared();
    let mut statement = connect().new_statement().expect("statement");
    statement
        .set_sql_query(
            "SELECT c_int, c_long, c_ulong, c_double, c_dec, c_char32, c_str, \
             c_ts, c_date, c_time, c_bool, c_bytes FROM adbc_types",
        )
        .expect("query");

    let reader = statement.execute().expect("execute");
    let schema = reader.schema();

    let types: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.data_type().to_string())
        .collect();

    assert_eq!(
        types[2], "UInt64",
        "unsigned long should not degrade to a string"
    );
    assert_eq!(types[4], "Decimal128(19, 4)");
    assert_eq!(types[8], "Date32");
    assert_eq!(types[10], "Boolean");

    // The properties are what let a consumer recreate a column Arrow cannot name.
    let metadata = schema.field(5).metadata();
    assert_eq!(
        metadata.get("kinetica.type").map(String::as_str),
        Some("char32")
    );

    let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
    assert_eq!(rows, 1);
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn pages_through_a_result_larger_than_one_request() {
    let _running = kinetica_adbc_tests::shared();
    // adbc_pg holds 50,001 rows and a request is capped at 20,000, so this
    // only passes if the reader fetches successive pages.
    let mut statement = connect().new_statement().expect("statement");
    statement
        .set_sql_query("SELECT id FROM adbc_pg")
        .expect("query");

    let reader = statement.execute().expect("execute");

    let mut rows = 0usize;
    let mut ids = std::collections::HashSet::new();

    for batch in reader {
        let batch = batch.expect("batch");
        rows += batch.num_rows();

        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .expect("int32 column");

        for i in 0..column.len() {
            ids.insert(column.value(i));
        }
    }

    assert_eq!(rows, 50_001, "every row should arrive");
    assert_eq!(ids.len(), 50_001, "and none of them twice");
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn describes_a_table_without_reading_it() {
    let _running = kinetica_adbc_tests::shared();
    let connection = connect();
    let schema = connection
        .get_table_schema(None, Some("ki_home"), "adbc_types")
        .expect("table schema");

    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(names.contains(&"c_ulong"), "got {names:?}");

    // The types come from the query path, so they match what a select returns
    // rather than being rebuilt from catalog text.
    let ulong = schema.field_with_name("c_ulong").expect("column");
    assert_eq!(ulong.data_type().to_string(), "UInt64");
    assert_eq!(
        ulong.metadata().get("kinetica.type").map(String::as_str),
        Some("ulong")
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn lists_the_table_types() {
    let _running = kinetica_adbc_tests::shared();
    let connection = connect();
    let reader = connection.get_table_types().expect("table types");

    assert_eq!(reader.schema().field(0).name(), "table_type");

    let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
    assert!(rows >= 2, "expected several table types, got {rows}");
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn browses_the_catalog() {
    let _running = kinetica_adbc_tests::shared();
    use adbc_core::options::ObjectDepth;

    let connection = connect();
    let reader = connection
        .get_objects(
            ObjectDepth::All,
            None,
            Some("ki_home"),
            Some("adbc_types"),
            None,
            None,
        )
        .expect("get_objects");

    assert_eq!(reader.schema().field(0).name(), "catalog_name");

    let batches: Vec<_> = reader.map(|b| b.expect("batch")).collect();
    assert_eq!(batches.len(), 1);

    // One catalog holding one schema holding the table we asked for.
    let catalogs = batches[0]
        .column_by_name("catalog_db_schemas")
        .expect("catalog_db_schemas")
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .expect("list");

    let db_schemas = catalogs
        .value(0)
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .expect("struct")
        .clone();

    assert_eq!(db_schemas.len(), 1, "only the schema asked for");

    let tables = db_schemas
        .column_by_name("db_schema_tables")
        .expect("db_schema_tables")
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .expect("list")
        .value(0);

    let tables = tables
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .expect("struct");
    assert_eq!(tables.len(), 1, "only the table asked for");

    let columns = tables
        .column_by_name("table_columns")
        .expect("table_columns")
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .expect("list")
        .value(0);

    assert_eq!(columns.len(), 12, "every column of adbc_types");
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn reports_a_bad_query() {
    let _running = kinetica_adbc_tests::shared();
    let mut statement = connect().new_statement().expect("statement");
    statement
        .set_sql_query("SELECT * FROM no_such_table_here")
        .expect("query");

    let error = statement.execute().err().expect("should fail");
    assert!(!error.message.is_empty());
}

/// Builds a stream the driver can bind, which is what a bulk loader hands it.
/// @return The reader over one batch
fn stream_of(batch: arrow_array::RecordBatch) -> Box<dyn arrow_array::RecordBatchReader + Send> {
    let schema = batch.schema();

    Box::new(arrow_array::RecordBatchIterator::new(
        vec![Ok(batch)],
        schema,
    ))
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn ingests_a_bound_stream() {
    let _running = kinetica_adbc_tests::shared();
    use adbc_core::options::{OptionStatement, OptionValue};
    use adbc_core::Optionable;
    use arrow_array::{
        BooleanArray, Date32Array, Float64Array, Int32Array, RecordBatch, StringArray,
        TimestampMillisecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    let mut connection = connect();

    let mut ddl = connection.new_statement().expect("statement");
    ddl.set_sql_query(
        "CREATE OR REPLACE TABLE adbc_ingest \
         (id INT NOT NULL, name VARCHAR(32), amount DOUBLE, seen TIMESTAMP, \
          day DATE, ok BOOLEAN, PRIMARY KEY (id))",
    )
    .expect("ddl");
    ddl.execute_update().expect("create");

    let rows = 5_000;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
        Field::new(
            "seen",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("day", DataType::Date32, true),
        Field::new("ok", DataType::Boolean, true),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from_iter_values(0..rows)),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|i| format!("row-{i}")),
            )),
            Arc::new(Float64Array::from_iter_values(
                (0..rows).map(|i| i as f64 / 4.0),
            )),
            Arc::new(TimestampMillisecondArray::from_iter_values(
                (0..rows).map(|i| 1_700_000_000_000 + i as i64 * 1_000),
            )),
            Arc::new(Date32Array::from_iter_values((0..rows).map(|i| 20_000 + i))),
            Arc::new(BooleanArray::from_iter((0..rows).map(|i| Some(i % 2 == 0)))),
        ],
    )
    .expect("batch");

    let mut ingest = connection.new_statement().expect("statement");
    ingest
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("adbc_ingest".into()),
        )
        .expect("target");
    ingest.bind_stream(stream_of(batch)).expect("bind");

    let inserted = ingest.execute_update().expect("ingest");
    assert_eq!(inserted, Some(rows as i64));

    let mut check = connection.new_statement().expect("statement");
    check
        .set_sql_query(
            "SELECT COUNT(*) AS n, MIN(name) AS lo, SUM(amount) AS total, \
             COUNT(CASE WHEN ok THEN 1 END) AS trues FROM adbc_ingest",
        )
        .expect("query");

    let batches: Vec<_> = check
        .execute()
        .expect("execute")
        .map(|b| b.expect("batch"))
        .collect();
    let row = &batches[0];

    let count = row
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("n");
    assert_eq!(count.value(0), rows as i64);

    let lowest = row
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("lo");
    assert_eq!(lowest.value(0), "row-0");

    let total = row
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("total");
    assert!((total.value(0) - (0..rows).map(|i| i as f64 / 4.0).sum::<f64>()).abs() < 1.0);

    let trues = row
        .column(3)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("trues");
    assert_eq!(trues.value(0), (rows as i64 + 1) / 2);
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn ingest_needs_a_target_table() {
    let _running = kinetica_adbc_tests::shared();
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).expect("batch");

    let mut statement = connect().new_statement().expect("statement");
    statement.bind_stream(stream_of(batch)).expect("bind");

    assert!(statement.execute_update().is_err());
}

/// Sorting reaches the rows through a different fetch than a plain scan, and
/// that fetch used to hand the head stringified values while the response was
/// still encoded as though they were raw.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn sorting_does_not_change_the_types() {
    let _running = kinetica_adbc_tests::shared();
    let mut connection = connect();

    let mut ddl = connection.new_statement().expect("statement");
    ddl.set_sql_query(
        "CREATE OR REPLACE TABLE adbc_sorted (id INT, ts TIMESTAMP, dt DATETIME, d DATE)",
    )
    .expect("ddl");
    ddl.execute_update().expect("create");

    let mut insert = connection.new_statement().expect("statement");
    insert
        .set_sql_query(
            "INSERT INTO adbc_sorted VALUES \
             (1, 1700000000000, '2023-11-14 22:13:20', '2023-11-14'), \
             (2, 1700000001000, '2023-11-14 22:13:21', '2023-11-15')",
        )
        .expect("insert");
    insert.execute_update().expect("rows");

    let read = |connection: &mut adbc_driver_kinetica::KineticaConnection, sql: &str| {
        let mut statement = connection.new_statement().expect("statement");
        statement.set_sql_query(sql).expect("query");

        let reader = statement.execute().expect("execute");
        let schema = reader.schema();
        let batches: Vec<_> = reader.map(|b| b.expect("batch")).collect();

        let types: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| f.data_type().to_string())
            .collect();
        let stamps: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<arrow_array::TimestampMillisecondArray>()
                    .expect("ts")
                    .values()
                    .to_vec()
            })
            .collect();

        (types, stamps)
    };

    let plain = read(&mut connection, "SELECT id, ts, dt, d FROM adbc_sorted");
    let sorted = read(
        &mut connection,
        "SELECT id, ts, dt, d FROM adbc_sorted ORDER BY id",
    );

    assert_eq!(
        plain.0, sorted.0,
        "sorting should not change the column types"
    );
    assert_eq!(sorted.1, vec![1_700_000_000_000, 1_700_000_001_000]);
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn partitions_cover_the_result_exactly_once() {
    let _running = kinetica_adbc_tests::shared();
    use adbc_core::options::{OptionStatement, OptionValue};
    use adbc_core::Optionable;

    let mut connection = connect();

    // Its own table, since the tests share an instance and run at once.
    let mut ddl = connection.new_statement().expect("statement");
    ddl.set_sql_query(
        "CREATE OR REPLACE TABLE adbc_parts AS          SELECT generate_series AS id FROM TABLE(GENERATE_SERIES(0, 4999))",
    )
    .expect("ddl");
    ddl.execute_update().expect("create");

    let mut statement = connection.new_statement().expect("statement");

    statement
        .set_option(
            OptionStatement::Other("adbc.kinetica.batch_size".into()),
            OptionValue::Int(1_000),
        )
        .expect("batch size");
    statement
        .set_sql_query("SELECT id FROM adbc_parts")
        .expect("query");

    let result = statement.execute_partitions().expect("partitions");

    assert_eq!(result.schema.field(0).name(), "id");
    assert_eq!(result.partitions.len(), 5, "5000 rows in pieces of 1000");

    // Each piece is read on its own connection, which is what a consumer that
    // distributes them would do.
    let mut seen: Vec<i32> = Vec::new();

    for piece in &result.partitions {
        let reader = connect().read_partition(piece).expect("read");

        for batch in reader {
            let batch = batch.expect("batch");
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .expect("id");
            seen.extend(ids.values());
        }
    }

    seen.sort_unstable();
    seen.dedup();

    assert_eq!(seen.len(), 5_000, "every row once, and no row twice");
    assert_eq!(seen.first(), Some(&0));
    assert_eq!(seen.last(), Some(&4_999));
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn cancels_a_running_query() {
    let _running = kinetica_adbc_tests::shared();
    use std::sync::mpsc;
    use std::time::Duration;

    let slow = "SELECT COUNT(*) FROM adbc_parts a, adbc_parts b, adbc_parts c \
                WHERE a.id < b.id AND b.id < c.id";

    let (sender, receiver) = mpsc::channel();

    let running = std::thread::spawn(move || {
        let mut statement = connect().new_statement().expect("statement");
        statement.set_sql_query(slow).expect("query");
        sender.send(()).expect("started");

        statement.execute().map(|reader| reader.count())
    });

    receiver
        .recv_timeout(Duration::from_secs(10))
        .expect("start");
    std::thread::sleep(Duration::from_secs(2));

    let mut canceller = connect().new_statement().expect("statement");
    canceller.set_sql_query(slow).expect("query");
    canceller.cancel().expect("cancel");

    assert!(
        running.join().expect("thread").is_err(),
        "a cancelled query should not succeed"
    );
}

/// Builds the table both insert-routing tests write into.
/// @return A connection with the table freshly created
fn insert_target(name: &str) -> adbc_driver_kinetica::KineticaConnection {
    let mut connection = connect();
    let mut ddl = connection.new_statement().expect("statement");

    ddl.set_sql_query(format!(
        "CREATE OR REPLACE TABLE {name} (id INT, name VARCHAR(32), amount DOUBLE)"
    ))
    .expect("ddl");
    ddl.execute_update().expect("create");

    connection
}

/// @return One batch of ids and names, without the third column
fn two_column_batch(rows: i32) -> arrow_array::RecordBatch {
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_iter_values(0..rows)),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|i| format!("row-{i}")),
            )),
        ],
    )
    .expect("batch")
}

/// A plain INSERT of nothing but markers says the same as a target table, so
/// its rows go to the workers and the column it does not name stays null.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_plain_insert_goes_to_the_workers() {
    let _running = kinetica_adbc_tests::shared();
    let mut connection = insert_target("adbc_plain");

    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_sql_query("INSERT INTO adbc_plain (id, name) VALUES (?, ?)")
        .expect("query");
    statement
        .bind_stream(stream_of(two_column_batch(2_000)))
        .expect("bind");

    assert_eq!(statement.execute_update().expect("insert"), Some(2_000));

    let mut check = connection.new_statement().expect("statement");
    check
        .set_sql_query("SELECT COUNT(*) AS n, COUNT(amount) AS filled FROM adbc_plain")
        .expect("query");

    let batches: Vec<_> = check
        .execute()
        .expect("execute")
        .map(|b| b.expect("batch"))
        .collect();
    let row = &batches[0];

    let count = row
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("n");
    let filled = row
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("filled");

    assert_eq!(count.value(0), 2_000);
    assert_eq!(filled.value(0), 0, "the unnamed column should be left null");
}

/// An INSERT whose values are worked out by the server cannot go to the
/// workers, so it runs there instead — and still gets the right answer.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn an_insert_with_expressions_goes_to_the_server() {
    let _running = kinetica_adbc_tests::shared();
    let mut connection = insert_target("adbc_expr");

    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_sql_query("INSERT INTO adbc_expr (id, name, amount) VALUES (?, UPPER(?), ? * 2)")
        .expect("query");
    statement
        .bind_stream(stream_of(three_column_batch(10)))
        .expect("bind");

    assert_eq!(statement.execute_update().expect("insert"), Some(10));

    let mut check = connection.new_statement().expect("statement");
    check
        .set_sql_query("SELECT name, amount FROM adbc_expr WHERE id = 3")
        .expect("query");

    let batches: Vec<_> = check
        .execute()
        .expect("execute")
        .map(|b| b.expect("batch"))
        .collect();
    let row = &batches[0];

    let name = row
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("name");
    let amount = row
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .expect("amount");

    assert_eq!(
        name.value(0),
        "ROW-3",
        "the server should have applied UPPER"
    );
    assert_eq!(
        amount.value(0),
        6.0,
        "the server should have applied the arithmetic"
    );
}

/// @return One batch of ids, names and amounts
fn three_column_batch(rows: i32) -> arrow_array::RecordBatch {
    use arrow_array::{Float64Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_iter_values(0..rows)),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|i| format!("row-{i}")),
            )),
            Arc::new(Float64Array::from_iter_values((0..rows).map(|i| i as f64))),
        ],
    )
    .expect("batch")
}

/// @return A statement bound to one batch, aimed at a table, in the given mode
fn ingest_into(
    connection: &mut adbc_driver_kinetica::KineticaConnection,
    table: &str,
    mode: &str,
    batch: arrow_array::RecordBatch,
) -> adbc_core::error::Result<Option<i64>> {
    use adbc_core::options::{OptionStatement, OptionValue};
    use adbc_core::Optionable;

    let mut statement = connection.new_statement().expect("statement");

    statement.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(table.into()),
    )?;
    statement.set_option(
        OptionStatement::IngestMode,
        OptionValue::String(mode.into()),
    )?;
    statement.bind_stream(stream_of(batch))?;
    statement.execute_update()
}

/// @return The number of rows in the table
fn count_of(connection: &mut adbc_driver_kinetica::KineticaConnection, table: &str) -> i64 {
    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_sql_query(format!("SELECT COUNT(*) AS n FROM {table}"))
        .expect("query");

    let batches: Vec<_> = statement
        .execute()
        .expect("execute")
        .map(|b| b.expect("batch"))
        .collect();

    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("n")
        .value(0)
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn create_makes_the_table_and_then_refuses_to() {
    let _running = kinetica_adbc_tests::shared();
    let mut connection = connect();

    let mut drop = connection.new_statement().expect("statement");
    drop.set_sql_query("DROP TABLE IF EXISTS adbc_mode_create")
        .expect("ddl");
    drop.execute_update().expect("drop");

    assert_eq!(
        ingest_into(
            &mut connection,
            "adbc_mode_create",
            "adbc.ingest.mode.create",
            three_column_batch(100)
        )
        .expect("create"),
        Some(100)
    );
    assert_eq!(count_of(&mut connection, "adbc_mode_create"), 100);

    // The table is there now, which is exactly what this mode will not accept.
    assert!(
        ingest_into(
            &mut connection,
            "adbc_mode_create",
            "adbc.ingest.mode.create",
            three_column_batch(100)
        )
        .is_err(),
        "create should refuse a table that exists"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn replace_starts_the_table_again_and_create_append_adds_to_it() {
    let _running = kinetica_adbc_tests::shared();
    let mut connection = connect();

    ingest_into(
        &mut connection,
        "adbc_mode_replace",
        "adbc.ingest.mode.replace",
        three_column_batch(100),
    )
    .expect("first");
    ingest_into(
        &mut connection,
        "adbc_mode_replace",
        "adbc.ingest.mode.replace",
        three_column_batch(40),
    )
    .expect("second");

    assert_eq!(
        count_of(&mut connection, "adbc_mode_replace"),
        40,
        "replace should start over"
    );

    ingest_into(
        &mut connection,
        "adbc_mode_replace",
        "adbc.ingest.mode.create_append",
        three_column_batch(10),
    )
    .expect("append to it");

    assert_eq!(
        count_of(&mut connection, "adbc_mode_replace"),
        50,
        "create_append should add"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_created_table_keeps_the_types_the_stream_had() {
    let _running = kinetica_adbc_tests::shared();
    use adbc_core::options::{OptionStatement, OptionValue};
    use adbc_core::Optionable;

    let mut connection = connect();

    let mut source = connection.new_statement().expect("statement");
    source
        .set_sql_query("SELECT * FROM adbc_types")
        .expect("query");

    let reader = source.execute().expect("execute");
    let wanted: Vec<String> = reader
        .schema()
        .fields()
        .iter()
        .map(|f| f.data_type().to_string())
        .collect();

    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("adbc_types_copy".into()),
        )
        .expect("target");
    statement
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("adbc.ingest.mode.replace".into()),
        )
        .expect("mode");
    statement.bind_stream(Box::new(reader)).expect("bind");
    statement.execute_update().expect("copy");

    let mut check = connection.new_statement().expect("statement");
    check
        .set_sql_query("SELECT * FROM adbc_types_copy")
        .expect("query");

    let got: Vec<String> = check
        .execute()
        .expect("execute")
        .schema()
        .fields()
        .iter()
        .map(|f| f.data_type().to_string())
        .collect();

    assert_eq!(got, wanted, "a copy should come back as the original did");
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_statement_says_how_many_parameters_it_wants() {
    let _running = kinetica_adbc_tests::shared();
    let mut statement = connect().new_statement().expect("statement");

    statement
        .set_sql_query("SELECT * FROM adbc_types WHERE c_int = ? AND c_str = ?")
        .expect("query");

    let schema = statement.get_parameter_schema().expect("parameters");

    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "$1");
    assert_eq!(schema.field(1).data_type(), &arrow_schema::DataType::Null);

    statement.set_sql_query("SELECT 1").expect("query");
    assert_eq!(
        statement
            .get_parameter_schema()
            .expect("none")
            .fields()
            .len(),
        0
    );
}

/// Walks the nested shape down to the statistics of one table.
/// @return Every (column, key, value) reported for it
fn statistics_of(
    connection: &mut adbc_driver_kinetica::KineticaConnection,
    table: &str,
    approximate: bool,
) -> Vec<(Option<String>, i16, i64)> {
    use arrow_array::{Array, Int16Array, Int64Array, ListArray, StringArray, StructArray};

    let reader = connection
        .get_statistics(None, Some("ki_home"), Some(table), approximate)
        .expect("statistics");

    let mut found = Vec::new();

    for batch in reader {
        let batch = batch.expect("batch");
        let catalogs = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("catalogs");

        for c in 0..catalogs.len() {
            let schemas_value = catalogs.value(c);
            let schemas = schemas_value
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("schemas");
            let lists = schemas
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("lists");

            for s in 0..lists.len() {
                let stats_value = lists.value(s);
                let stats = stats_value
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("stats");

                let tables = stats
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("tables");
                let columns = stats
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("columns");
                let keys = stats
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .expect("keys");
                let values = stats
                    .column(3)
                    .as_any()
                    .downcast_ref::<arrow_array::UnionArray>()
                    .expect("values");

                for i in 0..stats.len() {
                    if tables.value(i) != table {
                        continue;
                    }

                    let held = values.value(i);
                    let count = held
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("int64")
                        .value(0);

                    found.push((
                        (!columns.is_null(i)).then(|| columns.value(i).to_string()),
                        keys.value(i),
                        count,
                    ));
                }
            }
        }
    }

    found
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn statistics_report_the_row_count_without_reading_the_table() {
    let _running = kinetica_adbc_tests::shared();
    let mut connection = connect();

    let mut ddl = connection.new_statement().expect("statement");
    ddl.set_sql_query(
        "CREATE OR REPLACE TABLE adbc_stats AS \
         SELECT generate_series AS id, CASE WHEN generate_series % 4 = 0 THEN NULL \
         ELSE generate_series % 10 END AS bucket FROM TABLE(GENERATE_SERIES(0, 99))",
    )
    .expect("ddl");
    ddl.execute_update().expect("create");

    let approximate = statistics_of(&mut connection, "adbc_stats", true);

    // ADBC_STATISTIC_ROW_COUNT_KEY
    assert_eq!(
        approximate,
        vec![(None, 6, 100)],
        "only the row count, and it is exact"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn asking_for_exact_statistics_counts_the_columns_too() {
    let _running = kinetica_adbc_tests::shared();
    let mut connection = connect();

    let mut ddl = connection.new_statement().expect("statement");
    ddl.set_sql_query(
        "CREATE OR REPLACE TABLE adbc_stats_exact AS \
         SELECT generate_series AS id, CASE WHEN generate_series % 4 = 0 THEN NULL \
         ELSE generate_series % 10 END AS bucket FROM TABLE(GENERATE_SERIES(0, 99))",
    )
    .expect("ddl");
    ddl.execute_update().expect("create");

    let exact = statistics_of(&mut connection, "adbc_stats_exact", false);

    let of = |column: &str, key: i16| {
        exact
            .iter()
            .find(|(c, k, _)| c.as_deref() == Some(column) && *k == key)
            .map(|(_, _, v)| *v)
    };

    // ADBC_STATISTIC_NULL_COUNT_KEY is 5, DISTINCT_COUNT is 1, ROW_COUNT is 6.
    assert_eq!(of("id", 5), Some(0), "the ids are all there");
    assert_eq!(of("id", 1), Some(100), "and all different");
    assert_eq!(of("bucket", 5), Some(25), "every fourth row is null");
    assert_eq!(
        of("bucket", 1),
        Some(10),
        "the survivors still cover every bucket"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn the_counts_are_there_to_be_read_after_a_run() {
    let _running = kinetica_adbc_tests::shared();
    use adbc_core::options::OptionStatement;
    use adbc_core::Optionable;

    let mut connection = connect();

    let mut ddl = connection.new_statement().expect("statement");
    ddl.set_sql_query(
        "CREATE OR REPLACE TABLE adbc_counts_rs AS \
         SELECT generate_series AS id FROM TABLE(GENERATE_SERIES(0, 99))",
    )
    .expect("ddl");
    ddl.execute_update().expect("create");

    let affected = OptionStatement::Other("adbc.kinetica.count_affected".into());
    let total = OptionStatement::Other("adbc.kinetica.total_number_of_records".into());

    // A query knows how big its whole result is, and changed nothing.
    let mut query = connection.new_statement().expect("statement");
    query
        .set_sql_query("SELECT id FROM adbc_counts_rs")
        .expect("query");
    query.execute().expect("execute").for_each(drop);

    assert_eq!(
        query.get_option_int(affected.clone()).expect("affected"),
        -1
    );
    assert_eq!(query.get_option_int(total.clone()).expect("total"), 100);
    assert_eq!(
        query
            .get_option_double(OptionStatement::MaxProgress)
            .expect("progress"),
        100.0
    );

    // A statement that changes rows knows how many, and has no result to size.
    let mut update = connection.new_statement().expect("statement");
    update
        .set_sql_query("DELETE FROM adbc_counts_rs WHERE id >= 60")
        .expect("query");

    assert_eq!(update.execute_update().expect("delete"), Some(40));
    assert_eq!(update.get_option_int(affected).expect("affected"), 40);
    assert_eq!(update.get_option_int(total).expect("total"), -1);

    // An unknown option is still refused rather than answered with a count.
    assert!(update
        .get_option_int(OptionStatement::Other("adbc.kinetica.nonsense".into()))
        .is_err());
}

// --------------------------------------------------------------------------
// Several callers at once, and streams that do not fit the table

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn readers_on_separate_connections_do_not_disturb_each_other() {
    let _running = kinetica_adbc_tests::shared();
    let mut connection = connect();

    let mut ddl = connection.new_statement().expect("statement");
    ddl.set_sql_query(
        "CREATE OR REPLACE TABLE adbc_conc_read AS \
         SELECT generate_series AS id FROM TABLE(GENERATE_SERIES(0, 19999))",
    )
    .expect("ddl");
    ddl.execute_update().expect("create");

    let readers: Vec<_> = (0..6)
        .map(|_| {
            std::thread::spawn(|| {
                let mut statement = connect().new_statement().expect("statement");
                statement
                    .set_sql_query("SELECT id FROM adbc_conc_read")
                    .expect("query");

                statement
                    .execute()
                    .expect("execute")
                    .map(|batch| batch.expect("batch").num_rows())
                    .sum::<usize>()
            })
        })
        .collect();

    for reader in readers {
        assert_eq!(reader.join().expect("thread"), 20_000);
    }
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn writers_on_separate_connections_all_arrive() {
    let _running = kinetica_adbc_tests::shared();
    use adbc_core::options::{OptionStatement, OptionValue};
    use adbc_core::Optionable;

    let mut connection = connect();

    let mut ddl = connection.new_statement().expect("statement");
    ddl.set_sql_query("CREATE OR REPLACE TABLE adbc_conc_write (id INT, who INT)")
        .expect("ddl");
    ddl.execute_update().expect("create");

    let writers: Vec<_> = (0..4)
        .map(|who| {
            std::thread::spawn(move || {
                use arrow_array::{Int32Array, RecordBatch};
                use arrow_schema::{DataType, Field, Schema};
                use std::sync::Arc;

                let schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, true),
                    Field::new("who", DataType::Int32, true),
                ]));

                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int32Array::from_iter_values(0..2_500)),
                        Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(
                            who, 2_500,
                        ))),
                    ],
                )
                .expect("batch");

                let mut statement = connect().new_statement().expect("statement");
                statement
                    .set_option(
                        OptionStatement::TargetTable,
                        OptionValue::String("adbc_conc_write".into()),
                    )
                    .expect("target");
                statement.bind_stream(stream_of(batch)).expect("bind");
                statement.execute_update().expect("ingest")
            })
        })
        .collect();

    for writer in writers {
        assert_eq!(writer.join().expect("thread"), Some(2_500));
    }

    let mut check = connection.new_statement().expect("statement");
    check
        .set_sql_query("SELECT COUNT(*) AS n, COUNT(DISTINCT who) AS writers FROM adbc_conc_write")
        .expect("query");

    let batches: Vec<_> = check
        .execute()
        .expect("execute")
        .map(|b| b.expect("batch"))
        .collect();
    let row = &batches[0];

    let total = row
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("n");
    let writers = row
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("writers");

    assert_eq!(
        total.value(0),
        10_000,
        "every writer's rows should be there"
    );
    assert_eq!(writers.value(0), 4, "and all four should have written");
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn reading_while_another_connection_writes_returns_whole_rows() {
    let _running = kinetica_adbc_tests::shared();
    let mut connection = connect();

    let mut ddl = connection.new_statement().expect("statement");
    ddl.set_sql_query(
        "CREATE OR REPLACE TABLE adbc_conc_mixed AS \
         SELECT generate_series AS id FROM TABLE(GENERATE_SERIES(0, 4999))",
    )
    .expect("ddl");
    ddl.execute_update().expect("create");

    let writer = std::thread::spawn(|| {
        let mut statement = connect().new_statement().expect("statement");
        statement
            .set_sql_query("INSERT INTO adbc_conc_mixed SELECT id + 100000 FROM adbc_conc_mixed")
            .expect("query");
        statement.execute_update().expect("insert")
    });

    // The reader may or may not see the new rows; what it must not do is fail,
    // or hand back a row that was half written.
    let mut statement = connect().new_statement().expect("statement");
    statement
        .set_sql_query("SELECT id FROM adbc_conc_mixed")
        .expect("query");

    let seen: usize = statement
        .execute()
        .expect("execute")
        .map(|batch| batch.expect("batch").num_rows())
        .sum();

    writer.join().expect("thread");

    assert!(
        seen >= 5_000,
        "the reader saw {seen}, fewer than were there before it started"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_stream_the_table_has_no_place_for_is_reported() {
    let _running = kinetica_adbc_tests::shared();
    use adbc_core::options::{OptionStatement, OptionValue};
    use adbc_core::Optionable;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let mut connection = connect();

    let mut ddl = connection.new_statement().expect("statement");
    ddl.set_sql_query("CREATE OR REPLACE TABLE adbc_mismatch (id INT)")
        .expect("ddl");
    ddl.execute_update().expect("create");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("nowhere", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])) as arrow_array::ArrayRef,
            Arc::new(Int32Array::from(vec![2])),
        ],
    )
    .expect("batch");

    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("adbc_mismatch".into()),
        )
        .expect("target");
    statement.bind_stream(stream_of(batch)).expect("bind");

    let failure = statement
        .execute_update()
        .expect_err("a column with nowhere to go");

    assert!(
        failure.to_string().contains("nowhere"),
        "the error should name the column: {failure}"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_table_with_columns_the_stream_omits_takes_nulls() {
    let _running = kinetica_adbc_tests::shared();
    use adbc_core::options::{OptionStatement, OptionValue};
    use adbc_core::Optionable;

    let mut connection = connect();

    let mut ddl = connection.new_statement().expect("statement");
    ddl.set_sql_query(
        "CREATE OR REPLACE TABLE adbc_partial (id INT, name VARCHAR(16), extra DOUBLE)",
    )
    .expect("ddl");
    ddl.execute_update().expect("create");

    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("adbc_partial".into()),
        )
        .expect("target");
    statement
        .bind_stream(stream_of(two_column_batch(100)))
        .expect("bind");

    assert_eq!(statement.execute_update().expect("ingest"), Some(100));

    let mut check = connection.new_statement().expect("statement");
    check
        .set_sql_query("SELECT COUNT(*) AS n, COUNT(extra) AS filled FROM adbc_partial")
        .expect("query");

    let batches: Vec<_> = check
        .execute()
        .expect("execute")
        .map(|b| b.expect("batch"))
        .collect();
    let row = &batches[0];

    assert_eq!(
        row.column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("n")
            .value(0),
        100
    );
    assert_eq!(
        row.column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("filled")
            .value(0),
        0,
        "the column the stream said nothing about should be null"
    );
}

/// The inserters are shared by every statement on a connection, so two
/// ingests into one table meet at the same cached one.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn two_ingests_on_one_connection_share_the_inserter() {
    let _running = kinetica_adbc_tests::shared();
    use adbc_core::options::{OptionStatement, OptionValue};
    use adbc_core::Optionable;
    use std::sync::{Arc, Barrier};

    let mut connection = connect();

    let mut ddl = connection.new_statement().expect("statement");
    ddl.set_sql_query("CREATE OR REPLACE TABLE adbc_shared_ins (id INT, name VARCHAR(16))")
        .expect("ddl");
    ddl.execute_update().expect("create");

    let connection = Arc::new(std::sync::Mutex::new(connection));
    let ready = Arc::new(Barrier::new(4));

    let ingests: Vec<_> = (0..4)
        .map(|_| {
            let connection = Arc::clone(&connection);
            let ready = Arc::clone(&ready);

            std::thread::spawn(move || {
                // Statements are taken one at a time; the ingests themselves
                // then run together on the connection's shared inserter.
                let mut statement = connection
                    .lock()
                    .unwrap()
                    .new_statement()
                    .expect("statement");

                statement
                    .set_option(
                        OptionStatement::TargetTable,
                        OptionValue::String("adbc_shared_ins".into()),
                    )
                    .expect("target");
                statement
                    .bind_stream(stream_of(two_column_batch(500)))
                    .expect("bind");

                ready.wait();
                statement.execute_update().expect("ingest")
            })
        })
        .collect();

    for ingest in ingests {
        assert_eq!(ingest.join().expect("thread"), Some(500));
    }

    let mut check = connection
        .lock()
        .unwrap()
        .new_statement()
        .expect("statement");
    check
        .set_sql_query("SELECT COUNT(*) AS n FROM adbc_shared_ins")
        .expect("query");

    let batches: Vec<_> = check
        .execute()
        .expect("execute")
        .map(|b| b.expect("batch"))
        .collect();

    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("n")
            .value(0),
        2_000,
        "every ingest's rows should be there"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn an_update_reports_what_it_changed_and_a_miss_reports_nothing() {
    let _running = kinetica_adbc_tests::shared();
    let mut connection = connect();

    let mut ddl = connection.new_statement().expect("statement");
    ddl.set_sql_query(
        "CREATE OR REPLACE TABLE adbc_dml_counts AS \
         SELECT generate_series AS id FROM TABLE(GENERATE_SERIES(0, 99))",
    )
    .expect("ddl");
    ddl.execute_update().expect("create");

    let run = |connection: &mut adbc_driver_kinetica::KineticaConnection, sql: &str| {
        let mut statement = connection.new_statement().expect("statement");
        statement.set_sql_query(sql).expect("query");
        statement.execute_update().expect("run")
    };

    // Many rows at once, not just the one row the other tests change.
    assert_eq!(
        run(
            &mut connection,
            "UPDATE adbc_dml_counts SET id = id + 1000 WHERE id < 40"
        ),
        Some(40)
    );

    // Matching nothing is not a failure, and changes nothing.
    assert_eq!(
        run(
            &mut connection,
            "UPDATE adbc_dml_counts SET id = 0 WHERE id = 999999"
        ),
        Some(0)
    );
    assert_eq!(
        run(
            &mut connection,
            "DELETE FROM adbc_dml_counts WHERE id = 999999"
        ),
        Some(0)
    );

    assert_eq!(
        run(
            &mut connection,
            "DELETE FROM adbc_dml_counts WHERE id >= 1000"
        ),
        Some(40)
    );

    let mut check = connection.new_statement().expect("statement");
    check
        .set_sql_query("SELECT COUNT(*) AS n FROM adbc_dml_counts")
        .expect("query");

    let batches: Vec<_> = check
        .execute()
        .expect("execute")
        .map(|b| b.expect("batch"))
        .collect();

    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("n")
            .value(0),
        60
    );
}

// --------------------------------------------------------------------------
// One result, read in pages

/// Pages come from one materialized result, so a table changing under the
/// reader cannot move rows between the offsets it is walking.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_paged_read_sees_the_result_it_started_with() {
    let _running = kinetica_adbc_tests::shared();
    use adbc_core::options::{OptionStatement, OptionValue};
    use adbc_core::Optionable;

    let mut connection = connect();

    let mut ddl = connection.new_statement().expect("statement");
    ddl.set_sql_query(
        "CREATE OR REPLACE TABLE adbc_pinned AS \
         SELECT generate_series AS id FROM TABLE(GENERATE_SERIES(0, 19999))",
    )
    .expect("ddl");
    ddl.execute_update().expect("create");

    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_option(
            OptionStatement::Other("adbc.kinetica.batch_size".into()),
            OptionValue::Int(5_000),
        )
        .expect("batch size");
    // A join, since a single-table query is still answered without a result
    // being kept, which is a server-side matter rather than a driver one.
    statement
        .set_sql_query(
            "SELECT a.id FROM adbc_pinned a JOIN adbc_pinned b ON a.id = b.id ORDER BY a.id",
        )
        .expect("query");

    let mut reader = statement.execute().expect("execute");

    // The first page is already read; now take the ground away from the rest.
    let first = reader.next().expect("a first page").expect("batch");
    assert_eq!(first.num_rows(), 5_000);

    let mut meddler = connect().new_statement().expect("statement");
    meddler
        .set_sql_query("DELETE FROM adbc_pinned WHERE id < 5000")
        .expect("query");
    meddler.execute_update().expect("delete");

    let rest: usize = reader.map(|b| b.expect("batch").num_rows()).sum();

    assert_eq!(
        first.num_rows() + rest,
        20_000,
        "the read should finish the result it started, not the table as it now is"
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_finished_read_lets_go_of_what_the_server_kept() {
    // Counts this process's paging tables, so nothing beside it may make one
    let _running = kinetica_adbc_tests::exclusive();
    let mut connection = connect();

    let mut ddl = connection.new_statement().expect("statement");
    ddl.set_sql_query(
        "CREATE OR REPLACE TABLE adbc_kept AS \
         SELECT generate_series AS id FROM TABLE(GENERATE_SERIES(0, 999))",
    )
    .expect("ddl");
    ddl.execute_update().expect("create");

    let before = kinetica_adbc_tests::paging_tables_held();

    {
        let mut statement = connection.new_statement().expect("statement");
        statement
            .set_sql_query(
                "SELECT a.id FROM adbc_kept a JOIN adbc_kept b ON a.id = b.id ORDER BY a.id",
            )
            .expect("query");

        // Small pages, so the result is still being held when it is counted.
        use adbc_core::Optionable as _;

        statement
            .set_option(
                adbc_core::options::OptionStatement::Other("adbc.kinetica.batch_size".into()),
                OptionValue::Int(100),
            )
            .expect("batch size");

        let mut reader = statement.execute().expect("execute");
        let mut rows = reader
            .next()
            .expect("a first batch")
            .expect("batch")
            .num_rows();

        // Without this the test would pass on a query that never made a
        // paging table at all, which is how it used to pass.
        assert!(
            kinetica_adbc_tests::paging_tables_held() > before,
            "the join left no paging table, so there is nothing to clean up"
        );

        rows += reader.map(|b| b.expect("batch").num_rows()).sum::<usize>();

        assert_eq!(rows, 1_000);
    }

    assert_eq!(
        kinetica_adbc_tests::paging_tables_held(),
        before,
        "the result should not outlive the reader that asked for it"
    );
}

/// Each kept inserter holds a queue per worker and a flush of its own, so a
/// connection loading many tables must not keep one for every table it saw.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_connection_does_not_keep_an_inserter_for_every_table_it_ever_saw() {
    let _running = kinetica_adbc_tests::shared();
    use adbc_core::options::{OptionStatement, OptionValue};
    use adbc_core::Optionable;

    let mut connection = connect();

    // More tables than the cache holds, each written to twice: the second
    // round finds the early ones evicted and builds them again.
    for round in 0..2 {
        for table in 0..20 {
            let name = format!("adbc_many_{table}");

            if round == 0 {
                let mut ddl = connection.new_statement().expect("statement");
                ddl.set_sql_query(format!(
                    "CREATE OR REPLACE TABLE {name} (id INT, name VARCHAR(16))"
                ))
                .expect("ddl");
                ddl.execute_update().expect("create");
            }

            let mut statement = connection.new_statement().expect("statement");
            statement
                .set_option(
                    OptionStatement::TargetTable,
                    OptionValue::String(name.clone()),
                )
                .expect("target");
            statement
                .bind_stream(stream_of(two_column_batch(10)))
                .expect("bind");

            assert_eq!(statement.execute_update().expect("ingest"), Some(10));
        }
    }

    // Every row of both rounds arrived, evictions and all.
    let mut check = connection.new_statement().expect("statement");
    check
        .set_sql_query("SELECT COUNT(*) AS n FROM adbc_many_0")
        .expect("query");

    let batches: Vec<_> = check
        .execute()
        .expect("execute")
        .map(|b| b.expect("batch"))
        .collect();

    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("n")
            .value(0),
        20,
        "a table whose inserter was evicted and rebuilt should still have both rounds"
    );
}

/// @return The first column of every batch the reader hands back, as integers
fn first_column(
    reader: impl Iterator<Item = Result<arrow_array::RecordBatch, arrow_schema::ArrowError>>,
) -> Vec<i64> {
    reader
        .flat_map(|batch| {
            let batch = batch.expect("batch");
            let column = arrow_cast::cast(batch.column(0), &arrow_schema::DataType::Int64)
                .expect("an integer column");
            column
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .expect("Int64")
                .values()
                .to_vec()
        })
        .collect()
}

/// @return One column of parameters, a row for each value
fn parameter_rows(values: &[i64]) -> arrow_array::RecordBatch {
    use arrow_schema::{DataType, Field, Schema};

    arrow_array::RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![Field::new("0", DataType::Int64, false)])),
        vec![std::sync::Arc::new(arrow_array::Int64Array::from(
            values.to_vec(),
        ))],
    )
    .expect("parameters")
}

/// ADBC runs a query bound to several rows once for each, and reads back each
/// run's rows in the order the rows were bound.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_query_bound_to_several_rows_runs_once_for_each() {
    let _running = kinetica_adbc_tests::shared();
    let mut connection = connect();

    let mut statement = connection.new_statement().expect("statement");
    statement.set_sql_query("SELECT 1 + ? AS n").expect("query");
    statement.bind(parameter_rows(&[1, 2, 3, 4])).expect("bind");

    assert_eq!(
        first_column(statement.execute().expect("execute")),
        vec![2, 3, 4, 5]
    );
}

/// Each run of a query bound to several rows pages through a result of its
/// own, and lets go of it before the next is made; the total is the whole
/// read's.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn several_rows_page_through_each_result_and_let_go_of_all_of_them() {
    use adbc_core::options::OptionStatement;
    use adbc_core::Optionable as _;

    // Counts this process's paging tables, so nothing beside it may make one
    let _running = kinetica_adbc_tests::exclusive();
    let mut connection = connect();
    let before = kinetica_adbc_tests::paging_tables_held();

    {
        let mut statement = connection.new_statement().expect("statement");
        statement
            .set_sql_query("SELECT id FROM adbc_pg WHERE grp = ? ORDER BY id")
            .expect("query");

        // Smaller than one group, so every run takes more than one page.
        statement
            .set_option(
                OptionStatement::Other("adbc.kinetica.batch_size".into()),
                OptionValue::Int(5_000),
            )
            .expect("batch size");
        statement.bind(parameter_rows(&[3, 1, 5])).expect("bind");

        let ids = first_column(statement.execute().expect("execute"));

        // adbc_pg holds 1 to 50001, 7143 in each of the seven groups.
        assert_eq!(ids.len(), 3 * 7_143);
        assert!(ids[..7_143].iter().all(|id| id % 7 == 3), "group 3 first");
        assert!(
            ids[7_143..14_286].iter().all(|id| id % 7 == 1),
            "then group 1"
        );
        assert!(ids[14_286..].iter().all(|id| id % 7 == 5), "then group 5");
        assert!(
            ids[..7_143].windows(2).all(|pair| pair[0] < pair[1]),
            "each run's rows in its own order, across its pages"
        );

        assert_eq!(
            statement
                .get_option_int(OptionStatement::Other(
                    "adbc.kinetica.total_number_of_records".into()
                ))
                .expect("total"),
            3 * 7_143,
            "the total should be the whole read's, not the last run's"
        );
    }

    assert_eq!(
        kinetica_adbc_tests::paging_tables_held(),
        before,
        "no run's result should outlive the reader"
    );
}

/// A row cap is the read's, not each run's: it can end the read part way
/// through a run, and the runs past it are never made.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_row_cap_spans_the_runs_and_stops_them() {
    use adbc_core::options::OptionStatement;
    use adbc_core::Optionable as _;

    let _running = kinetica_adbc_tests::shared();
    let mut connection = connect();

    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_sql_query("SELECT id FROM adbc_pg WHERE grp = ? ORDER BY id")
        .expect("query");
    statement
        .set_option(
            OptionStatement::Other("adbc.kinetica.max_rows".into()),
            OptionValue::Int(10_000),
        )
        .expect("max rows");
    statement.bind(parameter_rows(&[0, 1, 2])).expect("bind");

    let ids = first_column(statement.execute().expect("execute"));

    assert_eq!(ids.len(), 10_000);
    assert!(ids[7_143..].iter().all(|id| id % 7 == 1));

    // The third run was never made, so its rows are not in the total.
    assert_eq!(
        statement
            .get_option_int(OptionStatement::Other(
                "adbc.kinetica.total_number_of_records".into()
            ))
            .expect("total"),
        2 * 7_143
    );
}

/// What a query bound to several rows cannot do is refused, saying why.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_query_bound_to_no_rows_or_partitioned_over_several_is_refused() {
    use adbc_core::error::Status;

    let _running = kinetica_adbc_tests::shared();
    let mut connection = connect();

    let mut statement = connection.new_statement().expect("statement");
    statement.set_sql_query("SELECT 1 + ? AS n").expect("query");

    statement.bind(parameter_rows(&[])).expect("bind");
    let error = statement.execute().err().expect("no rows refused");
    assert_eq!(error.status, Status::InvalidArguments, "{}", error.message);

    statement.bind(parameter_rows(&[1, 2])).expect("bind");
    let error = statement
        .execute_partitions()
        .expect_err("partitions refused");
    assert_eq!(error.status, Status::NotImplemented, "{}", error.message);
}
