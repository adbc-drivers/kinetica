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

//! Every column type Kinetica stores, read back through the driver. The
//! server's Arrow encoding is covered by a test in gpudb-core; this is the
//! other half, that arrow-rs decodes what the server sends into the type and
//! the value a caller expects, and that a null survives the trip.
//!
//! The instance and its login come from KI_URL, KI_USER and KI_PWD.

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::sync::{Connection, Database, Driver, Statement};
use adbc_driver_kinetica::KineticaDriver;
use arrow_array::Array;

// Every test here rebuilds this one table, and some count the process's paging
// tables, so each holds `exclusive()` and they run one at a time.
const TABLE: &str = "adbc_all_types";

/// A column, how it is declared, what goes in it, and what must come back.
struct Column {
    name: &'static str,
    sql_type: &'static str,
    literal: &'static str,
    arrow_type: &'static str,
    value: &'static str,
}

const fn column(
    name: &'static str,
    sql_type: &'static str,
    literal: &'static str,
    arrow_type: &'static str,
    value: &'static str,
) -> Column {
    Column {
        name,
        sql_type,
        literal,
        arrow_type,
        value,
    }
}

/// Kinetica's storable column types. A type added to the server belongs here
/// the day it appears, with the Arrow type it is meant to arrive as.
const COLUMNS: &[Column] = &[
    column("c_int", "INTEGER", "7", "Int32", "7"),
    column("c_int8", "INT8", "3", "Int8", "3"),
    column("c_int16", "INT16", "300", "Int16", "300"),
    column(
        "c_long",
        "BIGINT",
        "1234567890123",
        "Int64",
        "1234567890123",
    ),
    // Must not degrade to a string on the way through.
    column(
        "c_ulong",
        "UNSIGNED BIGINT",
        "18446744073709551610",
        "UInt64",
        "18446744073709551610",
    ),
    column("c_float", "REAL", "1.25", "Float32", "1.25"),
    column("c_double", "DOUBLE", "2.5", "Float64", "2.5"),
    column("c_string", "VARCHAR", "'hello'", "Utf8", "hello"),
    // Stored little-endian, so the bytes run opposite to the text; arriving
    // as readable text is the thing being checked.
    column("c_char1", "VARCHAR(1)", "'x'", "Utf8", "x"),
    column("c_char8", "VARCHAR(8)", "'abcdefgh'", "Utf8", "abcdefgh"),
    column(
        "c_char256",
        "VARCHAR(256)",
        "'a longer value'",
        "Utf8",
        "a longer value",
    ),
    column("c_date", "DATE", "'2026-10-14'", "Date32", "2026-10-14"),
    column(
        "c_time",
        "TIME",
        "'12:34:56.789'",
        "Time32(ms)",
        "12:34:56.789",
    ),
    // DATETIME is a wall-clock reading and carries no zone; TIMESTAMP below is
    // an instant and is tagged UTC. That tag is the only thing separating them.
    column(
        "c_datetime",
        "DATETIME",
        "'2026-10-14 12:34:56.789'",
        "Timestamp(ms)",
        "2026-10-14T12:34:56.789",
    ),
    column(
        "c_ts",
        "TIMESTAMP",
        "'2023-11-14 22:13:20.123'",
        "Timestamp(ms, \"UTC\")",
        // Checked by its epoch value below: arrow-cast renders a zoned
        // timestamp as empty here, which would make a string check vacuous.
        "",
    ),
    column(
        "c_decimal",
        "DECIMAL(18,4)",
        "12345.6789",
        "Decimal128(18, 4)",
        "12345.6789",
    ),
    column("c_ipv4", "IPV4", "'10.1.2.3'", "Utf8", "10.1.2.3"),
    // arrow-rs has no registered uuid extension, so the storage type surfaces.
    column(
        "c_uuid",
        "UUID",
        "'12345678-1234-5678-1234-567812345678'",
        "FixedSizeBinary(16)",
        "12345678123456781234567812345678",
    ),
    // WKB, not the WKT that Parquet export writes: little-endian byte order,
    // type 1 (point), then the two coordinates.
    column(
        "c_wkt",
        "GEOMETRY",
        "'POINT(1 2)'",
        "Binary",
        "0101000000000000000000f03f0000000000000040",
    ),
    column("c_bytes", "VARBINARY", "X'010203'", "Binary", "010203"),
    column("c_json", "JSON", "'{\"a\":1}'", "Utf8", "{\"a\":1}"),
    // Rank-0 holds only the stringified form, so an array travels as text.
    column("c_array", "INT[]", "ARRAY[1,2,3]", "Utf8", "[1,2,3]"),
    column(
        "c_vector",
        "VECTOR(3)",
        "'[1.5,2.5,3.5]'",
        "FixedSizeList(3 x Float32)",
        "[1.5, 2.5, 3.5]",
    ),
];

/// Renders one value for comparison. arrow-cast cannot format a timestamp
/// carrying a named zone without chrono-tz, which the arrow-cast this
/// resolves to does not offer, so that case falls back to the raw array; it
/// compares just as well because both sides come through here.
/// @return A string standing for the value
fn value_of(column: &arrow_array::ArrayRef, row: usize) -> String {
    match arrow_cast::display::array_value_to_string(column, row) {
        Ok(text) if !text.is_empty() => text,
        _ => format!("{:?}", column.slice(row, 1)),
    }
}

fn connect() -> adbc_driver_kinetica::KineticaConnection {
    let kinetica_adbc_tests::Login {
        url,
        user,
        password,
    } = kinetica_adbc_tests::login();

    let mut driver = KineticaDriver::default();

    driver
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(url)),
            (OptionDatabase::Username, OptionValue::String(user)),
            (OptionDatabase::Password, OptionValue::String(password)),
        ])
        .expect("database")
        .new_connection()
        .expect("connection")
}

fn run(connection: &mut adbc_driver_kinetica::KineticaConnection, sql: &str) {
    let mut statement = connection.new_statement().expect("statement");
    statement.set_sql_query(sql).expect("query");
    statement.execute_update().expect("run");
}

/// Builds the table: one row holding a value of every type, one row of nulls.
fn fixture(connection: &mut adbc_driver_kinetica::KineticaConnection) {
    let definitions: Vec<String> = COLUMNS
        .iter()
        .map(|c| format!("{} {}", c.name, c.sql_type))
        .collect();
    let values: Vec<&str> = COLUMNS.iter().map(|c| c.literal).collect();
    let nulls: Vec<&str> = COLUMNS.iter().map(|_| "NULL").collect();

    run(connection, &format!("DROP TABLE IF EXISTS {TABLE}"));
    run(
        connection,
        &format!(
            "CREATE TABLE {TABLE} (row_id INTEGER, {})",
            definitions.join(", ")
        ),
    );
    run(
        connection,
        &format!("INSERT INTO {TABLE} VALUES (1, {})", values.join(", ")),
    );
    run(
        connection,
        &format!("INSERT INTO {TABLE} VALUES (2, {})", nulls.join(", ")),
    );
}

/// Reads the fixture back, the populated row first.
/// @return The one batch the result fits in
fn read_all(connection: &mut adbc_driver_kinetica::KineticaConnection) -> arrow_array::RecordBatch {
    let names: Vec<&str> = COLUMNS.iter().map(|c| c.name).collect();

    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_sql_query(format!(
            "SELECT {} FROM {TABLE} ORDER BY row_id",
            names.join(", ")
        ))
        .expect("query");

    let batches: Vec<_> = statement
        .execute()
        .expect("execute")
        .map(|b| b.expect("batch"))
        .collect();

    assert_eq!(batches.len(), 1, "the fixture should fit in one batch");

    batches.into_iter().next().unwrap()
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn every_type_arrives_as_the_documented_arrow_type_and_value() {
    let _running = kinetica_adbc_tests::exclusive();
    let mut connection = connect();
    fixture(&mut connection);

    let batch = read_all(&mut connection);
    let schema = batch.schema();

    assert_eq!(batch.num_columns(), COLUMNS.len());
    assert_eq!(batch.num_rows(), 2);

    for (index, expected) in COLUMNS.iter().enumerate() {
        let field = schema.field(index);
        let column = batch.column(index);
        let name = expected.name;

        assert_eq!(field.name(), name);

        assert_eq!(
            field.data_type().to_string(),
            expected.arrow_type,
            "{name} changed Arrow type"
        );

        if !expected.value.is_empty() {
            assert_eq!(value_of(column, 0), expected.value, "{name} changed value");
        }

        // The Kinetica type travels in metadata, so a consumer can recreate a
        // column Arrow has no name for.
        assert!(
            field.metadata().contains_key("KINETICA:type"),
            "{name} lost its KINETICA:type metadata"
        );

        // Row two is all nulls, so every type carries a validity bitmap the
        // decoder agrees with.
        assert!(
            !column.is_null(0),
            "{name} read back null when it had a value"
        );
        assert!(column.is_null(1), "{name} did not read back null");
    }

    run(&mut connection, &format!("DROP TABLE IF EXISTS {TABLE}"));
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_timestamp_is_an_instant_and_a_datetime_is_not() {
    let _running = kinetica_adbc_tests::exclusive();
    let mut connection = connect();
    fixture(&mut connection);

    let batch = read_all(&mut connection);
    let schema = batch.schema();

    let index = |name: &str| COLUMNS.iter().position(|c| c.name == name).expect(name);

    let timestamp = index("c_ts");
    let datetime = index("c_datetime");

    // Same storage, and the zone is what tells a reader how to interpret it.
    match schema.field(timestamp).data_type() {
        arrow_schema::DataType::Timestamp(unit, zone) => {
            assert_eq!(*unit, arrow_schema::TimeUnit::Millisecond);
            assert_eq!(zone.as_deref(), Some("UTC"));
        }
        other => panic!("c_ts is {other}"),
    }

    match schema.field(datetime).data_type() {
        arrow_schema::DataType::Timestamp(unit, zone) => {
            assert_eq!(*unit, arrow_schema::TimeUnit::Millisecond);
            assert_eq!(zone.as_deref(), None, "a datetime must not claim a zone");
        }
        other => panic!("c_datetime is {other}"),
    }

    // The epoch value, which the zoned column's rendering does not show.
    let values = batch
        .column(timestamp)
        .as_any()
        .downcast_ref::<arrow_array::TimestampMillisecondArray>()
        .expect("timestamp");

    assert_eq!(values.value(0), 1_700_000_000_123);

    run(&mut connection, &format!("DROP TABLE IF EXISTS {TABLE}"));
}

// --------------------------------------------------------------------------
// Ingest. The read path above says what comes out; this says the same values
// go back in. Rather than inventing Arrow input and guessing what the driver
// will accept, it feeds a batch read from one table into an identical one,
// so the assertion is that a round trip changes nothing.
// --------------------------------------------------------------------------

const INGEST_TABLE: &str = "adbc_all_types_ingest";

/// Creates an empty table with the same columns as the fixture.
fn empty_twin(connection: &mut adbc_driver_kinetica::KineticaConnection) {
    let definitions: Vec<String> = COLUMNS
        .iter()
        .map(|c| format!("{} {}", c.name, c.sql_type))
        .collect();

    run(connection, &format!("DROP TABLE IF EXISTS {INGEST_TABLE}"));
    run(
        connection,
        &format!("CREATE TABLE {INGEST_TABLE} ({})", definitions.join(", ")),
    );
}

/// Reads the named columns of a table, the populated row first.
/// @return The one batch the result fits in
fn read_columns(
    connection: &mut adbc_driver_kinetica::KineticaConnection,
    table: &str,
    names: &[&str],
    order_by: &str,
) -> arrow_array::RecordBatch {
    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_sql_query(format!(
            "SELECT {} FROM {table} ORDER BY {order_by} NULLS LAST",
            names.join(", ")
        ))
        .expect("query");

    let batches: Vec<_> = statement
        .execute()
        .expect("execute")
        .map(|b| b.expect("batch"))
        .collect();

    assert_eq!(batches.len(), 1, "the fixture should fit in one batch");

    batches.into_iter().next().unwrap()
}

/// Sends a batch to a table, as a client handing over Arrow does.
/// @return What the server reported writing
fn ingest(
    connection: &mut adbc_driver_kinetica::KineticaConnection,
    table: &str,
    batch: arrow_array::RecordBatch,
) -> adbc_core::error::Result<Option<i64>> {
    use adbc_core::options::OptionStatement;
    use adbc_core::Optionable;

    let schema = batch.schema();
    let stream: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
        arrow_array::RecordBatchIterator::new(vec![Ok(batch)], schema),
    );

    let mut statement = connection.new_statement().expect("statement");
    statement.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(table.into()),
    )?;
    statement.set_option(
        OptionStatement::IngestMode,
        OptionValue::String("adbc.ingest.mode.append".into()),
    )?;
    statement.bind_stream(stream)?;
    statement.execute_update()
}

// --------------------------------------------------------------------------
// Ingest. The tests above say what comes out of the server; these say the same
// values go back in. Rather than inventing Arrow input and guessing what the
// driver accepts, a batch read from one table is fed into an identical one, so
// the assertion is that a round trip changes nothing.
// --------------------------------------------------------------------------

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn every_type_survives_an_arrow_ingest() {
    let _running = kinetica_adbc_tests::exclusive();
    let mut connection = connect();
    fixture(&mut connection);
    empty_twin(&mut connection);

    let names: Vec<&str> = COLUMNS.iter().map(|c| c.name).collect();

    let source = read_columns(&mut connection, TABLE, &names, "c_int");

    assert_eq!(
        ingest(&mut connection, INGEST_TABLE, source.clone()).expect("ingest"),
        Some(2),
        "both rows should have been written"
    );

    let landed = read_columns(&mut connection, INGEST_TABLE, &names, "c_int");

    assert_eq!(landed.num_columns(), source.num_columns());
    assert_eq!(landed.num_rows(), source.num_rows());

    for (index, expected) in COLUMNS.iter().enumerate() {
        let name = expected.name;
        let before = source.column(index);
        let after = landed.column(index);

        assert_eq!(
            after.data_type().to_string(),
            expected.arrow_type,
            "{name} came back as a different type after ingest"
        );

        assert_eq!(
            value_of(after, 0),
            value_of(before, 0),
            "{name} changed value through ingest"
        );

        // The null row has to survive as a null, not as a zero or an empty
        // string, which is what a lost validity bitmap would turn it into.
        assert!(after.is_null(1), "{name} lost its null through ingest");
    }

    run(
        &mut connection,
        &format!("DROP TABLE IF EXISTS {INGEST_TABLE}"),
    );
    run(&mut connection, &format!("DROP TABLE IF EXISTS {TABLE}"));
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_geometry_round_trips_into_a_text_column() {
    let _running = kinetica_adbc_tests::exclusive();
    let mut connection = connect();
    fixture(&mut connection);

    run(&mut connection, "DROP TABLE IF EXISTS adbc_wkt_ingest");
    run(
        &mut connection,
        "CREATE TABLE adbc_wkt_ingest (row_id INTEGER, c_wkt GEOMETRY)",
    );

    let source = read_columns(&mut connection, TABLE, &["row_id", "c_wkt"], "row_id");

    assert_eq!(
        ingest(&mut connection, "adbc_wkt_ingest", source.clone()).expect("ingest"),
        Some(2)
    );

    let landed = read_columns(
        &mut connection,
        "adbc_wkt_ingest",
        &["row_id", "c_wkt"],
        "row_id",
    );

    // The server sends WKB whatever the column is declared as, so getting the
    // same bytes back means the WKB -> WKT -> WKB trip lost nothing.
    assert_eq!(
        value_of(landed.column(1), 0),
        value_of(source.column(1), 0),
        "geometry changed through ingest"
    );
    assert!(landed.column(1).is_null(1), "geometry lost its null");

    run(&mut connection, "DROP TABLE IF EXISTS adbc_wkt_ingest");
    run(&mut connection, &format!("DROP TABLE IF EXISTS {TABLE}"));
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_vector_round_trips_through_ingest() {
    let _running = kinetica_adbc_tests::exclusive();
    let mut connection = connect();
    fixture(&mut connection);

    run(&mut connection, "DROP TABLE IF EXISTS adbc_vector_ingest");
    run(
        &mut connection,
        "CREATE TABLE adbc_vector_ingest (row_id INTEGER, c_vector VECTOR(3))",
    );

    let source = read_columns(&mut connection, TABLE, &["row_id", "c_vector"], "row_id");

    assert_eq!(
        ingest(&mut connection, "adbc_vector_ingest", source.clone()).expect("ingest"),
        Some(2)
    );

    let landed = read_columns(
        &mut connection,
        "adbc_vector_ingest",
        &["row_id", "c_vector"],
        "row_id",
    );

    assert_eq!(
        value_of(landed.column(1), 0),
        value_of(source.column(1), 0),
        "vector changed through ingest"
    );
    assert!(landed.column(1).is_null(1), "vector lost its null");

    run(&mut connection, "DROP TABLE IF EXISTS adbc_vector_ingest");
    run(&mut connection, &format!("DROP TABLE IF EXISTS {TABLE}"));
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_conversion_that_cannot_be_made_is_reported_rather_than_nulled() {
    let _running = kinetica_adbc_tests::exclusive();
    let mut connection = connect();

    run(&mut connection, "DROP TABLE IF EXISTS adbc_strict_cast");
    run(
        &mut connection,
        "CREATE TABLE adbc_strict_cast (row_id INTEGER, n INTEGER)",
    );

    // Text that is not a number, into an integer column. Under the default
    // (safe) cast options this wrote a null and reported success.
    let batch = arrow_array::RecordBatch::try_from_iter(vec![
        (
            "row_id",
            std::sync::Arc::new(arrow_array::Int32Array::from(vec![1])) as arrow_array::ArrayRef,
        ),
        (
            "n",
            std::sync::Arc::new(arrow_array::StringArray::from(vec!["not a number"]))
                as arrow_array::ArrayRef,
        ),
    ])
    .expect("batch");

    let failure = ingest(&mut connection, "adbc_strict_cast", batch)
        .expect_err("an impossible conversion should be reported");

    assert!(
        failure.message.to_lowercase().contains("cast"),
        "expected a cast error, got: {}",
        failure.message
    );

    run(&mut connection, "DROP TABLE IF EXISTS adbc_strict_cast");
}

// --------------------------------------------------------------------------
// Partitions. A consumer that distributes work asks for the result in pieces
// and reads each one on its own connection, so what matters is that the
// pieces together are the whole result exactly once, that every type survives
// being read that way, and that the server does not keep holding the result
// afterwards.
// --------------------------------------------------------------------------

const PARTITION_TABLE: &str = "adbc_partition_types";

/// How many copies of the fixture the partitioned table holds. Two source
/// rows each, so the table is twice this, which is enough to come back in
/// several pieces.
const COPIES: i64 = 1_500;

/// Builds a table of every type, large enough to be partitioned, with a
/// unique row_id and the fixture's null row repeated through it.
fn partition_fixture(connection: &mut adbc_driver_kinetica::KineticaConnection) {
    let names: Vec<String> = COLUMNS.iter().map(|c| format!("t.{}", c.name)).collect();

    fixture(connection);

    run(
        connection,
        &format!("DROP TABLE IF EXISTS {PARTITION_TABLE}"),
    );
    run(
        connection,
        &format!(
            "CREATE TABLE {PARTITION_TABLE} AS SELECT \
             CAST((gs.generate_series - 1) * 2 + t.row_id AS INTEGER) AS row_id, {} \
             FROM {TABLE} t, TABLE(GENERATE_SERIES(1, {COPIES})) gs",
            names.join(", ")
        ),
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn partitions_return_every_row_of_every_type_exactly_once() {
    let _running = kinetica_adbc_tests::exclusive();
    use adbc_core::options::OptionStatement;
    use adbc_core::Optionable;

    let mut connection = connect();
    partition_fixture(&mut connection);

    let names: Vec<&str> = COLUMNS.iter().map(|c| c.name).collect();

    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_option(
            OptionStatement::Other("kinetica.query.batch_size".into()),
            OptionValue::Int(500),
        )
        .expect("batch size");
    statement
        .set_sql_query(format!(
            "SELECT row_id, {} FROM {PARTITION_TABLE}",
            names.join(", ")
        ))
        .expect("query");

    let result = statement.execute_partitions().expect("partitions");

    assert!(
        result.partitions.len() > 1,
        "the result should have come back in pieces, got {}",
        result.partitions.len()
    );

    let mut seen: Vec<i32> = Vec::new();
    let mut values_seen = 0usize;
    let mut nulls_seen = 0usize;

    // Each piece on its own connection, as a consumer distributing them would.
    for piece in &result.partitions {
        let reader = connect().read_partition(piece).expect("read");

        for batch in reader {
            let batch = batch.expect("batch");

            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .expect("row_id");

            for row in 0..batch.num_rows() {
                seen.push(ids.value(row));

                // A row is either the fixture's value row or its null row, and
                // every column has to agree about which, or a piece has torn
                // one row's columns apart.
                let odd = ids.value(row) % 2 == 1;

                for (index, expected) in COLUMNS.iter().enumerate() {
                    let column = batch.column(index + 1);
                    let name = expected.name;

                    if odd {
                        assert!(!column.is_null(row), "{name} null in a value row");

                        if !expected.value.is_empty() {
                            assert_eq!(
                                value_of(column, row),
                                expected.value,
                                "{name} changed value through a partition"
                            );
                        }
                    } else {
                        assert!(column.is_null(row), "{name} not null in a null row");
                    }
                }

                if odd {
                    values_seen += 1;
                } else {
                    nulls_seen += 1;
                }
            }
        }
    }

    let total = (COPIES * 2) as usize;

    assert_eq!(seen.len(), total, "a row was lost or repeated");
    assert_eq!(values_seen, total / 2);
    assert_eq!(nulls_seen, total / 2);

    seen.sort_unstable();
    seen.dedup();

    assert_eq!(seen.len(), total, "a row_id came back twice");
    assert_eq!(seen.first(), Some(&1));
    assert_eq!(seen.last(), Some(&(total as i32)));

    run(
        &mut connection,
        &format!("DROP TABLE IF EXISTS {PARTITION_TABLE}"),
    );
    run(&mut connection, &format!("DROP TABLE IF EXISTS {TABLE}"));
}

/// @return A join, so the server materializes a result and keeps a paging
///         table for it; a single-table query is answered without one
fn materializing_query() -> String {
    format!(
        "SELECT a.row_id FROM {PARTITION_TABLE} a \
         JOIN {PARTITION_TABLE} b ON a.row_id = b.row_id"
    )
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_partitioned_read_keeps_a_paging_table_and_then_lets_it_go() {
    let _running = kinetica_adbc_tests::exclusive();
    use adbc_core::options::OptionStatement;
    use adbc_core::Optionable;
    use kinetica_adbc_tests::paging_tables_held;

    let mut connection = connect();
    partition_fixture(&mut connection);

    let before = paging_tables_held();
    let total = (COPIES * 2) as usize;

    {
        let mut statement = connection.new_statement().expect("statement");
        statement
            .set_option(
                OptionStatement::Other("kinetica.query.batch_size".into()),
                OptionValue::Int(500),
            )
            .expect("batch size");
        statement
            .set_sql_query(materializing_query())
            .expect("query");

        let result = statement.execute_partitions().expect("partitions");

        // Without this the rest would pass on a query that never made a
        // paging table at all, which is how this used to pass.
        assert!(
            paging_tables_held() > before,
            "the join left no paging table, so there is nothing to clean up"
        );

        let mut rows = 0usize;
        for piece in &result.partitions {
            let reader = connect().read_partition(piece).expect("read");
            rows += reader.map(|b| b.expect("batch").num_rows()).sum::<usize>();
        }

        assert_eq!(rows, total);
    }

    // The statement has gone, and the result it was holding with it.
    assert_eq!(
        paging_tables_held(),
        before,
        "a partitioned read left its result behind on the server"
    );

    run(
        &mut connection,
        &format!("DROP TABLE IF EXISTS {PARTITION_TABLE}"),
    );
    run(&mut connection, &format!("DROP TABLE IF EXISTS {TABLE}"));
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn partitions_nobody_reads_are_let_go_of_too() {
    let _running = kinetica_adbc_tests::exclusive();
    use adbc_core::options::OptionStatement;
    use adbc_core::Optionable;
    use kinetica_adbc_tests::paging_tables_held;

    let mut connection = connect();
    partition_fixture(&mut connection);

    let before = paging_tables_held();

    {
        let mut statement = connection.new_statement().expect("statement");
        statement
            .set_option(
                OptionStatement::Other("kinetica.query.batch_size".into()),
                OptionValue::Int(500),
            )
            .expect("batch size");
        statement
            .set_sql_query(materializing_query())
            .expect("query");

        let result = statement.execute_partitions().expect("partitions");

        assert!(
            paging_tables_held() > before,
            "the join left no paging table, so there is nothing to clean up"
        );

        // Only the first piece; a consumer that fails partway, or decides it
        // has seen enough, leaves the rest untouched.
        let reader = connect()
            .read_partition(&result.partitions[0])
            .expect("read");
        let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();

        assert!(rows > 0);
    }

    assert_eq!(
        paging_tables_held(),
        before,
        "abandoning the remaining partitions left the result on the server"
    );

    run(
        &mut connection,
        &format!("DROP TABLE IF EXISTS {PARTITION_TABLE}"),
    );
    run(&mut connection, &format!("DROP TABLE IF EXISTS {TABLE}"));
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_partition_read_after_its_statement_is_gone_is_refused() {
    let _running = kinetica_adbc_tests::exclusive();
    use adbc_core::error::Status;
    use adbc_core::options::OptionStatement;
    use adbc_core::Optionable;
    use kinetica_adbc_tests::paging_tables_held;

    let mut connection = connect();
    partition_fixture(&mut connection);

    let before = paging_tables_held();

    let partitions = {
        let mut statement = connection.new_statement().expect("statement");
        statement
            .set_option(
                OptionStatement::Other("kinetica.query.batch_size".into()),
                OptionValue::Int(500),
            )
            .expect("batch size");
        statement
            .set_sql_query(materializing_query())
            .expect("query");

        statement
            .execute_partitions()
            .expect("partitions")
            .partitions
    };

    // The statement has let go of the result. Asked for it by name, the server
    // runs the query again, which is a different snapshot from the one the
    // partitions were cut from.
    let refused = match connect().read_partition(&partitions[0]) {
        Err(refused) => refused,
        Ok(_) => panic!("a partition whose result is gone should not be read"),
    };

    assert_eq!(refused.status, Status::InvalidState, "{refused}");
    assert_eq!(
        paging_tables_held(),
        before,
        "the run a stale partition caused was left on the server"
    );

    run(
        &mut connection,
        &format!("DROP TABLE IF EXISTS {PARTITION_TABLE}"),
    );
    run(&mut connection, &format!("DROP TABLE IF EXISTS {TABLE}"));
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn partitions_of_a_materialized_query_see_the_result_they_started_with() {
    let _running = kinetica_adbc_tests::exclusive();
    use adbc_core::options::OptionStatement;
    use adbc_core::Optionable;

    let mut connection = connect();
    partition_fixture(&mut connection);

    let total = (COPIES * 2) as usize;

    // A join is answered from a result the server holds, so its pieces are a
    // snapshot. A single-table query is answered without one, so its pieces
    // are not.
    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_option(
            OptionStatement::Other("kinetica.query.batch_size".into()),
            OptionValue::Int(500),
        )
        .expect("batch size");
    statement
        .set_sql_query(format!(
            "SELECT a.row_id FROM {PARTITION_TABLE} a \
             JOIN {PARTITION_TABLE} b ON a.row_id = b.row_id"
        ))
        .expect("query");

    let result = statement.execute_partitions().expect("partitions");
    assert!(result.partitions.len() > 1);

    let mut rows = 0usize;

    // The first piece, then the table loses most of its rows underneath.
    let reader = connect()
        .read_partition(&result.partitions[0])
        .expect("read");
    rows += reader.map(|b| b.expect("batch").num_rows()).sum::<usize>();

    run(
        &mut connection,
        &format!("DELETE FROM {PARTITION_TABLE} WHERE row_id > 10"),
    );

    for piece in &result.partitions[1..] {
        let reader = connect().read_partition(piece).expect("read");
        rows += reader.map(|b| b.expect("batch").num_rows()).sum::<usize>();
    }

    assert_eq!(
        rows, total,
        "a delete under a partitioned read lost rows the result already had"
    );

    run(
        &mut connection,
        &format!("DROP TABLE IF EXISTS {PARTITION_TABLE}"),
    );
    run(&mut connection, &format!("DROP TABLE IF EXISTS {TABLE}"));
}
