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

//! Building the table an ingest is about to fill.
//!
//! Kinetica creates tables through SQL, not through the ingest endpoints, so a
//! mode that creates one writes the DDL from the stream's own schema. The
//! mapping is the reverse of the one the server encodes with, so a table read
//! out through this driver and written back arrives as it left.

use adbc_core::error::Result as AdbcResult;
use adbc_core::options::IngestMode;
use arrow_schema::{DataType, Field, Schema};

use crate::error::unsupported;

/// The metadata key Arrow carries an extension type's name under.
const EXTENSION: &str = "ARROW:extension:name";

/// The metadata key a column's properties are described under, which says
/// what Arrow cannot: how wide a string is, and what a number means.
const PROPERTIES: &str = crate::metadata::PROPERTIES;

/// The same, as the server names it, for Arrow that did not come through
/// this driver.
const SERVER_PROPERTIES: &str = "kinetica.properties";

/// @return The statement that makes the table, or None for a mode that expects
///         it to be there already
pub fn create(table: &str, schema: &Schema, mode: IngestMode) -> AdbcResult<Option<String>> {
    let opening = match mode {
        IngestMode::Append => return Ok(None),
        IngestMode::Create => "CREATE TABLE",
        IngestMode::Replace => "CREATE OR REPLACE TABLE",
        IngestMode::CreateAppend => "CREATE TABLE IF NOT EXISTS",
    };

    let mut columns = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        columns.push(format!(
            "{} {}{}",
            quote(field.name()),
            column_type(field)?,
            if field.is_nullable() { "" } else { " NOT NULL" }
        ));
    }

    Ok(Some(format!(
        "{opening} {} ({})",
        qualify(table),
        columns.join(", ")
    )))
}

/// @return The table name with every part quoted, so a name needing it survives
fn qualify(table: &str) -> String {
    table.split('.').map(quote).collect::<Vec<_>>().join(".")
}

/// @return The quoted identifier
fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// @return The Kinetica column type that holds the field's values
fn column_type(field: &Field) -> AdbcResult<String> {
    // An extension names the type outright, which is the only way Arrow can
    // carry the ones it has no layout of its own for.
    if let Some(name) = field.metadata().get(EXTENSION) {
        return match name.as_str() {
            "arrow.uuid" => Ok("UUID".into()),
            "arrow.json" => Ok("JSON".into()),
            name if name.starts_with("geoarrow.") => Ok("GEOMETRY".into()),
            other => Err(unsupported(&format!("the extension type '{other}'"))),
        };
    }

    // A column the server described says how wide it is and what it means,
    // which is what tells a fixed-width string from an unbounded one.
    if let Some(refined) = from_properties(field) {
        return Ok(refined);
    }

    match field.data_type() {
        DataType::Boolean => Ok("BOOLEAN".into()),
        DataType::Int8 => Ok("TINYINT".into()),
        DataType::Int16 | DataType::UInt8 => Ok("SMALLINT".into()),
        DataType::Int32 | DataType::UInt16 => Ok("INTEGER".into()),
        DataType::Int64 | DataType::UInt32 => Ok("BIGINT".into()),
        DataType::UInt64 => Ok("UNSIGNED BIGINT".into()),
        DataType::Float16 | DataType::Float32 => Ok("REAL".into()),
        DataType::Float64 => Ok("DOUBLE".into()),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            Ok(format!("DECIMAL({precision},{scale})"))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("VARCHAR".into()),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Ok("BLOB".into()),
        DataType::Date32 | DataType::Date64 => Ok("DATE".into()),
        DataType::Time32(_) | DataType::Time64(_) => Ok("TIME".into()),
        // Kinetica's timestamp is an instant and its datetime is a wall clock,
        // which is the same distinction Arrow draws with the zone.
        DataType::Timestamp(_, Some(_)) => Ok("TIMESTAMP".into()),
        DataType::Timestamp(_, None) => Ok("DATETIME".into()),
        DataType::FixedSizeList(item, size) if item.data_type() == &DataType::Float32 => {
            Ok(format!("VECTOR({size})"))
        }
        // A dictionary is how the values were carried, not what they are.
        DataType::Dictionary(_, values) => column_type(&Field::new(
            field.name(),
            values.as_ref().clone(),
            field.is_nullable(),
        )),
        other => Err(unsupported(&format!("a column of type {other}"))),
    }
}

/// @return The column type the server's own properties call for, if they say
fn from_properties(field: &Field) -> Option<String> {
    let properties = field
        .metadata()
        .get(PROPERTIES)
        .or_else(|| field.metadata().get(SERVER_PROPERTIES))?;

    for property in properties.split(',') {
        let property = property.trim();

        if let Some(width) = property.strip_prefix("char") {
            if width.parse::<u32>().is_ok() {
                return Some(format!("CHAR({width})"));
            }
        }

        match property {
            "ipv4" => return Some("IPV4".into()),
            "wkt" => return Some("GEOMETRY".into()),
            _ => {}
        }
    }

    None
}

/// Names a column's type as Kinetica's SQL writes it, from the type and the
/// properties the server describes it with: `char16` is `VARCHAR(16)`, and
/// `array` with the property `array(int,3)` is `INTEGER[3]`.
/// @return The SQL type name
pub(crate) fn sql_type_name(kinetica_type: &str, properties: &str) -> String {
    // A sized or element-typed column says the rest in its properties.
    let property = |prefix: &str| {
        properties
            .split(',')
            .map(str::trim)
            .find(|p| p.starts_with(prefix))
            .map(str::to_string)
    };

    match kinetica_type.trim() {
        "int8" => "TINYINT".into(),
        "int16" => "SMALLINT".into(),
        "int" => "INTEGER".into(),
        "long" => "BIGINT".into(),
        "ulong" => "UNSIGNED BIGINT".into(),
        "float" => "REAL".into(),
        "double" => "DOUBLE".into(),
        "boolean" => "BOOLEAN".into(),
        "string" => "VARCHAR".into(),
        "date" => "DATE".into(),
        "time" => "TIME".into(),
        "datetime" => "DATETIME".into(),
        "timestamp" => "TIMESTAMP".into(),
        "bytes" => "BLOB".into(),
        "json" => "JSON".into(),
        "uuid" => "UUID".into(),
        "geometry" => "GEOMETRY".into(),
        "ipv4" => "IPV4".into(),
        "vector" => match property("vector(") {
            Some(sized) => sized.to_uppercase(),
            None => "VECTOR".into(),
        },
        // An array's properties carry its element type and its size, in
        // one property that itself holds a comma: array(int,3).
        "array" => match properties.find("array(") {
            Some(start) => {
                let inner = &properties[start + "array(".len()..];
                let inner = inner.split(')').next().unwrap_or_default();
                let mut parts = inner.split(',').map(str::trim);
                let element = sql_type_name(parts.next().unwrap_or_default(), "");
                let size = parts.next().unwrap_or_default();
                format!("{element}[{size}]")
            }
            None => "ARRAY".into(),
        },
        other => {
            if let Some(width) = other.strip_prefix("char") {
                if width.chars().all(|c| c.is_ascii_digit()) && !width.is_empty() {
                    return format!("VARCHAR({width})");
                }
            }

            // decimal(10,2) and anything not named above are written as the
            // server gives them.
            other.to_uppercase()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::TimeUnit;
    use std::collections::HashMap;

    /// @return A schema of one field carrying the given metadata
    fn one(name: &str, data_type: DataType, metadata: &[(&str, &str)]) -> Schema {
        let pairs: HashMap<String, String> = metadata
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        Schema::new(vec![Field::new(name, data_type, true).with_metadata(pairs)])
    }

    #[test]
    fn appending_needs_no_table_made() {
        let schema = one("a", DataType::Int32, &[]);

        assert!(create("t", &schema, IngestMode::Append)
            .expect("create")
            .is_none());
    }

    #[test]
    fn each_mode_opens_the_statement_its_own_way() {
        let schema = one("a", DataType::Int32, &[]);

        for (mode, opening) in [
            (IngestMode::Create, "CREATE TABLE"),
            (IngestMode::Replace, "CREATE OR REPLACE TABLE"),
            (IngestMode::CreateAppend, "CREATE TABLE IF NOT EXISTS"),
        ] {
            let sql = create("s.t", &schema, mode).expect("create").expect("some");

            assert!(sql.starts_with(opening), "{sql}");
            assert!(sql.contains(r#""s"."t""#), "{sql}");
        }
    }

    #[test]
    fn every_type_the_driver_reads_can_be_written_back() {
        let cases = [
            (DataType::Boolean, "BOOLEAN"),
            (DataType::Int8, "TINYINT"),
            (DataType::Int16, "SMALLINT"),
            (DataType::Int32, "INTEGER"),
            (DataType::Int64, "BIGINT"),
            (DataType::UInt64, "UNSIGNED BIGINT"),
            (DataType::Float32, "REAL"),
            (DataType::Float64, "DOUBLE"),
            (DataType::Decimal128(19, 4), "DECIMAL(19,4)"),
            (DataType::Utf8, "VARCHAR"),
            (DataType::Binary, "BLOB"),
            (DataType::Date32, "DATE"),
            (DataType::Time32(TimeUnit::Millisecond), "TIME"),
            (
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                "TIMESTAMP",
            ),
            (DataType::Timestamp(TimeUnit::Millisecond, None), "DATETIME"),
            (
                DataType::FixedSizeList(
                    std::sync::Arc::new(Field::new("item", DataType::Float32, true)),
                    3,
                ),
                "VECTOR(3)",
            ),
        ];

        for (data_type, expected) in cases {
            let schema = one("c", data_type.clone(), &[]);
            let sql = create("t", &schema, IngestMode::Create)
                .expect("create")
                .expect("some");

            assert!(
                sql.contains(expected),
                "{data_type} should map to {expected}, got {sql}"
            );
        }
    }

    #[test]
    fn an_extension_names_its_own_type() {
        for (name, expected) in [
            ("arrow.uuid", "UUID"),
            ("arrow.json", "JSON"),
            ("geoarrow.wkb", "GEOMETRY"),
        ] {
            let schema = one("c", DataType::Binary, &[(EXTENSION, name)]);
            let sql = create("t", &schema, IngestMode::Create)
                .expect("create")
                .expect("some");

            assert!(
                sql.contains(expected),
                "{name} should map to {expected}, got {sql}"
            );
        }
    }

    #[test]
    fn a_column_is_named_as_kinetica_sql_writes_it() {
        for (kinetica_type, properties, expected) in [
            ("int8", "int8", "TINYINT"),
            ("int16", "int16", "SMALLINT"),
            ("int", "", "INTEGER"),
            ("long", "", "BIGINT"),
            ("ulong", "ulong", "UNSIGNED BIGINT"),
            ("float", "", "REAL"),
            ("double", "", "DOUBLE"),
            ("decimal(10,2)", "decimal(10,2),nullable", "DECIMAL(10,2)"),
            ("boolean", "boolean", "BOOLEAN"),
            ("string", "nullable", "VARCHAR"),
            ("char16", "char16,nullable", "VARCHAR(16)"),
            ("date", "date", "DATE"),
            ("time", "time", "TIME"),
            ("datetime", "datetime", "DATETIME"),
            ("timestamp", "timestamp", "TIMESTAMP"),
            ("bytes", "", "BLOB"),
            ("json", "json", "JSON"),
            ("uuid", "uuid", "UUID"),
            ("geometry", "wkt", "GEOMETRY"),
            ("ipv4", "ipv4", "IPV4"),
            ("vector", "vector(3),nullable", "VECTOR(3)"),
            ("array", "array(int,3),nullable", "INTEGER[3]"),
        ] {
            assert_eq!(
                sql_type_name(kinetica_type, properties),
                expected,
                "{kinetica_type} with {properties}"
            );
        }
    }

    #[test]
    fn the_properties_are_read_under_the_servers_own_key_too() {
        let schema = one(
            "c",
            DataType::Utf8,
            &[(SERVER_PROPERTIES, "char16,nullable")],
        );
        let sql = create("t", &schema, IngestMode::Create)
            .expect("create")
            .expect("some");

        assert!(sql.contains("CHAR(16)"), "{sql}");
    }

    #[test]
    fn the_servers_properties_say_what_arrow_cannot() {
        for (properties, expected) in [
            ("char32,nullable", "CHAR(32)"),
            ("ipv4,nullable", "IPV4"),
            ("wkt", "GEOMETRY"),
        ] {
            let schema = one("c", DataType::Utf8, &[(PROPERTIES, properties)]);
            let sql = create("t", &schema, IngestMode::Create)
                .expect("create")
                .expect("some");

            assert!(
                sql.contains(expected),
                "{properties} should map to {expected}, got {sql}"
            );
        }
    }

    #[test]
    fn a_column_that_takes_no_nulls_says_so() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let sql = create("t", &schema, IngestMode::Create)
            .expect("create")
            .expect("some");

        assert!(sql.contains("NOT NULL"), "{sql}");
    }
}
