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

//! Turns a bound Arrow batch into the parameters `execute_sql` accepts.
//!
//! Kinetica takes parameters either as a JSON array in the request options or
//! as Avro records alongside a schema. The JSON array is used here, because it
//! needs no schema and covers every type a query is likely to be parameterized
//! on.
//!
//! What it does not cover for free is precision. A JSON number is a double, so
//! a 64-bit integer past 2^53 arrives rounded: 18446744073709551615 comes back
//! as 18446744073709552000. Anything JSON cannot hold exactly is therefore sent
//! as a string and left for the server to parse, which it does for these types
//! anyway. Binary parameters have no JSON form at all and would need the Avro
//! path.

use adbc_core::error::Result as AdbcResult;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, Time32MillisecondType, TimestampMillisecondType, UInt64Type,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};

use crate::error::unsupported;

/// Renders one row of a bound batch as the JSON array Kinetica substitutes.
/// @return The array text, ready for the query_parameters option
pub fn row_to_json(batch: &RecordBatch, row: usize) -> AdbcResult<String> {
    let mut parts = Vec::with_capacity(batch.num_columns());

    for column in batch.columns() {
        parts.push(if column.is_null(row) {
            "null".to_string()
        } else {
            value_to_json(column, row)?
        });
    }

    Ok(format!("[{}]", parts.join(",")))
}

/// @return One value as JSON, or an error if its type cannot be a parameter
fn value_to_json(column: &dyn Array, row: usize) -> AdbcResult<String> {
    Ok(match column.data_type() {
        DataType::Boolean => column.as_boolean().value(row).to_string(),
        DataType::Int8 => column.as_primitive::<Int8Type>().value(row).to_string(),
        DataType::Int16 => column.as_primitive::<Int16Type>().value(row).to_string(),
        DataType::Int32 => column.as_primitive::<Int32Type>().value(row).to_string(),
        // Only quoted once a JSON number would stop being exact: quoting a
        // small one instead makes the server infer a string, so SELECT $1
        // would come back as text rather than a number.
        DataType::Int64 => exact_integer(column.as_primitive::<Int64Type>().value(row) as i128),
        DataType::UInt64 => exact_integer(column.as_primitive::<UInt64Type>().value(row) as i128),
        DataType::Float32 => column.as_primitive::<Float32Type>().value(row).to_string(),
        DataType::Float64 => column.as_primitive::<Float64Type>().value(row).to_string(),
        DataType::Utf8 => quote(column.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => quote(column.as_string::<i64>().value(row)),

        // A date arrives as a day offset and goes back as the text Kinetica
        // parses, since the parameter list carries no type of its own.
        DataType::Date32 => {
            let days = column.as_primitive::<Date32Type>().value(row);
            match chrono_date(days) {
                Some(text) => quote(&text),
                None => return Err(unsupported("a date parameter outside the supported range")),
            }
        }

        // Exact to the last digit as text; as a JSON number it would not be.
        DataType::Decimal128(_, _) => {
            quote(&column.as_primitive::<Decimal128Type>().value_as_string(row))
        }

        DataType::Time32(TimeUnit::Millisecond) => {
            let ms = column.as_primitive::<Time32MillisecondType>().value(row);
            quote(&format!(
                "{:02}:{:02}:{:02}.{:03}",
                ms / 3_600_000,
                (ms / 60_000) % 60,
                (ms / 1_000) % 60,
                ms % 1_000
            ))
        }

        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let ms = column.as_primitive::<TimestampMillisecondType>().value(row);
            let days = ms.div_euclid(86_400_000);
            let rest = ms.rem_euclid(86_400_000);

            match chrono_date(days as i32) {
                Some(date) => quote(&format!(
                    "{date} {:02}:{:02}:{:02}.{:03}",
                    rest / 3_600_000,
                    (rest / 60_000) % 60,
                    (rest / 1_000) % 60,
                    rest % 1_000
                )),
                None => return Err(unsupported("a timestamp parameter outside the range")),
            }
        }

        // A column of untyped nulls, which is how a bare None arrives.
        DataType::Null => "null".to_string(),

        DataType::Binary | DataType::LargeBinary => {
            return Err(unsupported(
                "binary parameters; JSON has no form for them, so they need the Avro path",
            ));
        }

        other => {
            return Err(unsupported(&format!("{other} parameters")));
        }
    })
}

/// A JSON number is a double, so beyond 2^53 it stops being exact and the
/// value has to travel as text instead.
/// @return The value as a JSON number, or as a string when too large
fn exact_integer(value: i128) -> String {
    const EXACT_LIMIT: i128 = 1 << 53;

    if value.abs() < EXACT_LIMIT {
        value.to_string()
    } else {
        format!("\"{value}\"")
    }
}

/// @return The JSON string form, with the characters JSON requires escaped
fn quote(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');

    for c in value.chars() {
        match c {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            c if (c as u32) < 0x20 => escaped.push_str(&format!("\\u{:04x}", c as u32)),
            c => escaped.push(c),
        }
    }

    escaped.push('"');
    escaped
}

/// @return The date that many days after the epoch, as text
fn chrono_date(days_from_epoch: i32) -> Option<String> {
    // Civil from days, so the crate graph stays as small as it is.
    let z = i64::from(days_from_epoch) + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    Some(format!("{y:04}-{m:02}-{d:02}"))
}

// --------------------------------------------------------------------------
// What a statement asks to be given

/// Counts the parameters a statement expects, skipping the places a marker can
/// appear without being one: string literals, quoted identifiers and comments.
/// `?` are counted as they come; `$n` are numbered, so the highest one seen is
/// the count whether or not it is repeated.
/// @return The number of parameters the statement takes
pub fn count(sql: &str) -> usize {
    let bytes = sql.as_bytes();

    let mut positional = 0;
    let mut highest = 0;
    let mut at = 0;

    while at < bytes.len() {
        match bytes[at] {
            b'\'' => at = skip_quoted(bytes, at, b'\''),
            b'"' => at = skip_quoted(bytes, at, b'"'),
            b'`' => at = skip_quoted(bytes, at, b'`'),
            b'[' => at = skip_to(bytes, at + 1, b']'),
            b'-' if bytes.get(at + 1) == Some(&b'-') => at = skip_to(bytes, at, b'\n'),
            b'/' if bytes.get(at + 1) == Some(&b'*') => at = skip_block_comment(bytes, at),
            b'?' => {
                positional += 1;
                at += 1;
            }
            b'$' => {
                let start = at + 1;
                let end = start
                    + bytes[start..]
                        .iter()
                        .take_while(|b| b.is_ascii_digit())
                        .count();

                if end > start {
                    // The digits are bounded by the slice, so this cannot fail.
                    let number: usize = sql[start..end].parse().unwrap_or(0);
                    highest = highest.max(number);
                }

                at = end.max(at + 1);
            }
            _ => at += 1,
        }
    }

    highest.max(positional)
}

/// @return The offset past a run delimited by `quote`, where the delimiter
///         doubled stands for itself
fn skip_quoted(bytes: &[u8], at: usize, quote: u8) -> usize {
    let mut index = at + 1;

    while index < bytes.len() {
        if bytes[index] == quote {
            if bytes.get(index + 1) == Some(&quote) {
                index += 2;
                continue;
            }

            return index + 1;
        }

        index += 1;
    }

    bytes.len()
}

/// @return The offset past the next `until`, or the end
fn skip_to(bytes: &[u8], at: usize, until: u8) -> usize {
    match bytes[at..].iter().position(|b| *b == until) {
        Some(found) => at + found + 1,
        None => bytes.len(),
    }
}

/// @return The offset past the closing `*/`, or the end
fn skip_block_comment(bytes: &[u8], at: usize) -> usize {
    let mut index = at + 2;

    while index + 1 < bytes.len() {
        if (bytes[index] == b'*') && (bytes[index + 1] == b'/') {
            return index + 2;
        }

        index += 1;
    }

    bytes.len()
}

#[cfg(test)]
mod counting {
    use super::count;

    #[test]
    fn markers_are_counted_as_they_come() {
        assert_eq!(count("SELECT * FROM t WHERE a = ? AND b = ?"), 2);
        assert_eq!(count("INSERT INTO t VALUES (?, ?, ?)"), 3);
        assert_eq!(count("SELECT 1"), 0);
    }

    #[test]
    fn numbered_markers_are_counted_by_the_highest() {
        assert_eq!(count("SELECT $1, $2, $1"), 2);
        assert_eq!(count("SELECT $3"), 3, "a gap still has to be filled");
    }

    #[test]
    fn a_marker_is_only_a_marker_where_it_can_be_one() {
        assert_eq!(count("SELECT '?' FROM t"), 0);
        assert_eq!(count(r#"SELECT "a?b" FROM t"#), 0);
        assert_eq!(count("SELECT * FROM t -- ? not this one\n WHERE a = ?"), 1);
        assert_eq!(count("SELECT /* ? nor this */ ? FROM t"), 1);
        assert_eq!(count("SELECT 'it''s ?', ? FROM t"), 1);
    }
}
