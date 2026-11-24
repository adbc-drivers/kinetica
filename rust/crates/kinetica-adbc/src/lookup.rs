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
//! `/execute/sql` answers from the head rank: it plans the statement, asks the
//! workers for the rows and sends them back. A `SELECT` that pins every key
//! column to a value needs none of that — the key says which rank holds the
//! row, and that rank can answer on its own.
//!
//! The rule is deliberately narrow, and anything not recognized still runs as
//! SQL, so a miss costs a round trip through the head rank and never changes
//! what a statement means. What is required:
//!
//! * a projection of plain columns, or `*`;
//! * one table;
//! * a `WHERE` that is a conjunction of comparisons against literals;
//! * every key column — the shard key, or the primary key where there is no
//!   shard key — pinned by `=` exactly once.
//!
//! Comparisons left over after the key is accounted for are passed to the
//! worker as a filter, which it applies to its own data. The rank holding the
//! key holds every row matching it, so the answer is the one the SQL engine
//! would have given.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use adbc_core::error::Result as AdbcResult;
use regex::Regex;

use gpudb_api::{Column, ColumnType, RecordEncoding, RecordRetriever, ShardKeyValue, Type};

use crate::error::could_not;
use crate::runtime;
use crate::session::Session;
use crate::statement::Page;

/// An identifier, quoted however the dialect in front of us quotes them.
const IDENTIFIER: &str = r#"(?:[A-Za-z_]\w*|"[^"]*"|`[^`]*`|\[[^\]]*\])"#;

/// A literal a comparison can be made against: a quoted string with doubled
/// quotes for an embedded one, a number, or a boolean. A marker (`?`, `$1`)
/// is not among them — its value is bound rather than written, and a bound
/// statement is left to the server.
const LITERAL: &str =
    r"(?:'(?:''|[^'])*'|[-+]?\d+(?:\.\d*)?(?:[eE][-+]?\d+)?|[-+]?\.\d+|TRUE|FALSE)";

/// The comparisons that can be made. Only `=` can pin a key column; the rest
/// can only ever be left over, and are filtered on the worker.
const OPERATOR: &str = r"(?:<=|>=|=|<|>)";

/// The whole statement. Anything trailing — an ORDER BY, a GROUP BY, a LIMIT,
/// a join, a second table — leaves the match unanchored and so fails.
static SELECT: LazyLock<Regex> = LazyLock::new(|| {
    let pattern = format!(
        concat!(
            r"(?is)^\s*SELECT\s+(\*|{id}(?:\s*,\s*{id})*)\s+FROM\s+((?:{id}\s*\.\s*)*{id})\s+",
            r"WHERE\s+({id}\s*{op}\s*{lit}(?:\s+AND\s+{id}\s*{op}\s*{lit})*)\s*;?\s*$"
        ),
        id = IDENTIFIER,
        op = OPERATOR,
        lit = LITERAL
    );

    Regex::new(&pattern).expect("the select pattern is valid")
});

/// One comparison out of the WHERE clause, pulled from the conjunction the
/// statement pattern matched as a whole.
static COMPARISON: LazyLock<Regex> = LazyLock::new(|| {
    let pattern = format!(
        r"(?is)({id})\s*({op})\s*({lit})",
        id = IDENTIFIER,
        op = OPERATOR,
        lit = LITERAL
    );

    Regex::new(&pattern).expect("the comparison pattern is valid")
});

/// A comment can hold anything, including something that would change what the
/// statement means, so they come out before it is read.
static COMMENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)/\*.*?\*/|--[^\n]*").expect("the comment pattern is valid"));

/// A SELECT of the shape a key lookup could carry, before the table is known.
pub struct Select {
    pub table: String,
    /// The columns the statement projects, or None where it projects `*`.
    pub columns: Option<Vec<String>>,
    pub comparisons: Vec<Comparison>,
}

/// One `column <op> literal` out of the WHERE clause.
pub struct Comparison {
    pub column: String,
    pub operator: String,
    /// The literal exactly as written, quotes and all, so that what the
    /// worker filters on is what the statement said.
    pub literal: String,
}

/// @return The statement, if its shape is one a key lookup could carry
pub fn recognize(sql: &str) -> Option<Select> {
    let bare = COMMENT.replace_all(sql, " ");
    let matched = SELECT.captures(&bare)?;

    let projection = matched.get(1)?.as_str();
    let comparisons = COMPARISON
        .captures_iter(matched.get(3)?.as_str())
        .map(|found| Comparison {
            column: unquote(&found[1]),
            operator: found[2].to_string(),
            literal: found[3].trim().to_string(),
        })
        .collect();

    Some(Select {
        table: unquote(matched.get(2)?.as_str()),
        columns: if projection.trim() == "*" {
            None
        } else {
            Some(projection.split(',').map(unquote).collect())
        },
        comparisons,
    })
}

/// @return The name a quoted identifier stands for
fn unquote(name: &str) -> String {
    let name = name.trim();

    for (open, close) in [('"', '"'), ('`', '`'), ('[', ']')] {
        if name.starts_with(open) && name.ends_with(close) && name.len() >= 2 {
            return name[1..name.len() - 1].to_string();
        }
    }

    name.to_string()
}

/// @return The text a quoted SQL literal stands for, with doubled quotes
///         reduced to one
fn literal_text(literal: &str) -> String {
    match literal
        .strip_prefix('\'')
        .and_then(|l| l.strip_suffix('\''))
    {
        Some(inner) => inner.replace("''", "'"),
        None => literal.to_string(),
    }
}

/// A key value the lookup holds, which [`ShardKeyValue`] borrows from.
///
/// The typed variants are parsed here rather than passed as text because that
/// is what the client's key encoding takes, and a value it cannot use means
/// this statement is not a lookup after all.
enum Value {
    Int(i32),
    Int8(i8),
    Int16(i16),
    Long(i64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    Timestamp(i64),
    /// Text, and which of the borrowing variants it stands for.
    Text(Textual, String),
}

/// The [`ShardKeyValue`] variants that carry text.
#[derive(Clone, Copy)]
enum Textual {
    String,
    Date,
    DateTime,
    Time,
    Decimal,
    Ipv4,
    Uuid,
    Ulong,
}

impl Value {
    /// @return The value as the client's key encoding takes it
    fn borrowed(&self) -> ShardKeyValue<'_> {
        match self {
            Value::Int(v) => ShardKeyValue::Int(*v),
            Value::Int8(v) => ShardKeyValue::Int8(*v),
            Value::Int16(v) => ShardKeyValue::Int16(*v),
            Value::Long(v) => ShardKeyValue::Long(*v),
            Value::Float(v) => ShardKeyValue::Float(*v),
            Value::Double(v) => ShardKeyValue::Double(*v),
            Value::Boolean(v) => ShardKeyValue::Boolean(*v),
            Value::Timestamp(v) => ShardKeyValue::Timestamp(*v),
            Value::Text(Textual::String, v) => ShardKeyValue::String(v),
            Value::Text(Textual::Date, v) => ShardKeyValue::Date(v),
            Value::Text(Textual::DateTime, v) => ShardKeyValue::DateTime(v),
            Value::Text(Textual::Time, v) => ShardKeyValue::Time(v),
            Value::Text(Textual::Decimal, v) => ShardKeyValue::Decimal(v),
            Value::Text(Textual::Ipv4, v) => ShardKeyValue::Ipv4(v),
            Value::Text(Textual::Uuid, v) => ShardKeyValue::Uuid(v),
            Value::Text(Textual::Ulong, v) => ShardKeyValue::Ulong(v),
        }
    }

    /// Reads a literal as a value of the column it is compared against.
    ///
    /// A literal the column's type cannot take is not an error here: the
    /// statement simply is not a lookup, and the server says what is wrong
    /// with it when it runs as SQL.
    /// @return The value, or None if this column cannot carry a key value
    fn of(column: &Column, literal: &str) -> Option<Self> {
        let text = literal_text(literal);
        let textual = |kind| Some(Value::Text(kind, text.clone()));

        match column.column_type() {
            ColumnType::Integer => text.parse().ok().map(Value::Int),
            ColumnType::Int8 => text.parse().ok().map(Value::Int8),
            ColumnType::Int16 => text.parse().ok().map(Value::Int16),
            ColumnType::Long => text.parse().ok().map(Value::Long),
            ColumnType::Float => text.parse().ok().map(Value::Float),
            ColumnType::Double => text.parse().ok().map(Value::Double),

            // A timestamp key is hashed on its milliseconds, so only a literal
            // already written that way is one. A date-and-time literal is left
            // to the server, which knows how to read it.
            ColumnType::Timestamp => text.parse().ok().map(Value::Timestamp),

            // Anything not a recognized word is false, which is what the
            // client's own text-to-boolean does.
            ColumnType::Boolean => Some(Value::Boolean(matches!(
                text.to_ascii_lowercase().as_str(),
                "true" | "t" | "yes" | "y" | "1"
            ))),

            ColumnType::String
            | ColumnType::Char1
            | ColumnType::Char2
            | ColumnType::Char4
            | ColumnType::Char8
            | ColumnType::Char16
            | ColumnType::Char32
            | ColumnType::Char64
            | ColumnType::Char128
            | ColumnType::Char256 => textual(Textual::String),

            ColumnType::Date => textual(Textual::Date),
            ColumnType::DateTime => textual(Textual::DateTime),
            ColumnType::Time => textual(Textual::Time),
            ColumnType::Decimal => textual(Textual::Decimal),
            ColumnType::Ipv4 => textual(Textual::Ipv4),
            ColumnType::Uuid => textual(Textual::Uuid),
            ColumnType::Ulong => textual(Textual::Ulong),

            // Types the server will not hash as a key, so a table keyed on
            // one cannot be looked up by it.
            ColumnType::Bytes
            | ColumnType::Array
            | ColumnType::Json
            | ColumnType::Vector
            | ColumnType::Wkb
            | ColumnType::Wkt => None,
        }
    }
}

/// A lookup worked out against the table it reads, ready to be run at any
/// offset.
pub(crate) struct Planned {
    keyed: Arc<Keyed>,
    /// The columns to ask for, `*` expanded, since the response carries them
    /// in the order they are asked for and that order is the result's.
    columns: Vec<String>,
    /// One value per key column, in key order.
    key_values: Vec<Value>,
    /// What the key did not account for, filtered on the worker.
    expression: Option<String>,
}

impl Planned {
    /// Reads one page from the rank holding the key.
    /// @return The page, in the same shape a page of SQL comes back in
    pub(crate) fn read(&self, offset: i64) -> AdbcResult<Page> {
        let key_values: Vec<ShardKeyValue<'_>> =
            self.key_values.iter().map(Value::borrowed).collect();

        let response = runtime::block_on(self.keyed.retriever.get_columns_by_key_raw_with_offset(
            self.columns.clone(),
            &key_values,
            self.expression.as_deref(),
            offset,
            RecordEncoding::Arrow,
        ))
        .map_err(could_not("look up the key"))?;

        Ok(Page {
            paging_table: String::new(),
            result_tables: Vec::new(),
            stream: response.binary_encoded_response.into_vec(),
            // A query changed nothing, which is what -1 says.
            affected: -1,
            total: response.total_number_of_records,
            has_more: response.has_more_records,
        })
    }
}

/// A table that can be looked up by key, and what it takes to do so.
struct Keyed {
    retriever: RecordRetriever,
    record_type: Arc<Type>,
    /// The key columns, in the order the client's key encoding wants them.
    key_columns: Vec<String>,
}

/// How many tables a connection remembers the answer for. Each holds the
/// worker URLs and the shard mapping, so they are not kept without limit.
const KEPT_RETRIEVERS: usize = 16;

/// What a connection has worked out about the tables it has looked up.
///
/// A table that cannot be looked up is remembered too, as `None`, so a query
/// shaped like a lookup against a table without a usable key does not pay for
/// a `/show/table` every time it runs.
#[derive(Default)]
pub struct Retrievers {
    known: Mutex<HashMap<String, Entry>>,
    /// Counts uses, so the least recent can be told from the rest.
    clock: std::sync::atomic::AtomicU64,
}

struct Entry {
    keyed: Option<Arc<Keyed>>,
    used: u64,
}

impl Retrievers {
    /// Forgets every table, so the next lookup works the answer out again.
    ///
    /// What is remembered here is a table's key and the shard mapping that
    /// goes with it, and a statement that changes a table changes both. A
    /// stale answer would route a lookup to the rank that used to hold the
    /// row, so this is correctness and not just freshness.
    ///
    /// Known limit: only what this connection runs is noticed. A table
    /// altered elsewhere — another connection, another client — is not, and
    /// the lookup goes on using what it last read.
    pub(crate) fn forget(&self) {
        self.known.lock().unwrap().clear();
    }

    /// @return What is known about the table, worked out on first use, or
    ///         None if it cannot be looked up by key
    fn get(&self, session: &Session, table: &str) -> Option<Arc<Keyed>> {
        let now = self
            .clock
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        if let Some(entry) = self.known.lock().unwrap().get_mut(table) {
            entry.used = now;

            return entry.keyed.clone();
        }

        // Built without the lock, since it reads the table from the server.
        // Two statements asking at once both build, and the second overwrites
        // the first; they are the same answer, and holding the lock across a
        // round trip to keep one of them would cost more than it saves.
        let keyed = Self::build(session, table).map(Arc::new);
        let mut known = self.known.lock().unwrap();

        known.insert(
            table.to_string(),
            Entry {
                keyed: keyed.clone(),
                used: now,
            },
        );

        if known.len() > KEPT_RETRIEVERS {
            let coldest = known
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(name, _)| name.clone());

            if let Some(name) = coldest {
                known.remove(&name);
            }
        }

        keyed
    }

    /// Looks the table up and builds a retriever for it.
    ///
    /// Every failure means the same thing here — this statement runs as SQL —
    /// so a table that does not exist, or has no key, or has a key of a type
    /// that cannot be hashed, all come back as None rather than as errors. The
    /// server says what is wrong with the statement when it runs it.
    /// @return What it takes to look the table up, if it can be
    fn build(session: &Session, table: &str) -> Option<Keyed> {
        let record_type =
            Arc::new(runtime::block_on(Type::from_table(session.client(), table)).ok()?);

        // The server shards on the explicit shard key, and only on the primary
        // key where there is none. The union of the two would be the wrong
        // key: a table with PRIMARY KEY (a, b) and SHARD KEY (a) is sharded on
        // `a` alone.
        let key_indices = if record_type.has_shard_key() {
            record_type.shard_key_indices()
        } else {
            record_type.primary_key_indices()
        };

        if key_indices.is_empty() {
            return None;
        }

        let key_columns: Vec<String> = key_indices
            .iter()
            .filter_map(|index| record_type.get_column(*index))
            .map(|column| column.name().to_string())
            .collect();

        if key_columns.len() != key_indices.len() {
            return None;
        }

        let retriever = runtime::block_on(RecordRetriever::new(
            Arc::clone(session.client()),
            table,
            Arc::clone(&record_type),
        ))
        .ok()?;

        Some(Keyed {
            retriever,
            record_type,
            key_columns,
        })
    }
}

/// Whether a name can go into a filter expression as it stands.
///
/// A filter expression is not SQL, and double quotes in one make a string
/// rather than quote an identifier, so there is no way to write a name that
/// is not a plain word. A table with such a column is left to the server.
/// @return Whether the name is a bare identifier
fn is_plain(name: &str) -> bool {
    let mut characters = name.chars();

    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && characters.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Whether a statement leaves every table's shape as it was.
///
/// Only the two that are common enough to be worth telling apart are
/// recognized. Everything else — a CREATE, an ALTER, a DROP, anything
/// unrecognized — is taken to have changed something, so the cost of being
/// wrong is a `/show/table` rather than a row read from the wrong rank.
/// @return Whether what a connection knows about its tables still holds
pub(crate) fn reads_only(sql: &str) -> bool {
    let bare = COMMENT.replace_all(sql, " ");
    let first = bare.split_whitespace().next().unwrap_or("");

    first.eq_ignore_ascii_case("select") || first.eq_ignore_ascii_case("insert")
}

/// Finds the column a name in the statement refers to.
///
/// A statement may spell a column in any case, and what goes back to the
/// server has to be the table's own spelling.
/// @return The column, or None if the table has none by that name
fn resolve<'a>(record_type: &'a Type, name: &str) -> Option<&'a Column> {
    record_type.get_column_by_name(name).or_else(|| {
        record_type
            .columns()
            .find(|column| column.name().eq_ignore_ascii_case(name))
    })
}

/// Works out whether a statement can be answered by the rank holding its row.
///
/// `schema` is the one unqualified names are looked for in, so the table the
/// retriever reads is the table the statement meant.
/// @return The lookup to run, or None if this statement is not one
pub(crate) fn plan(session: &Session, sql: &str, schema: Option<&str>) -> Option<Planned> {
    let select = recognize(sql)?;

    let table = match schema {
        Some(schema) if !select.table.contains('.') => format!("{schema}.{}", select.table),
        _ => select.table.clone(),
    };

    let keyed = session.retrievers().get(session, &table)?;

    // Every key column has to be pinned, by equality, exactly once. A key only
    // partly given does not say which rank holds the row.
    let mut key_values = Vec::with_capacity(keyed.key_columns.len());

    for name in &keyed.key_columns {
        let mut pinning = select
            .comparisons
            .iter()
            .filter(|c| c.operator == "=" && c.column.eq_ignore_ascii_case(name));

        let pin = pinning.next()?;

        if pinning.next().is_some() {
            return None;
        }

        key_values.push(Value::of(resolve(&keyed.record_type, name)?, &pin.literal)?);
    }

    // What is left is filtered by the worker over its own data. The rank
    // holding the key holds every row with that key, so filtering there gives
    // the rows the SQL engine would have.
    let leftover: Vec<String> = select
        .comparisons
        .iter()
        .filter(|c| {
            !(c.operator == "="
                && keyed
                    .key_columns
                    .iter()
                    .any(|name| c.column.eq_ignore_ascii_case(name)))
        })
        .map(|c| {
            resolve(&keyed.record_type, &c.column)
                .filter(|column| is_plain(column.name()))
                .map(|column| format!("{} {} {}", column.name(), c.operator, c.literal))
        })
        .collect::<Option<Vec<String>>>()?;

    // Asking for columns the table does not have would be the server's error
    // to give, and it gives a better one for the statement as written.
    let columns = match &select.columns {
        None => keyed
            .record_type
            .columns()
            .map(|column| column.name().to_string())
            .collect(),
        Some(named) => named
            .iter()
            .map(|name| resolve(&keyed.record_type, name).map(|c| c.name().to_string()))
            .collect::<Option<Vec<String>>>()?,
    };

    Some(Planned {
        keyed,
        columns,
        key_values,
        expression: if leftover.is_empty() {
            None
        } else {
            Some(leftover.join(" and "))
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pinned_key_is_recognized() {
        let select = recognize("SELECT * FROM t WHERE id = 1").expect("recognized");

        assert_eq!(select.table, "t");
        assert!(select.columns.is_none());
        assert_eq!(select.comparisons.len(), 1);
        assert_eq!(select.comparisons[0].column, "id");
        assert_eq!(select.comparisons[0].operator, "=");
        assert_eq!(select.comparisons[0].literal, "1");
    }

    #[test]
    fn the_projection_keeps_its_order_and_the_table_its_schema() {
        let select =
            recognize("select b, \"A b\" from my_schema.t where k = 'x'").expect("recognized");

        assert_eq!(select.table, "my_schema.t");
        assert_eq!(select.columns.unwrap(), vec!["b", "A b"]);
    }

    #[test]
    fn every_comparison_of_a_conjunction_is_read() {
        let select = recognize("SELECT * FROM t WHERE a = 1 AND b >= 'two' AND c < 3.5")
            .expect("recognized");

        let read: Vec<_> = select
            .comparisons
            .iter()
            .map(|c| (c.column.as_str(), c.operator.as_str(), c.literal.as_str()))
            .collect();

        assert_eq!(
            read,
            [("a", "=", "1"), ("b", ">=", "'two'"), ("c", "<", "3.5")]
        );
    }

    /// A quote inside a literal is written twice, and stands for one.
    #[test]
    fn a_doubled_quote_is_one_quote() {
        let select = recognize("SELECT * FROM t WHERE name = 'O''Hara'").expect("recognized");

        assert_eq!(select.comparisons[0].literal, "'O''Hara'");
        assert_eq!(literal_text(&select.comparisons[0].literal), "O'Hara");
    }

    /// Everything the rank holding one row cannot answer by itself stays with
    /// the server, which can.
    #[test]
    fn anything_else_is_left_to_the_server() {
        for sql in [
            "SELECT * FROM t",
            "SELECT * FROM t WHERE id = 1 ORDER BY id",
            "SELECT * FROM t WHERE id = 1 LIMIT 10",
            "SELECT * FROM t WHERE id = 1 OR id = 2",
            "SELECT * FROM t WHERE id IN (1, 2)",
            "SELECT * FROM t WHERE id = ?",
            "SELECT * FROM t WHERE id = $1",
            "SELECT * FROM t WHERE id = 1 GROUP BY id",
            "SELECT count(*) FROM t WHERE id = 1",
            "SELECT a + 1 FROM t WHERE id = 1",
            "SELECT * FROM t JOIN u ON t.id = u.id WHERE t.id = 1",
            "SELECT * FROM t WHERE id = (SELECT max(id) FROM u)",
            "SELECT * FROM t WHERE id = 1 UNION SELECT * FROM u WHERE id = 2",
            "SELECT * FROM t WHERE name LIKE 'a%'",
            "SELECT * FROM t WHERE id IS NULL",
            "SELECT * FROM t WHERE id = NULL",
            "UPDATE t SET a = 1 WHERE id = 1",
            "DELETE FROM t WHERE id = 1",
        ] {
            assert!(recognize(sql).is_none(), "{sql} should go to the server");
        }
    }

    #[test]
    fn a_comment_cannot_smuggle_a_clause_past_the_check() {
        assert!(recognize("SELECT * FROM t WHERE id = 1 /* */ ORDER BY id").is_none());
        assert!(recognize("/* hint */ SELECT * FROM t WHERE id = 1;").is_some());
    }
}
