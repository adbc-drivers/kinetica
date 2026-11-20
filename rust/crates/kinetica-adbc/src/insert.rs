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

//! Recognizing the one INSERT shape that multi-head ingest can carry.
//!
//! `/execute/sql` inserts from the head rank, so a statement that goes through
//! it does not spread across the workers. A plain `INSERT INTO t VALUES (?, ?)`
//! is only a table and a row, so its rows can go straight to the workers
//! instead; anything the server has to work out for itself — an expression, a
//! function, a SELECT, an ON CONFLICT clause — cannot, and is left to it.
//!
//! The rule is therefore deliberately narrow: what is not recognized still
//! runs, just through the server, so a miss costs speed and never meaning.

use std::sync::LazyLock;

use regex::Regex;

/// An INSERT whose rows can be sent to the workers as they are.
pub struct PlainInsert {
    pub table: String,
    /// The columns the statement names, in the order its values are given.
    /// None when it names none and the row is the whole table.
    pub columns: Option<Vec<String>>,
}

/// An identifier, quoted however the dialect in front of us quotes them.
const IDENTIFIER: &str = r#"(?:[A-Za-z_]\w*|"[^"]*"|`[^`]*`|\[[^\]]*\])"#;

/// The whole statement, which has to be nothing but a table, an optional
/// column list, and one row of parameter markers. Anything trailing — another
/// row, a clause, an expression — leaves the match unanchored and so fails.
static PLAIN_INSERT: LazyLock<Regex> = LazyLock::new(|| {
    let marker = r"(?:\?|\$\d+)";

    let pattern = format!(
        concat!(
            r"(?is)^\s*INSERT\s+INTO\s+((?:{id}\s*\.\s*)*{id})\s*",
            r"(?:\(\s*({id}(?:\s*,\s*{id})*)\s*\)\s*)?",
            r"VALUES\s*\(\s*{marker}(?:\s*,\s*{marker})*\s*\)\s*;?\s*$"
        ),
        id = IDENTIFIER,
        marker = marker
    );

    Regex::new(&pattern).expect("the insert pattern is valid")
});

/// A comment can hold anything, including something that would change what the
/// statement means, so they come out before it is read.
static COMMENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)/\*.*?\*/|--[^\n]*").expect("the comment pattern is valid"));

/// @return The insert, if its rows can go to the workers untouched
pub fn recognize(sql: &str) -> Option<PlainInsert> {
    let bare = COMMENT.replace_all(sql, " ");
    let matched = PLAIN_INSERT.captures(&bare)?;

    Some(PlainInsert {
        table: unquote(matched.get(1)?.as_str()),
        columns: matched
            .get(2)
            .map(|list| list.as_str().split(',').map(unquote).collect()),
    })
}

/// @return The name a quoted identifier stands for
fn unquote(name: &str) -> String {
    let name = name.trim();

    let pairs = [('"', '"'), ('`', '`'), ('[', ']')];

    for (open, close) in pairs {
        if name.starts_with(open) && name.ends_with(close) && name.len() >= 2 {
            return name[1..name.len() - 1].to_string();
        }
    }

    name.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_of_markers_goes_to_the_workers() {
        let plain = recognize("INSERT INTO t VALUES (?, ?, ?)").expect("recognized");

        assert_eq!(plain.table, "t");
        assert!(plain.columns.is_none());
    }

    #[test]
    fn the_named_columns_keep_their_order() {
        let plain = recognize("insert into my_schema.t (b, \"A b\", c) values ($1, $2, $3)")
            .expect("recognized");

        assert_eq!(plain.table, "my_schema.t");
        assert_eq!(plain.columns.unwrap(), vec!["b", "A b", "c"]);
    }

    #[test]
    fn a_trailing_semicolon_and_comments_are_not_the_statement() {
        assert!(recognize("/* hint */ INSERT INTO t VALUES (?, ?); ").is_some());
    }

    /// Everything the server has to work out for itself stays with the server.
    #[test]
    fn anything_but_a_plain_row_is_left_to_the_server() {
        for sql in [
            "INSERT INTO t VALUES (?, UPPER(?))",
            "INSERT INTO t VALUES (?, ? + 1)",
            "INSERT INTO t VALUES (?, NOW())",
            "INSERT INTO t VALUES (?, 'literal')",
            "INSERT INTO t SELECT * FROM u",
            "INSERT INTO t VALUES (?, ?) ON CONFLICT DO NOTHING",
            "INSERT INTO t VALUES (?, ?), (?, ?)",
            "INSERT INTO t VALUES (?, ?) WITH OPTIONS (BATCH SIZE = 1000)",
            "UPDATE t SET a = ?",
            "SELECT * FROM t WHERE a = ?",
        ] {
            assert!(
                recognize(sql).is_none(),
                "{sql} should go through the server"
            );
        }
    }

    #[test]
    fn a_comment_cannot_smuggle_a_clause_past_the_check() {
        assert!(recognize("INSERT INTO t VALUES (?, ?) /* */ ON CONFLICT DO NOTHING").is_none());
    }
}
