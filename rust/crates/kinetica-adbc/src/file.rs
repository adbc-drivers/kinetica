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

//! Loading a local file into a table.
//!
//! `INSERT INTO t SELECT * FROM FILE."data.csv"` reads a file that is *here*
//! rather than on the server, so the server cannot run it: the rows are read
//! by the driver and sent to the workers like any other ingest.
//!
//! Every field is read as text and converted by the ingest path, which
//! already casts a column to whatever its target column wants. So there is no
//! second type mapping here — a CSV column and an Arrow column of strings
//! reach the workers by the same road.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use adbc_core::error::Result as AdbcResult;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_csv::reader::Format;
use arrow_csv::ReaderBuilder;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use regex::Regex;

use crate::error::{from_arrow, invalid, unsupported};
use crate::options;

/// An identifier, quoted however the dialect in front of us quotes them.
const IDENTIFIER: &str = r#"(?:[A-Za-z_]\w*|"[^"]*"|`[^`]*`|\[[^\]]*\])"#;

/// `INSERT INTO <table> [(<columns>)] SELECT {* | <columns>} FROM FILE."<path>"
/// [WITH OPTIONS (...)]`.
static FROM_FILE: LazyLock<Regex> = LazyLock::new(|| {
    let pattern = format!(
        concat!(
            r"(?is)^\s*INSERT\s+INTO\s+((?:{id}\s*\.\s*)*{id})\s*",
            r"(?:\(\s*({id}(?:\s*,\s*{id})*)\s*\)\s*)?",
            r#"SELECT\s+(\*|{id}(?:\s*,\s*{id})*)\s+FROM\s+FILE\s*\.\s*"([^"]*)"\s*"#,
            r"(?:WITH\s+OPTIONS\s*\(\s*(.*?)\s*\)\s*)?;?\s*$"
        ),
        id = IDENTIFIER
    );

    Regex::new(&pattern).expect("the from-file pattern is valid")
});

/// One `name = value` of a `WITH OPTIONS` list, where the name may be several
/// words and the value may be quoted.
static OPTION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?is)([A-Za-z_][\w ]*?)\s*(?:=|\bAS\b)\s*('(?:''|[^'])*'|[^\s,]+)")
        .expect("the option pattern is valid")
});

/// A comment can hold anything, including something that would change what
/// the statement means, so they come out before it is read.
static COMMENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)/\*.*?\*/|--[^\n]*").expect("the comment pattern is valid"));

/// An insert whose rows come from a file on this machine.
pub struct FromFile {
    pub table: String,
    /// The table columns the statement names, in the order the file's are
    /// read into them. None where it names none.
    pub columns: Option<Vec<String>>,
    /// The file columns the SELECT names, in the order it wants them. None
    /// for `*`, which takes them all as they come.
    pub selected: Option<Vec<String>>,
    /// The file to read, which may name several with a glob.
    pub path: String,
    /// What the statement said about reading it, which beats the connection.
    pub said: Vec<(String, String)>,
}

/// @return The statement, if its rows come from a local file
pub fn recognize(sql: &str) -> Option<FromFile> {
    let bare = COMMENT.replace_all(sql, " ");
    let matched = FROM_FILE.captures(&bare)?;

    let selected = matched.get(3)?.as_str();
    let said = matched
        .get(5)
        .map(|list| {
            OPTION
                .captures_iter(list.as_str())
                .map(|found| {
                    (
                        found[1].split_whitespace().collect::<Vec<_>>().join("_"),
                        unquote(&found[2]),
                    )
                })
                .collect()
        })
        .unwrap_or_default();

    Some(FromFile {
        table: unquote(matched.get(1)?.as_str()),
        columns: matched
            .get(2)
            .map(|list| list.as_str().split(',').map(unquote).collect()),
        selected: if selected.trim() == "*" {
            None
        } else {
            Some(selected.split(',').map(unquote).collect())
        },
        path: matched.get(4)?.as_str().to_string(),
        said,
    })
}

/// @return The name a quoted identifier or literal stands for
fn unquote(text: &str) -> String {
    let text = text.trim();

    for (open, close) in [('"', '"'), ('`', '`'), ('[', ']'), ('\'', '\'')] {
        if text.starts_with(open) && text.ends_with(close) && text.len() >= 2 {
            let inner = &text[1..text.len() - 1];

            return if open == '\'' {
                inner.replace("''", "'")
            } else {
                inner.to_string()
            };
        }
    }

    text.to_string()
}

/// How a local file is read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRead {
    /// None takes the separator from the first file's extension.
    pub delimiter: Option<u8>,
    pub quote: u8,
    pub escape: Option<u8>,
    pub comment: Option<u8>,
    pub has_header: bool,
    /// None makes an empty field the null, which is arrow's own default.
    pub null_string: Option<String>,
    /// Rows passed over at the start of each file, after its header.
    pub skip: usize,
    /// The most rows to read across every file matched.
    pub limit: Option<usize>,
    /// Whether the table is emptied first.
    pub initial_clear: bool,
}

impl Default for FileRead {
    fn default() -> Self {
        Self {
            delimiter: None,
            quote: b'"',
            escape: None,
            comment: None,
            has_header: true,
            null_string: None,
            skip: 0,
            limit: None,
            initial_clear: false,
        }
    }
}

impl FileRead {
    /// Takes the option if it is one of these.
    /// @return Whether the name was one of these options
    pub fn set(&mut self, name: &str, value: &str) -> AdbcResult<bool> {
        match name {
            options::FILE_READ_DELIMITER => self.delimiter = Some(character(name, value)?),
            options::FILE_READ_QUOTE_CHAR => self.quote = character(name, value)?,
            options::FILE_READ_ESCAPE_CHAR => self.escape = Some(character(name, value)?),
            options::FILE_READ_COMMENT_CHAR => self.comment = Some(character(name, value)?),
            options::FILE_READ_HAS_HEADER => self.has_header = flag(name, value)?,
            options::FILE_READ_NULL_STRING => self.null_string = Some(value.to_string()),
            options::FILE_READ_SKIP => self.skip = count(name, value)?,
            options::FILE_READ_LIMIT => self.limit = Some(count(name, value)?),
            options::FILE_READ_INITIAL_CLEAR => self.initial_clear = flag(name, value)?,
            _ => return Ok(false),
        }

        Ok(true)
    }

    /// @return What was said under this name, if it is one of these
    pub fn get(&self, name: &str) -> Option<String> {
        let value = match name {
            options::FILE_READ_DELIMITER => said(self.delimiter)?,
            options::FILE_READ_QUOTE_CHAR => (self.quote as char).to_string(),
            options::FILE_READ_ESCAPE_CHAR => said(self.escape)?,
            options::FILE_READ_COMMENT_CHAR => said(self.comment)?,
            options::FILE_READ_HAS_HEADER => self.has_header.to_string(),
            options::FILE_READ_NULL_STRING => self.null_string.clone()?,
            options::FILE_READ_SKIP => self.skip.to_string(),
            options::FILE_READ_LIMIT => self.limit?.to_string(),
            options::FILE_READ_INITIAL_CLEAR => self.initial_clear.to_string(),
            _ => return None,
        };

        Some(value)
    }

    /// Takes what a statement's own `WITH OPTIONS (...)` said, which beats
    /// what the connection was told. The names are the option names without
    /// the prefix, written with spaces or underscores.
    /// @return A copy carrying what the statement said
    pub(crate) fn and(&self, said: &[(String, String)]) -> AdbcResult<Self> {
        let mut read = self.clone();

        for (name, value) in said {
            let full = format!("adbc.kinetica.{}", name.to_lowercase());

            // Anything else is for the server to make sense of, not this: a
            // statement may carry ingest options beside the file ones.
            let _ = read.set(&full, value)?;
        }

        Ok(read)
    }

    /// @return The separator to read with, given the first file's name
    fn separator(&self, first: &Path) -> u8 {
        if let Some(delimiter) = self.delimiter {
            return delimiter;
        }

        match first
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_lowercase)
            .as_deref()
        {
            Some("psv") => b'|',
            Some("tsv") => b'\t',
            _ => b',',
        }
    }

    /// @return The arrow-csv format these settings describe
    fn format(&self, first: &Path) -> Format {
        let mut format = Format::default()
            .with_header(self.has_header)
            .with_delimiter(self.separator(first))
            .with_quote(self.quote);

        if let Some(escape) = self.escape {
            format = format.with_escape(escape);
        }

        if let Some(comment) = self.comment {
            format = format.with_comment(comment);
        }

        format
    }
}

/// @return The one character the value names
fn character(name: &str, value: &str) -> AdbcResult<u8> {
    let mut characters = value.chars();

    match (characters.next(), characters.next()) {
        // Written as an escape, since a tab cannot be typed into most places
        // a connection string is written.
        (Some('\\'), Some('t')) => Ok(b'\t'),
        (Some(one), None) if one.is_ascii() => Ok(one as u8),
        _ => Err(invalid(&format!(
            "{name} takes one ASCII character, not '{value}'"
        ))),
    }
}

/// @return What the text says, for an option that is on or off
fn flag(name: &str, value: &str) -> AdbcResult<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(invalid(&format!(
            "{name} takes true or false, not '{value}'"
        ))),
    }
}

/// @return The whole number the text says
fn count(name: &str, value: &str) -> AdbcResult<usize> {
    value
        .parse()
        .map_err(|_| invalid(&format!("{name} takes a whole number, not '{value}'")))
}

/// @return A character as it would be written back
fn said(value: Option<u8>) -> Option<String> {
    value.map(|byte| (byte as char).to_string())
}

/// The files a path names, which may be one or a glob of many.
///
/// @return Every file matched, in a settled order, or an error if none is
pub(crate) fn matching(path: &str) -> AdbcResult<Vec<PathBuf>> {
    let mut found: Vec<PathBuf> = glob::glob(path)
        .map_err(|e| invalid(&format!("'{path}' is not a usable file pattern ({e})")))?
        .filter_map(Result::ok)
        .filter(|found| found.is_file())
        .collect();

    // A glob hands them back in whatever order the directory gives, which
    // would make two runs of the same statement load the rows in two orders.
    found.sort();

    if found.is_empty() {
        return Err(invalid(&format!("'{path}', which matches no file")));
    }

    Ok(found)
}

/// Reads every matched file as one stream of batches, each column text.
///
/// The first file's columns are the stream's: one schema covers them all, so
/// the files a glob matches have to have the same columns in the same order.
pub(crate) struct Rows {
    settings: FileRead,
    schema: SchemaRef,
    /// The columns to keep, where the statement named them.
    projection: Option<Vec<usize>>,
    /// The shape handed out, which is the schema projected.
    handed: SchemaRef,
    rest: std::vec::IntoIter<PathBuf>,
    reading: Option<arrow_csv::Reader<File>>,
    /// What is left of the row cap, where there is one.
    allowed: Option<usize>,
}

impl Rows {
    /// @return A reader over the files, with the columns the statement asked
    ///         for in the order it asked for them
    pub(crate) fn new(
        files: Vec<PathBuf>,
        settings: FileRead,
        selected: Option<&[String]>,
    ) -> AdbcResult<Self> {
        let first = files.first().expect("a file to read").clone();
        let schema = Self::columns_of(&first, &settings)?;

        let projection = match selected {
            None => None,
            Some(names) => Some(
                names
                    .iter()
                    .map(|name| {
                        schema
                            .fields()
                            .iter()
                            .position(|field| field.name().eq_ignore_ascii_case(name))
                            .ok_or_else(|| {
                                unsupported(&format!(
                                    "a column '{name}', which '{}' does not have",
                                    first.display()
                                ))
                            })
                    })
                    .collect::<AdbcResult<Vec<usize>>>()?,
            ),
        };

        let handed = match &projection {
            None => Arc::clone(&schema),
            Some(indices) => Arc::new(schema.project(indices).map_err(from_arrow)?),
        };

        let allowed = settings.limit;

        Ok(Self {
            settings,
            schema,
            projection,
            handed,
            rest: files.into_iter(),
            reading: None,
            allowed,
        })
    }

    /// The columns of a file, every one of them read as text: the ingest path
    /// casts each to whatever its target column wants, so inferring types
    /// here would only be undone there.
    /// @return The file's columns, as text
    fn columns_of(first: &Path, settings: &FileRead) -> AdbcResult<SchemaRef> {
        let mut file =
            File::open(first).map_err(|e| invalid(&format!("'{}' ({e})", first.display())))?;

        // Only the names are taken from this; a row is enough to find them.
        let (inferred, _) = settings
            .format(first)
            .infer_schema(&mut file, Some(1))
            .map_err(from_arrow)?;

        let fields: Vec<Field> = inferred
            .fields()
            .iter()
            .map(|field| Field::new(field.name(), DataType::Utf8, true))
            .collect();

        Ok(Arc::new(Schema::new(fields)))
    }

    /// Opens the next file, if there is one.
    /// @return Whether a file was opened
    fn open_next(&mut self) -> Result<bool, ArrowError> {
        let Some(path) = self.rest.next() else {
            return Ok(false);
        };

        let file = File::open(&path).map_err(|e| {
            ArrowError::ExternalError(Box::new(std::io::Error::other(format!(
                "{}: {e}",
                path.display()
            ))))
        })?;

        let mut builder = ReaderBuilder::new(Arc::clone(&self.schema))
            .with_header(self.settings.has_header)
            .with_delimiter(self.settings.separator(&path))
            .with_quote(self.settings.quote);

        if let Some(escape) = self.settings.escape {
            builder = builder.with_escape(escape);
        }

        if let Some(comment) = self.settings.comment {
            builder = builder.with_comment(comment);
        }

        if let Some(null) = &self.settings.null_string {
            builder = builder.with_null_regex(
                Regex::new(&format!("^{}$", regex::escape(null)))
                    .expect("an escaped literal is a valid pattern"),
            );
        }

        // Rows passed over at the start of this file, which is what skipping
        // means: the header is not one of them.
        if self.settings.skip > 0 {
            builder = builder.with_bounds(self.settings.skip, usize::MAX);
        }

        self.reading = Some(builder.build(file)?);

        Ok(true)
    }
}

impl Iterator for Rows {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.allowed == Some(0) {
                return None;
            }

            if self.reading.is_none() {
                match self.open_next() {
                    Ok(true) => {}
                    Ok(false) => return None,
                    Err(e) => return Some(Err(e)),
                }
            }

            let reader = self.reading.as_mut()?;

            match reader.next() {
                // This file is done; the next one carries on the same stream.
                None => self.reading = None,
                Some(Err(e)) => return Some(Err(e)),
                Some(Ok(batch)) => {
                    let batch = match &self.projection {
                        None => batch,
                        Some(indices) => match batch.project(indices) {
                            Ok(projected) => projected,
                            Err(e) => return Some(Err(e)),
                        },
                    };

                    // The cap is counted across every file, so the batch that
                    // reaches it is cut rather than the file being finished.
                    let batch = match self.allowed {
                        None => batch,
                        Some(left) => {
                            let taking = left.min(batch.num_rows());

                            self.allowed = Some(left - taking);

                            if taking < batch.num_rows() {
                                batch.slice(0, taking)
                            } else {
                                batch
                            }
                        }
                    };

                    if batch.num_rows() > 0 {
                        return Some(Ok(batch));
                    }
                }
            }
        }
    }
}

impl RecordBatchReader for Rows {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.handed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_insert_is_recognized() {
        let found = recognize("INSERT INTO t SELECT * FROM FILE.\"/tmp/a.csv\"").expect("read");

        assert_eq!(found.table, "t");
        assert!(found.columns.is_none());
        assert!(found.selected.is_none());
        assert_eq!(found.path, "/tmp/a.csv");
        assert!(found.said.is_empty());
    }

    #[test]
    fn both_column_lists_are_read_in_order() {
        let found = recognize(
            "insert into my_schema.t (num, who) select id, \"full name\" from file.\"a.csv\";",
        )
        .expect("read");

        assert_eq!(found.table, "my_schema.t");
        assert_eq!(found.columns.unwrap(), vec!["num", "who"]);
        assert_eq!(found.selected.unwrap(), vec!["id", "full name"]);
    }

    #[test]
    fn with_options_are_read_as_names_and_values() {
        let found = recognize(
            "INSERT INTO t SELECT * FROM FILE.\"a.csv\" WITH OPTIONS (\
             file read limit = 10, FILE_READ_HAS_HEADER = false, file read null string = 'NULL')",
        )
        .expect("read");

        assert_eq!(
            found.said,
            vec![
                ("file_read_limit".to_string(), "10".to_string()),
                ("FILE_READ_HAS_HEADER".to_string(), "false".to_string()),
                ("file_read_null_string".to_string(), "NULL".to_string()),
            ]
        );
    }

    /// What a statement says beats what the connection was told, and a name
    /// that is not one of these is left alone rather than refused: a
    /// statement may carry ingest options beside the file ones.
    #[test]
    fn a_statements_own_options_win() {
        let mut connection = FileRead::default();
        connection
            .set(options::FILE_READ_LIMIT, "100")
            .expect("limit");

        let said = [
            ("file_read_limit".to_string(), "5".to_string()),
            ("batch_size".to_string(), "1000".to_string()),
        ];
        let read = connection.and(&said).expect("merged");

        assert_eq!(read.limit, Some(5));
        assert!(read.has_header, "what was not said is left as it was");
    }

    #[test]
    fn a_delimiter_follows_the_extension_unless_it_is_said() {
        let settings = FileRead::default();

        assert_eq!(settings.separator(Path::new("a.csv")), b',');
        assert_eq!(settings.separator(Path::new("a.psv")), b'|');
        assert_eq!(settings.separator(Path::new("a.TSV")), b'\t');
        assert_eq!(settings.separator(Path::new("a")), b',');

        let mut said = FileRead::default();
        said.set(options::FILE_READ_DELIMITER, ";")
            .expect("delimiter");

        assert_eq!(
            said.separator(Path::new("a.psv")),
            b';',
            "said beats guessed"
        );
    }

    /// A tab cannot be typed into most places a connection string is written.
    #[test]
    fn a_tab_delimiter_can_be_written_as_an_escape() {
        let mut settings = FileRead::default();
        settings
            .set(options::FILE_READ_DELIMITER, r"\t")
            .expect("tab");

        assert_eq!(settings.delimiter, Some(b'\t'));
    }

    #[test]
    fn a_value_that_is_not_one_character_is_refused() {
        let mut settings = FileRead::default();

        assert!(settings.set(options::FILE_READ_QUOTE_CHAR, "ab").is_err());
        assert!(settings.set(options::FILE_READ_SKIP, "many").is_err());
        assert!(settings
            .set(options::FILE_READ_HAS_HEADER, "maybe")
            .is_err());
    }

    /// Anything the server can run itself stays the server's.
    #[test]
    fn anything_else_is_left_to_the_server() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "INSERT INTO t SELECT * FROM u",
            "INSERT INTO t SELECT * FROM FILE.'a.csv'",
            "SELECT * FROM FILE.\"a.csv\"",
            "INSERT INTO t SELECT a + 1 FROM FILE.\"a.csv\"",
            "CREATE TABLE t AS SELECT * FROM FILE.\"a.csv\"",
        ] {
            assert!(recognize(sql).is_none(), "{sql} should go to the server");
        }
    }
}
