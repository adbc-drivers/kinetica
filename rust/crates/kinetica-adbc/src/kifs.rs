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

//! Moving files between this host and the server's own file system.
//!
//! `UPLOAD` and `DOWNLOAD` are not SQL the server can run: the files are
//! here, not there, so the statement is carried out by the client and never
//! sent.

use std::sync::OnceLock;

use adbc_core::error::Result as AdbcResult;
use regex::Regex;

use gpudb_api::filesystem::KifsFileHandler;

use crate::error::{could_not, invalid};
use crate::runtime;
use crate::session::Session;

/// Which way the files go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// From this host to the server.
    Up,
    /// From the server to this host.
    Down,
}

/// A statement that moves files rather than rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transfer {
    pub direction: Direction,
    /// What to move: local paths going up, server paths coming down. A path
    /// may be a glob, which is resolved where the files are.
    pub sources: Vec<String>,
    /// Where they land: a server directory going up, a local one coming
    /// down.
    pub target: String,
}

/// `UPLOAD|DOWNLOAD [FILE[S]|DIR[ECTORY]] '<path>'[, '<path>']* INTO
/// '<path>'`, with a trailing `WITH OPTIONS (...)` allowed and ignored:
/// nothing it can carry changes what this does.
fn statement() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();

    PATTERN.get_or_init(|| {
        Regex::new(
            r"(?is)^\s*(UP|DOWN)LOAD\s+(?:FILES?|DIR(?:ECTORY)?)?\s*('[^']*'(?:\s*,\s*'[^']*')*)\s+INTO\s+'([^']*)'\s*(?:WITH\s+OPTIONS\s*\(.*\)\s*)?;?\s*$",
        )
        .expect("the transfer pattern is a pattern")
    })
}

/// One quoted path of the source list.
fn quoted() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();

    PATTERN.get_or_init(|| Regex::new(r"'([^']*)'").expect("the path pattern is a pattern"))
}

/// @return The transfer, if the statement is one
pub fn recognize(sql: &str) -> Option<Transfer> {
    let found = statement().captures(sql)?;

    let direction = match found.get(1)?.as_str().to_ascii_uppercase().as_str() {
        "UP" => Direction::Up,
        _ => Direction::Down,
    };

    let sources: Vec<String> = quoted()
        .captures_iter(found.get(2)?.as_str())
        .filter_map(|path| path.get(1).map(|path| path.as_str().to_string()))
        .filter(|path| !path.is_empty())
        .collect();

    if sources.is_empty() {
        return None;
    }

    Some(Transfer {
        direction,
        sources,
        target: found.get(3)?.as_str().to_string(),
    })
}

impl Transfer {
    /// Moves the files, making the server directory first when sending.
    ///
    /// Each source is handled on its own, so a list of three paths is three
    /// transfers; the handler resolves a glob among them where the files are.
    ///
    /// @return How many files were moved
    pub(crate) fn run(&self, session: &Session) -> AdbcResult<i64> {
        let handler = KifsFileHandler::new(std::sync::Arc::clone(session.client()), None);

        runtime::block_on(async {
            let mut moved = 0;

            if self.direction == Direction::Up {
                // A directory that is already there is not an error; one that
                // is missing would fail every upload after it.
                handler
                    .create_directory(&self.target, true)
                    .await
                    .map_err(could_not("create the KiFS directory"))?;
            }

            for source in &self.sources {
                let results = match self.direction {
                    Direction::Up => handler.upload(source, &self.target, None, None).await,
                    Direction::Down => handler.download(source, &self.target, None, None).await,
                }
                .map_err(could_not("transfer the file"))?;

                moved += counted(&results);
            }

            Ok(moved)
        })
    }

    /// @return An error for a transfer that names nothing to move to
    pub fn checked(self) -> AdbcResult<Self> {
        match self.target.is_empty() {
            true => Err(invalid("a transfer has to say where the files go")),
            false => Ok(self),
        }
    }
}

/// Files sent whole go in batches, and the handler reports one result per
/// batch, naming the files it held; a file big enough to be split reports one
/// result of its own. Counting results would therefore count three small
/// files as one.
/// @return How many files the results account for
fn counted(results: &[gpudb_api::filesystem::OperationResult]) -> i64 {
    results
        .iter()
        .map(|result| match result.full_file_names.len() {
            0 => 1,
            named => named,
        } as i64)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_upload_names_what_goes_and_where() {
        let found = recognize("UPLOAD FILE '/tmp/one.csv' INTO 'landing'").expect("an upload");

        assert_eq!(found.direction, Direction::Up);
        assert_eq!(found.sources, vec!["/tmp/one.csv"]);
        assert_eq!(found.target, "landing");
    }

    #[test]
    fn a_download_goes_the_other_way() {
        let found = recognize("download files 'landing/one.csv' into '/tmp'").expect("a download");

        assert_eq!(found.direction, Direction::Down);
        assert_eq!(found.sources, vec!["landing/one.csv"]);
        assert_eq!(found.target, "/tmp");
    }

    #[test]
    fn several_paths_are_all_taken() {
        let found =
            recognize("UPLOAD FILES '/tmp/a.csv', '/tmp/b.csv' INTO 'landing'").expect("an upload");

        assert_eq!(found.sources, vec!["/tmp/a.csv", "/tmp/b.csv"]);
    }

    #[test]
    fn the_word_naming_the_kind_is_optional() {
        assert!(recognize("UPLOAD '/tmp/a.csv' INTO 'landing'").is_some());
        assert!(recognize("UPLOAD DIRECTORY '/tmp/data' INTO 'landing'").is_some());
        assert!(recognize("UPLOAD DIR '/tmp/data' INTO 'landing'").is_some());
    }

    #[test]
    fn trailing_options_do_not_stop_it_being_recognized() {
        let found = recognize("UPLOAD FILE '/tmp/a.csv' INTO 'landing' WITH OPTIONS (x = 'y')")
            .expect("an upload");

        assert_eq!(found.target, "landing");
    }

    #[test]
    fn a_trailing_semicolon_is_not_part_of_the_path() {
        let found = recognize("UPLOAD FILE '/tmp/a.csv' INTO 'landing';").expect("an upload");

        assert_eq!(found.target, "landing");
    }

    #[test]
    fn a_glob_is_left_for_the_handler_to_resolve() {
        let found = recognize("UPLOAD FILES '/tmp/*.csv' INTO 'landing'").expect("an upload");

        assert_eq!(found.sources, vec!["/tmp/*.csv"]);
    }

    #[test]
    fn anything_else_is_the_servers_to_run() {
        assert!(recognize("SELECT * FROM t").is_none());
        assert!(recognize("INSERT INTO uploads VALUES (1)").is_none());
        // A table that happens to be called upload is not this statement.
        assert!(recognize("SELECT * FROM upload").is_none());
        // Nowhere to put them.
        assert!(recognize("UPLOAD FILE '/tmp/a.csv'").is_none());
    }

    #[test]
    fn a_transfer_with_nowhere_to_go_is_refused() {
        let found = recognize("UPLOAD FILE '/tmp/a.csv' INTO ''").expect("an upload");

        assert!(found.checked().is_err());
    }
}
