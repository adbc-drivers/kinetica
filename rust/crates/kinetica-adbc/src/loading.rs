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

//! How an ingest behaves: how many rows go at a time, whether they go
//! straight to the workers, and what happens to a row the workers will not
//! take.
//!
//! These are read when an inserter is built. A connection that changes one
//! afterwards gets an inserter built again for the next table it loads, which
//! `Inserters` arranges by holding what each of its own was built with.

use adbc_core::error::Result as AdbcResult;

use gpudb_api::{BulkInserterOptions, WorkerList};

use crate::error::invalid;
use crate::options;

/// Rows per batch by default, which is what the multi-head path is tuned for.
pub const DEFAULT_ROWS_PER_INSERTION: usize = 50_000;

/// What becomes of a row the workers will not take.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ErrorMode {
    /// The batch it is in fails, and the caller is told. The default, and
    /// what this driver has always done.
    #[default]
    Abort,
    /// The rest of the batch is taken and the row is dropped.
    Skip,
    /// The rest of the batch is taken and the server says what was wrong with
    /// each row it refused.
    Permissive,
}

/// What a caller has said about how its rows should be loaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Loading {
    pub error_mode: ErrorMode,
    pub truncate_strings: bool,
    pub rows_per_insertion: usize,
    /// Whether rows go straight to the workers rather than the head rank.
    pub multihead: bool,
}

impl Default for Loading {
    fn default() -> Self {
        Self {
            error_mode: ErrorMode::default(),
            truncate_strings: false,
            rows_per_insertion: DEFAULT_ROWS_PER_INSERTION,
            multihead: true,
        }
    }
}

impl Loading {
    /// Takes the option if it is one of these.
    /// @return Whether the name was one of these options
    pub fn set(&mut self, name: &str, value: &str) -> AdbcResult<bool> {
        match name {
            options::ERROR_MODE => self.error_mode = mode(value)?,
            options::TRUNCATE_STRINGS => self.truncate_strings = flag(name, value)?,
            options::MULTIHEAD_INSERT => self.multihead = flag(name, value)?,
            options::ROWS_PER_INSERTION => {
                let rows = value
                    .parse()
                    .map_err(|_| invalid(&format!("{name} takes a whole number, not '{value}'")))?;

                if rows == 0 {
                    return Err(invalid(&format!("{name} takes at least one row")));
                }

                self.rows_per_insertion = rows;
            }
            _ => return Ok(false),
        }

        Ok(true)
    }

    /// @return What was set, or None if the name is not one of these options
    pub fn get(&self, name: &str) -> Option<String> {
        let value = match name {
            options::ERROR_MODE => match self.error_mode {
                ErrorMode::Abort => "abort".to_string(),
                ErrorMode::Skip => "skip".to_string(),
                ErrorMode::Permissive => "permissive".to_string(),
            },
            options::TRUNCATE_STRINGS => self.truncate_strings.to_string(),
            options::MULTIHEAD_INSERT => self.multihead.to_string(),
            options::ROWS_PER_INSERTION => self.rows_per_insertion.to_string(),
            _ => return None,
        };

        Some(value)
    }

    /// Builds what the inserter is made with. The two primary-key options are
    /// the same ones a statement carries, since a caller saying what should
    /// happen to a key already there means it whichever way the rows arrive.
    /// @return The inserter options for this loading
    pub fn inserter_options(
        &self,
        update_on_existing_pk: bool,
        ignore_existing_pk: bool,
    ) -> BulkInserterOptions {
        let mut options = BulkInserterOptions::new();

        options
            .set_truncate_strings(self.truncate_strings)
            .set_update_on_existing_pk(update_on_existing_pk)
            .set_ignore_existing_pk(ignore_existing_pk);

        match self.error_mode {
            // What the server does unless told otherwise: a batch with a bad
            // row in it is refused whole.
            ErrorMode::Abort => {}
            ErrorMode::Skip => {
                options.set_allow_partial_batch(true);
            }
            ErrorMode::Permissive => {
                options
                    .set_allow_partial_batch(true)
                    .set_return_individual_errors(true);
            }
        }

        // An empty list is how the client is told not to go to the workers;
        // None, the default, means find them.
        if !self.multihead {
            options.set_worker_list(Some(WorkerList::empty()));
        }

        options
    }
}

/// @return What the text says, as a flag
fn flag(name: &str, value: &str) -> AdbcResult<bool> {
    match value {
        _ if value.eq_ignore_ascii_case("true") || value == "1" => Ok(true),
        _ if value.eq_ignore_ascii_case("false") || value == "0" => Ok(false),
        other => Err(invalid(&format!(
            "{name} takes true or false, not '{other}'"
        ))),
    }
}

/// @return The error mode the text names
fn mode(value: &str) -> AdbcResult<ErrorMode> {
    match value {
        _ if value.eq_ignore_ascii_case("abort") => Ok(ErrorMode::Abort),
        _ if value.eq_ignore_ascii_case("skip") => Ok(ErrorMode::Skip),
        _ if value.eq_ignore_ascii_case("permissive") => Ok(ErrorMode::Permissive),
        other => Err(invalid(&format!(
            "{} takes abort, skip or permissive, not '{other}'",
            options::ERROR_MODE
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_said_is_what_the_driver_always_did() {
        let loading = Loading::default();

        assert_eq!(loading.error_mode, ErrorMode::Abort);
        assert!(!loading.truncate_strings);
        assert!(loading.multihead);
        assert_eq!(loading.rows_per_insertion, DEFAULT_ROWS_PER_INSERTION);
    }

    #[test]
    fn an_error_mode_is_read_back_as_it_was_given() {
        let mut loading = Loading::default();
        loading.set(options::ERROR_MODE, "SKIP").unwrap();

        assert_eq!(loading.error_mode, ErrorMode::Skip);
        assert_eq!(loading.get(options::ERROR_MODE).unwrap(), "skip");
    }

    #[test]
    fn an_error_mode_outside_the_three_is_refused() {
        let mut loading = Loading::default();

        let error = loading.set(options::ERROR_MODE, "ignore").unwrap_err();

        assert!(
            error.message.contains("abort, skip or permissive"),
            "{}",
            error.message
        );
    }

    #[test]
    fn a_batch_of_no_rows_would_never_be_sent() {
        let mut loading = Loading::default();

        assert!(loading.set(options::ROWS_PER_INSERTION, "0").is_err());
        assert!(loading.set(options::ROWS_PER_INSERTION, "1").is_ok());
    }

    #[test]
    fn declining_the_workers_is_said_with_an_empty_list() {
        let mut loading = Loading::default();
        loading.set(options::MULTIHEAD_INSERT, "false").unwrap();

        let options = loading.inserter_options(false, false);

        assert!(options
            .worker_list
            .as_ref()
            .is_some_and(|workers| workers.is_empty()));
    }

    #[test]
    fn going_to_the_workers_leaves_the_client_to_find_them() {
        let options = Loading::default().inserter_options(false, false);

        assert!(options.worker_list.is_none());
    }

    #[test]
    fn a_name_that_is_not_one_of_these_is_left_to_the_caller() {
        let mut loading = Loading::default();

        assert!(!loading.set("adbc.kinetica.batch_size", "10").unwrap());
        assert!(loading.get("adbc.kinetica.batch_size").is_none());
    }
}
