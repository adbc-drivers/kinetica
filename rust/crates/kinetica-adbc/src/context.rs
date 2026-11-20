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

//! What a connection has been told, which every statement opened on it reads.
//!
//! One lock rather than one per setting: a statement wants all of it at once,
//! when it is about to send a request, and a caller changing a schema between
//! two statements should not have to think about which half a statement saw.

use std::sync::{Arc, Mutex};

use adbc_core::error::Result as AdbcResult;

use crate::error::invalid;

use crate::file::FileRead;
use crate::loading::Loading;
use crate::query::QueryOptions;

/// The connection's own state, shared with its statements.
#[derive(Debug, Clone)]
pub struct Context {
    /// The schema unqualified names are looked for in.
    pub schema: Option<String>,
    /// The catalog the statement runs against, where the server federates
    /// more than its own.
    pub catalog: Option<String>,
    /// The server options to put on every statement sent from here.
    pub query: QueryOptions,
    /// The most rows a read hands back, however many the result holds.
    /// None, the default, hands back all of them.
    pub max_rows: Option<i64>,
    /// Whether a read that pins every key column goes to the rank holding the
    /// row. Off unless a caller asks for it.
    pub use_key_lookup: bool,
    /// How an ingest from here behaves.
    pub loading: Loading,
    /// How a local file named by `INSERT ... FROM FILE` is read.
    pub file_read: FileRead,
}

/// Written out rather than derived, so that every default is stated in one
/// place rather than being whatever `Default` would give the field's type.
impl Default for Context {
    fn default() -> Self {
        Self {
            schema: None,
            catalog: None,
            query: QueryOptions::default(),
            max_rows: None,
            use_key_lookup: false,
            loading: Loading::default(),
            file_read: FileRead::default(),
        }
    }
}

/// The context as it is held: by the connection and by every statement, so
/// that a change made on the connection reaches a statement already open.
pub type Shared = Arc<Mutex<Context>>;

impl Context {
    /// Takes a copy, which is what a statement carries into the request it is
    /// about to send rather than holding the lock across it.
    /// @return What the connection has been told, as of now
    pub fn read(shared: &Shared) -> Self {
        shared.lock().unwrap().clone()
    }
}

/// @return What the text says, for an option that is on or off
pub fn flag(name: &str, value: &str) -> AdbcResult<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(invalid(&format!(
            "{name} takes true or false, not '{value}'"
        ))),
    }
}

/// @return The cap the text names, as a count of rows
pub fn rows(name: &str, value: &str) -> AdbcResult<i64> {
    value
        .parse()
        .map_err(|_| invalid(&format!("{name} takes a whole number, not '{value}'")))
}
