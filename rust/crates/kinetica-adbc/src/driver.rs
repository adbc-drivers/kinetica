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

//! The driver, which is the entry point every other object is reached from.

use adbc_core::error::Result as AdbcResult;
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::sync::Driver;
use adbc_core::Optionable;

use crate::database::KineticaDatabase;

// --------------------------------------------------------------------------
// Driver

#[derive(Debug, Default, Clone)]
pub struct KineticaDriver {}

impl Driver for KineticaDriver {
    type DatabaseType = KineticaDatabase;

    fn new_database(&mut self) -> AdbcResult<Self::DatabaseType> {
        Ok(KineticaDatabase::default())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> AdbcResult<Self::DatabaseType> {
        let mut database = KineticaDatabase::default();

        for (key, value) in opts {
            database.set_option(key, value)?;
        }

        Ok(database)
    }
}
