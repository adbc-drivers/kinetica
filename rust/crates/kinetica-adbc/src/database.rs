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

//! The database: where a connection's address and credentials are held until
//! one is opened.

use adbc_core::error::Result as AdbcResult;
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::sync::Database;
use adbc_core::Optionable;

use gpudb_api::{Kinetica, KineticaOptions, KineticaVersion};

use crate::connection::KineticaConnection;
use crate::context::{self, Context};
use crate::error::{not_found, to_adbc, unsupported, unsupported_server};
use crate::runtime;
use crate::settings::{self, Settings};

/// What the server records as the client that sent a request, which is how a
/// log tells this driver from the others.
const CLIENT_NAME: &str = "kinetica-adbc";

// --------------------------------------------------------------------------
// Database

/// Holds what is needed to connect; no connection is opened until asked.
#[derive(Debug, Default, Clone)]
pub struct KineticaDatabase {
    uri: String,
    username: String,
    password: String,
    /// Whether the connections opened here accept the transaction calls and
    /// do nothing, rather than refuse them.
    fake_transactions: bool,
    /// The server session the connections opened here put their SQL `#name`
    /// temp tables in; empty gives each one a session of its own.
    session_id: String,
    /// The user the connections opened here act as; empty acts as nobody but
    /// the signed-in user.
    impersonate_user: String,
    /// Everything else a caller can say about the connection itself.
    settings: Settings,
    /// What the connections opened here start out knowing -- the server
    /// options to put on a statement, and the cap on a read -- which each one
    /// can then change for itself.
    context: Context,
}

/// The first Kinetica version that supports Arrow encoding
pub const ARROW_VERSION: KineticaVersion = KineticaVersion::new(7, 2, 3, 24);

/// Whether the given server supports Arrow encoding.
/// @return An error naming the server's version and the one needed, if not
fn supported(version: Option<&KineticaVersion>, url: &str) -> AdbcResult<()> {
    match version {
        Some(version) if *version >= ARROW_VERSION => Ok(()),
        Some(version) => Err(unsupported_server(&format!(
            "the Kinetica server at {url} is version {}; this driver needs {ARROW_VERSION} or \
             later, the first to return query results as Arrow",
            version.full_version()
        ))),
        None => Err(unsupported_server(&format!(
            "the Kinetica server at {url} did not report its version; this driver needs \
             {ARROW_VERSION} or later"
        ))),
    }
}

impl Optionable for KineticaDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> AdbcResult<()> {
        let text = match value {
            OptionValue::String(v) => v,
            _ => return Err(unsupported("only string database options are supported")),
        };

        match key {
            OptionDatabase::Uri => self.uri = text,
            OptionDatabase::Username => self.username = text,
            OptionDatabase::Password => self.password = text,
            OptionDatabase::Other(ref name) if name == crate::options::FAKE_TRANSACTIONS => {
                self.fake_transactions = text.eq_ignore_ascii_case("true");
            }
            OptionDatabase::Other(ref name) if name == crate::options::SESSION_ID => {
                self.session_id = text;
            }
            OptionDatabase::Other(ref name) if name == crate::options::IMPERSONATE_USER => {
                self.impersonate_user = text;
            }
            OptionDatabase::Other(ref name) if self.settings.set(name, &text)? => {}
            OptionDatabase::Other(ref name) if name == crate::options::MAX_ROWS => {
                self.context.max_rows = Some(context::rows(name, &text)?);
            }
            OptionDatabase::Other(ref name) if name == crate::options::USE_KEY_LOOKUP => {
                self.context.use_key_lookup = context::flag(name, &text)?;
            }
            OptionDatabase::Other(ref name) if self.context.loading.set(name, &text)? => {}
            OptionDatabase::Other(ref name) if self.context.file_read.set(name, &text)? => {}
            OptionDatabase::Other(ref name) if self.context.query.set(name, &text)? => {}
            other => return Err(unsupported(&format!("database option {}", other.as_ref()))),
        }

        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> AdbcResult<String> {
        match key {
            OptionDatabase::Uri => Ok(self.uri.clone()),
            OptionDatabase::Username => Ok(self.username.clone()),
            OptionDatabase::Other(ref name) if name == crate::options::FAKE_TRANSACTIONS => {
                Ok(self.fake_transactions.to_string())
            }
            OptionDatabase::Other(ref name) if name == crate::options::SESSION_ID => {
                Ok(self.session_id.clone())
            }
            OptionDatabase::Other(ref name) if name == crate::options::IMPERSONATE_USER => {
                Ok(self.impersonate_user.clone())
            }
            OptionDatabase::Other(ref name) if name == crate::options::MAX_ROWS => self
                .context
                .max_rows
                .map(|rows| rows.to_string())
                .ok_or_else(|| not_found(crate::options::MAX_ROWS)),
            OptionDatabase::Other(ref name) if name == crate::options::USE_KEY_LOOKUP => {
                Ok(self.context.use_key_lookup.to_string())
            }
            OptionDatabase::Other(ref name) => self
                .settings
                .get(name)
                .or_else(|| self.context.loading.get(name))
                .or_else(|| self.context.file_read.get(name))
                .or_else(|| self.context.query.get(name))
                .ok_or_else(|| not_found(&format!("database option {name}"))),
            other => Err(not_found(&format!("database option {}", other.as_ref()))),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> AdbcResult<Vec<u8>> {
        Err(not_found(&format!("bytes option {}", key.as_ref())))
    }

    fn get_option_int(&self, key: Self::Option) -> AdbcResult<i64> {
        Err(not_found(&format!("integer option {}", key.as_ref())))
    }

    fn get_option_double(&self, key: Self::Option) -> AdbcResult<f64> {
        Err(not_found(&format!("double option {}", key.as_ref())))
    }
}

impl Database for KineticaDatabase {
    type ConnectionType = KineticaConnection;

    fn new_connection(&self) -> AdbcResult<Self::ConnectionType> {
        // One address or a whole ring, comma-separated; the client tries them
        // in the order they were given.
        let addresses = settings::urls(&self.uri);
        log::debug!("Kinetica ADBC Driver {} connecting", crate::DRIVER_VERSION);

        let mut options = KineticaOptions::new();
        options
            // The first of them, since a list is where to start rather than
            // which cluster to prefer; a caller that has a preference names it
            // with the primary_url option below.
            .set_primary_url(addresses.first().cloned().unwrap_or_default())
            .set_username(self.username.clone())
            .set_password(self.password.clone())
            // The release version, as `get_info` reports it, without the
            // build number, as the Rust API's own `User-Agent` entry has it.
            .set_client_info(Some(CLIENT_NAME), Some(crate::VERSION));

        // Said after the three above, so that a caller naming a primary
        // cluster of its own is not overwritten by the address it started at.
        // This is also where a sign-in happens, if one was asked for.
        let named = self.settings.apply(&mut options)?;

        // A token names its own subject, and that beats what the username
        // option said: the one is a guess at the identity, the other is the
        // identity the server will see.
        if let Some(user) = named {
            options.set_username(user);
        }

        // A session the caller named is the caller's to drop, which the client
        // knows: it arms no drop for one, so closing the connection leaves its
        // temp tables where a session of the connection's own would lose them.
        if !self.session_id.is_empty() {
            options.set_session_id(Some(&self.session_id));
        }

        let client = runtime::block_on(Kinetica::new(addresses, options)).map_err(to_adbc)?;

        // Error out if the server doesn't support Arrow encoding.
        let url = client.url().map(|url| url.to_string()).unwrap_or_default();

        if let Err(refused) = supported(client.server_version(), &url) {
            // Its session ends now rather than when the server gives up on it.
            runtime::block_on(client.close());

            return Err(refused);
        }

        // Replication rides on every request rather than being settled at
        // connect time, so the client is told once it exists.
        client.set_ha_sync_mode(self.settings.ha_sync_mode());

        KineticaConnection::new(
            client,
            self.fake_transactions,
            self.context.clone(),
            &self.impersonate_user,
        )
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> AdbcResult<Self::ConnectionType> {
        let mut connection = self.new_connection()?;

        for (key, value) in opts {
            connection.set_option(key, value)?;
        }

        Ok(connection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "http://127.0.0.1:9191/";

    #[test]
    fn the_first_version_with_arrow_and_every_later_one_is_accepted() {
        for version in [
            KineticaVersion::with_build(7, 2, 3, 24, 0),
            KineticaVersion::with_build(7, 2, 3, 24, 20_261_019_000_000),
            KineticaVersion::new(7, 2, 3, 25),
            KineticaVersion::new(7, 2, 4, 0),
            KineticaVersion::new(7, 3, 0, 0),
            KineticaVersion::new(8, 0, 0, 0),
        ] {
            assert!(
                supported(Some(&version), URL).is_ok(),
                "{}",
                version.full_version()
            );
        }
    }

    #[test]
    fn an_earlier_version_is_refused_with_both_versions_named() {
        let shared = KineticaVersion::with_build(7, 2, 3, 23, 20_261_014_183_705);

        let refused = supported(Some(&shared), URL).expect_err("7.2.3.23 has no Arrow");

        assert_eq!(refused.status, adbc_core::error::Status::NotImplemented);
        assert!(
            refused.message.contains("7.2.3.23.20261014183705"),
            "{}",
            refused.message
        );
        assert!(refused.message.contains("7.2.3.24"), "{}", refused.message);
        assert!(refused.message.contains(URL), "{}", refused.message);

        for version in [
            KineticaVersion::new(7, 2, 3, 0),
            KineticaVersion::new(7, 2, 2, 99),
            KineticaVersion::new(7, 1, 9, 50),
            KineticaVersion::new(6, 2, 0, 0),
        ] {
            assert!(
                supported(Some(&version), URL).is_err(),
                "{}",
                version.version()
            );
        }
    }

    #[test]
    fn a_server_that_does_not_say_its_version_is_refused() {
        assert!(supported(None, URL).is_err());
    }
}
