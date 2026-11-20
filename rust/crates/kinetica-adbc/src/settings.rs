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

//! What a caller can say about the connection itself: how it authenticates,
//! how long it waits, what it trusts, and which cluster it goes back to.
//!
//! Each of these is a setter on `KineticaOptions`; they are gathered here so
//! that the database holds one field rather than twenty, and so that a value
//! is refused when it is set rather than when the connection is opened.

use adbc_core::error::Result as AdbcResult;

use gpudb_api::{HaFailoverOrder, HaSynchronicityMode, KineticaOptions};

use crate::auth::Auth;
use crate::error::invalid;
use crate::options;

/// Seconds, as every timeout here is given, in the milliseconds the client
/// wants.
const MILLIS: u64 = 1_000;

/// The connection settings, holding only what a caller has actually said: an
/// option left alone keeps whatever the client defaults it to, rather than
/// having this repeat a default that may move.
#[derive(Debug, Clone)]
pub struct Settings {
    /// How the connection proves who it is, and the flow that gets it a
    /// token if it has to acquire one.
    auth: Auth,
    /// The cluster failover returns to once it is reachable again.
    primary_url: String,
    /// Which of a worker's addresses to use, where it has more than one.
    hostname_regex: String,
    trust_store_path: String,
    trust_store_password: String,
    bypass_ssl_cert_check: bool,
    /// Whether bulk record requests are Snappy-compressed.
    snappy: bool,
    failover: bool,
    auto_discovery: bool,
    failover_order: HaFailoverOrder,
    /// Seconds a request may take, 0 for no limit.
    request_timeout: Option<u64>,
    /// Seconds to wait for the server to answer.
    connect_timeout: Option<u64>,
    /// Seconds to spend on the first connection before giving up on a cluster.
    initial_connect_timeout: Option<u64>,
    /// Seconds between polls of the primary cluster, to fail back to it.
    failback_poll_interval: Option<u64>,
    max_retries: Option<usize>,
    /// How far a write must have replicated before the server answers, which
    /// is sent with every request rather than being a connect-time option.
    ha_sync_mode: HaSynchronicityMode,
    /// Where the log was last sent, so it can be read back. The log itself is
    /// the process's and is set as soon as it is named.
    log_path: String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            auth: Auth::default(),
            primary_url: String::new(),
            hostname_regex: String::new(),
            trust_store_path: String::new(),
            trust_store_password: String::new(),
            bypass_ssl_cert_check: false,
            snappy: true,
            failover: true,
            auto_discovery: true,
            failover_order: HaFailoverOrder::Sequential,
            request_timeout: None,
            connect_timeout: None,
            initial_connect_timeout: None,
            failback_poll_interval: None,
            max_retries: None,
            ha_sync_mode: HaSynchronicityMode::Default,
            log_path: String::new(),
        }
    }
}

impl Settings {
    /// Takes the option if it is one of these, leaving the caller to deal with
    /// the rest.
    /// @return Whether the name was one of these settings
    pub fn set(&mut self, name: &str, value: &str) -> AdbcResult<bool> {
        match name {
            options::PRIMARY_URL => self.primary_url = value.to_string(),
            options::HOSTNAME_REGEX => self.hostname_regex = value.to_string(),
            options::TRUST_STORE_PATH => self.trust_store_path = value.to_string(),
            options::TRUST_STORE_PASSWORD => self.trust_store_password = value.to_string(),
            options::BYPASS_SSL_CERT_CHECK => self.bypass_ssl_cert_check = flag(name, value)?,
            options::SNAPPY => self.snappy = flag(name, value)?,
            options::FAILOVER => self.failover = flag(name, value)?,
            options::AUTO_DISCOVERY => self.auto_discovery = flag(name, value)?,
            options::FAILOVER_ORDER => self.failover_order = order(value)?,
            options::REPLICATION => self.ha_sync_mode = replication(value)?,
            // Only a shorthand: a replication mode said outright wins, and
            // turning this off means nothing was asked for rather than that
            // the server should wait.
            options::NO_SYNC => {
                if flag(name, value)? && self.ha_sync_mode == HaSynchronicityMode::Default {
                    self.ha_sync_mode = HaSynchronicityMode::None;
                }
            }
            options::REQUEST_TIMEOUT => self.request_timeout = Some(number(name, value)?),
            options::CONNECT_TIMEOUT => self.connect_timeout = Some(number(name, value)?),
            options::INITIAL_CONNECT_TIMEOUT => {
                self.initial_connect_timeout = Some(number(name, value)?)
            }
            options::FAILBACK_POLL_INTERVAL => {
                self.failback_poll_interval = Some(number(name, value)?)
            }
            options::MAX_RETRIES => self.max_retries = Some(number(name, value)? as usize),
            // The log is the process's, not this connection's, so it is set
            // where it is said rather than when a connection is opened.
            options::LOG_LEVEL => crate::logging::set_level(value)?,
            options::LOG_PATH => {
                crate::logging::set_path(value)?;
                self.log_path = value.to_string();
            }
            // Anything the authentication takes, which is a prefix rather
            // than a list, so it is asked last.
            _ => return self.auth.set(name, value),
        }

        Ok(true)
    }

    /// Reads back what was set, which ADBC expects of any option a driver
    /// takes. The two secrets are write-only, as the password is.
    /// @return The value, or None if the name is not one of these settings
    pub fn get(&self, name: &str) -> Option<String> {
        let value = match name {
            options::PRIMARY_URL => self.primary_url.clone(),
            options::HOSTNAME_REGEX => self.hostname_regex.clone(),
            options::TRUST_STORE_PATH => self.trust_store_path.clone(),
            options::BYPASS_SSL_CERT_CHECK => self.bypass_ssl_cert_check.to_string(),
            options::SNAPPY => self.snappy.to_string(),
            options::FAILOVER => self.failover.to_string(),
            options::AUTO_DISCOVERY => self.auto_discovery.to_string(),
            options::FAILOVER_ORDER => match self.failover_order {
                HaFailoverOrder::Random => "random".to_string(),
                HaFailoverOrder::Sequential => "sequential".to_string(),
            },
            options::REPLICATION => self.ha_sync_mode.mode().to_string(),
            options::NO_SYNC => (self.ha_sync_mode == HaSynchronicityMode::None).to_string(),
            options::REQUEST_TIMEOUT => said(self.request_timeout)?,
            options::CONNECT_TIMEOUT => said(self.connect_timeout)?,
            options::INITIAL_CONNECT_TIMEOUT => said(self.initial_connect_timeout)?,
            options::FAILBACK_POLL_INTERVAL => said(self.failback_poll_interval)?,
            options::MAX_RETRIES => said(self.max_retries.map(|value| value as u64))?,
            options::LOG_LEVEL => crate::logging::level(),
            options::LOG_PATH => self.log_path.clone(),
            _ => return self.auth.get(name),
        };

        Some(value)
    }

    /// Puts what was said onto the options the client is built from, getting
    /// a token first if the caller asked for a flow rather than handing one
    /// over. Done here rather than at option-setting time, so that a sign-in
    /// a caller never uses is a sign-in that never happens.
    /// @return The name the token's own claims gave its holder, which the
    ///         caller puts on the options itself because it beats whatever it
    ///         set there, or None if nothing named one
    /// @return The replication mode every request carries, which the client
    ///         is told once it exists rather than through its options
    pub fn ha_sync_mode(&self) -> HaSynchronicityMode {
        self.ha_sync_mode
    }

    pub fn apply(&self, options: &mut KineticaOptions) -> AdbcResult<Option<String>> {
        let named = match self.auth.bearer()? {
            Some(bearer) => {
                options.set_oauth_token(bearer.token);
                bearer.user
            }
            None => None,
        };

        if !self.primary_url.is_empty() {
            options.set_primary_url(self.primary_url.clone());
        }

        if !self.hostname_regex.is_empty() {
            options.set_hostname_regex(Some(self.hostname_regex.clone()));
        }

        if !self.trust_store_path.is_empty() {
            options.set_trust_store_file_path(self.trust_store_path.clone());
            options.set_trust_store_password(self.trust_store_password.clone());
        }

        options
            .set_bypass_ssl_cert_check(self.bypass_ssl_cert_check)
            .set_use_snappy(self.snappy)
            .set_disable_failover(!self.failover)
            .set_disable_auto_discovery(!self.auto_discovery)
            .set_ha_failover_order(self.failover_order);

        if let Some(seconds) = self.request_timeout {
            options.set_timeout(seconds * MILLIS);
        }

        if let Some(seconds) = self.connect_timeout {
            options.set_server_connection_timeout(seconds * MILLIS);
        }

        if let Some(seconds) = self.initial_connect_timeout {
            options.set_initial_connection_attempt_timeout(seconds * MILLIS);
        }

        if let Some(seconds) = self.failback_poll_interval {
            options.set_failback_polling_interval(seconds);
        }

        if let Some(retries) = self.max_retries {
            options.set_max_retries(retries);
        }

        Ok(named)
    }
}

/// The `primary_url` the client is given names the cluster to come back to,
/// and the URL list names where to start. A caller with a ring gives the whole
/// ring as the URI, comma-separated.
/// @return The addresses to try, in the order they were given
pub fn urls(uri: &str) -> Vec<String> {
    uri.split(',')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(str::to_string)
        .collect()
}

/// @return The value, if it was said at all
fn said(value: Option<u64>) -> Option<String> {
    value.map(|value| value.to_string())
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

/// @return What the text says, as a whole number
fn number(name: &str, value: &str) -> AdbcResult<u64> {
    value
        .parse()
        .map_err(|_| invalid(&format!("{name} takes a whole number, not '{value}'")))
}

/// @return The failover order the text names
fn order(value: &str) -> AdbcResult<HaFailoverOrder> {
    match value {
        _ if value.eq_ignore_ascii_case("random") => Ok(HaFailoverOrder::Random),
        _ if value.eq_ignore_ascii_case("sequential") => Ok(HaFailoverOrder::Sequential),
        other => Err(invalid(&format!(
            "{} takes random or sequential, not '{other}'",
            options::FAILOVER_ORDER
        ))),
    }
}

/// Reads a replication mode, as the client reads one: the server's own name
/// (`REPL_SYNC`), the constant name (`SYNCHRONOUS`), or the mode without its
/// prefix (`sync`), in any case.
/// @return The mode named, or an error naming what is accepted
fn replication(value: &str) -> AdbcResult<HaSynchronicityMode> {
    // `default` is how a caller takes back a mode it set earlier, leaving the
    // decision to the server, so it is accepted along with the rest.
    HaSynchronicityMode::from_value(value).ok_or_else(|| {
        invalid(&format!(
            "{} takes sync, sync_parallel, async, async_parallel, none or \
             default, not '{value}'",
            options::REPLICATION
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The modes the client takes, by whichever of its names a caller uses.
    #[test]
    fn a_replication_mode_is_read_by_any_of_its_names() {
        let mut settings = Settings::default();

        assert_eq!(settings.ha_sync_mode(), HaSynchronicityMode::Default);

        for said in ["sync", "SYNC", "REPL_SYNC", "synchronous"] {
            settings.set(options::REPLICATION, said).expect(said);
            assert_eq!(
                settings.ha_sync_mode(),
                HaSynchronicityMode::Synchronous,
                "{said}"
            );
        }

        settings
            .set(options::REPLICATION, "async_parallel")
            .expect("async_parallel");
        assert_eq!(
            settings.ha_sync_mode(),
            HaSynchronicityMode::AsynchronousParallel
        );
    }

    /// A mode that means nothing to the client is refused where it is said.
    #[test]
    fn a_replication_mode_that_is_not_one_is_refused() {
        let mut settings = Settings::default();

        assert!(settings.set(options::REPLICATION, "eventually").is_err());
        assert_eq!(settings.ha_sync_mode(), HaSynchronicityMode::Default);
    }

    /// NoSync is shorthand for none, and gives way to a mode said outright,
    /// whichever order the two arrive in.
    #[test]
    fn no_sync_is_shorthand_and_yields_to_a_named_mode() {
        let mut settings = Settings::default();
        settings.set(options::NO_SYNC, "true").expect("no sync");
        assert_eq!(settings.ha_sync_mode(), HaSynchronicityMode::None);

        settings.set(options::REPLICATION, "sync").expect("sync");
        settings.set(options::NO_SYNC, "true").expect("no sync");
        assert_eq!(
            settings.ha_sync_mode(),
            HaSynchronicityMode::Synchronous,
            "a mode said outright is not undone by the shorthand"
        );

        let mut settings = Settings::default();
        settings.set(options::NO_SYNC, "false").expect("no sync");
        assert_eq!(
            settings.ha_sync_mode(),
            HaSynchronicityMode::Default,
            "turning it off asks for nothing, rather than asking the server to wait"
        );
    }

    #[test]
    fn one_url_is_one_address() {
        assert_eq!(urls("http://host:9191"), vec!["http://host:9191"]);
    }

    #[test]
    fn a_ring_is_given_as_a_list() {
        assert_eq!(
            urls("http://a:9191, http://b:9191 ,"),
            vec!["http://a:9191", "http://b:9191"]
        );
    }

    #[test]
    fn a_flag_is_read_either_way_round() {
        let mut settings = Settings::default();

        assert!(settings.set(options::SNAPPY, "FALSE").unwrap());
        assert_eq!(settings.get(options::SNAPPY).unwrap(), "false");

        assert!(settings.set(options::SNAPPY, "1").unwrap());
        assert_eq!(settings.get(options::SNAPPY).unwrap(), "true");
    }

    #[test]
    fn a_value_that_is_not_a_flag_is_refused_when_it_is_set() {
        let mut settings = Settings::default();

        let error = settings.set(options::FAILOVER, "maybe").unwrap_err();

        assert!(error.message.contains("true or false"), "{}", error.message);
    }

    #[test]
    fn a_failover_order_outside_the_two_is_refused() {
        let mut settings = Settings::default();

        assert!(settings.set(options::FAILOVER_ORDER, "random").unwrap());
        assert!(settings
            .set(options::FAILOVER_ORDER, "round-robin")
            .is_err());
    }

    #[test]
    fn a_name_that_is_not_one_of_these_is_left_to_the_caller() {
        let mut settings = Settings::default();

        assert!(!settings.set("adbc.kinetica.batch_size", "100").unwrap());
        assert!(settings.get("adbc.kinetica.batch_size").is_none());
    }

    #[test]
    fn a_timeout_is_said_in_seconds_and_sent_in_milliseconds() {
        let mut settings = Settings::default();
        settings.set(options::REQUEST_TIMEOUT, "30").unwrap();

        let mut options = KineticaOptions::new();
        settings.apply(&mut options).unwrap();

        assert_eq!(settings.get(options::REQUEST_TIMEOUT).unwrap(), "30");
        assert_eq!(options.timeout, 30_000);
    }

    #[test]
    fn a_timeout_nobody_set_is_not_reported_as_zero() {
        assert!(Settings::default().get(options::REQUEST_TIMEOUT).is_none());
    }

    #[test]
    fn the_two_disabling_flags_are_sent_the_way_round_the_client_wants() {
        let mut settings = Settings::default();
        settings.set(options::FAILOVER, "false").unwrap();

        let mut options = KineticaOptions::new();
        settings.apply(&mut options).unwrap();

        assert!(options.disable_failover);
        assert!(!options.disable_auto_discovery);
    }

    #[test]
    fn a_token_hands_back_the_name_its_claims_give_the_holder() {
        // {"sub":"analytics@example.com"}, with nothing signing it, since
        // only the issuing server could check a signature anyway.
        let token = concat!(
            "header.",
            "eyJzdWIiOiJhbmFseXRpY3NAZXhhbXBsZS5jb20ifQ.",
            "not-a-signature"
        );

        let mut settings = Settings::default();
        settings.set(options::AUTH_METHOD, "oauth").unwrap();
        settings.set(options::OAUTH_TOKEN, token).unwrap();

        let mut options = KineticaOptions::new();
        let named = settings.apply(&mut options).unwrap();

        assert_eq!(options.oauth_token, token, "the token is what gets sent");
        assert_eq!(
            named.as_deref(),
            Some("analytics@example.com"),
            "the caller puts this on the options, since it beats what it set"
        );
    }

    #[test]
    fn a_password_connection_names_nobody_and_sends_no_token() {
        let mut options = KineticaOptions::new();
        let named = Settings::default().apply(&mut options).unwrap();

        assert_eq!(named, None);
        assert!(options.oauth_token.is_empty());
    }

    #[test]
    fn the_trust_store_password_is_write_only() {
        let mut settings = Settings::default();
        settings
            .set(options::TRUST_STORE_PASSWORD, "secret")
            .unwrap();

        assert!(settings.get(options::TRUST_STORE_PASSWORD).is_none());
    }
}
