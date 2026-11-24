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

//! `kinetica://` URIs, the scheme a driver manager knows this driver by.
//!
//! ```text
//! kinetica://[user[:password]@]host[:port][,host[:port]...][/path][?name=value&...]
//! ```
//!
//! Several hosts are an HA ring, tried in the order given. The connection is
//! made over HTTPS unless `tls=false` says otherwise. Every other parameter is
//! an option under its own name -- `username`, `password`,
//! `kinetica.query.max_rows` -- and an option set on the database directly
//! beats one the URI gives. An `http://` or `https://` URI is taken as it is.

use adbc_core::error::Result as AdbcResult;
use percent_encoding::percent_decode_str;

use crate::error::invalid;

/// The scheme, as a driver manager matches it to this driver.
const SCHEME: &str = "kinetica";

/// The parameter that turns TLS off, which is not an option of its own.
const TLS: &str = "tls";

/// What a `kinetica://` URI says.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct KineticaUri {
    /// The addresses to connect to, as the URLs the client takes,
    /// comma-separated as the `uri` option takes them.
    pub(crate) urls: String,
    /// The user, the password and the options it names, in the order given.
    pub(crate) options: Vec<(String, String)>,
}

/// @return What a `kinetica://` URI says, or None for a URI in any other
///     scheme, which is taken as it is
pub(crate) fn parse(uri: &str) -> AdbcResult<Option<KineticaUri>> {
    let Some((scheme, rest)) = uri.trim().split_once("://") else {
        return Ok(None);
    };

    if !scheme.eq_ignore_ascii_case(SCHEME) {
        return Ok(None);
    }

    let (before_query, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (authority, path) = match before_query.find('/') {
        Some(slash) => before_query.split_at(slash),
        None => (before_query, ""),
    };
    // A password may hold an '@' of its own, written as %40; the last one
    // is where the hosts start.
    let (user_info, hosts) = match authority.rsplit_once('@') {
        Some((user_info, hosts)) => (Some(user_info), hosts),
        None => (None, authority),
    };

    let hosts: Vec<&str> = hosts.split(',').map(str::trim).collect();

    if hosts.iter().any(|host| host.is_empty()) {
        return Err(invalid(&format!(
            "the URI '{}' names no host where one is expected; it takes \
             kinetica://host[:port][,host[:port]...][/path][?name=value&...]",
            without_password(uri)
        )));
    }

    let mut tls = true;
    let mut options = Vec::new();

    if let Some(user_info) = user_info {
        let (user, password) = match user_info.split_once(':') {
            Some((user, password)) => (user, Some(password)),
            None => (user_info, None),
        };

        options.push(("username".to_string(), decoded(user)));

        if let Some(password) = password {
            options.push(("password".to_string(), decoded(password)));
        }
    }

    for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if name == TLS {
            tls = match value.as_ref() {
                said if said.eq_ignore_ascii_case("true") || said == "1" => true,
                said if said.eq_ignore_ascii_case("false") || said == "0" => false,
                other => {
                    return Err(invalid(&format!(
                        "the URI parameter {TLS} takes true or false, not '{other}'"
                    )));
                }
            };
        } else if name == "uri" {
            return Err(invalid("a kinetica:// URI cannot name another URI"));
        } else {
            options.push((name.into_owned(), value.into_owned()));
        }
    }

    let scheme = if tls { "https" } else { "http" };
    let urls = hosts
        .iter()
        .map(|host| format!("{scheme}://{host}{path}"))
        .collect::<Vec<_>>()
        .join(",");

    Ok(Some(KineticaUri { urls, options }))
}

/// @return The text with its percent escapes resolved
fn decoded(text: &str) -> String {
    percent_decode_str(text).decode_utf8_lossy().into_owned()
}

/// @return The URI with any password in it taken out, fit for a message
fn without_password(uri: &str) -> String {
    match uri.split_once("://") {
        Some((scheme, rest)) => match rest.split_once('@') {
            Some((user_info, hosts)) => {
                let user = user_info.split(':').next().unwrap_or_default();
                format!("{scheme}://{user}@{hosts}")
            }
            None => uri.to_string(),
        },
        None => uri.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(uri: &str) -> KineticaUri {
        parse(uri).expect("valid").expect("a kinetica URI")
    }

    #[test]
    fn another_scheme_is_taken_as_it_is() {
        assert_eq!(parse("http://localhost:9191").unwrap(), None);
        assert_eq!(parse("https://kinetica.example/gpudb-0").unwrap(), None);
    }

    #[test]
    fn a_host_is_reached_over_https_unless_tls_is_turned_off() {
        assert_eq!(
            parsed("kinetica://localhost:9191").urls,
            "https://localhost:9191"
        );
        assert_eq!(
            parsed("kinetica://localhost:9191?tls=false").urls,
            "http://localhost:9191"
        );
        assert_eq!(parsed("KINETICA://h:1?tls=TRUE").urls, "https://h:1");
    }

    #[test]
    fn a_path_goes_with_every_host_of_a_ring() {
        assert_eq!(
            parsed("kinetica://a:8082, b:8082/gpudb-0?tls=false").urls,
            "http://a:8082/gpudb-0,http://b:8082/gpudb-0"
        );
    }

    #[test]
    fn the_user_and_options_are_read_in_the_order_given() {
        let uri = parsed(
            "kinetica://me:p%40ss%3Aword@h:9191/?kinetica.query.max_rows=10&tls=false\
             &kinetica.session_id=a%20b",
        );

        assert_eq!(uri.urls, "http://h:9191/");
        assert_eq!(
            uri.options,
            vec![
                ("username".to_string(), "me".to_string()),
                ("password".to_string(), "p@ss:word".to_string()),
                ("kinetica.query.max_rows".to_string(), "10".to_string()),
                ("kinetica.session_id".to_string(), "a b".to_string()),
            ]
        );
    }

    #[test]
    fn a_user_without_a_password_names_only_the_user() {
        assert_eq!(
            parsed("kinetica://me@h").options,
            vec![("username".to_string(), "me".to_string())]
        );
    }

    #[test]
    fn a_missing_host_is_refused_without_the_password() {
        let refused = parse("kinetica://me:secret@h,,g").expect_err("an empty host");

        assert!(
            refused.message.contains("names no host"),
            "{}",
            refused.message
        );
        assert!(!refused.message.contains("secret"), "{}", refused.message);
    }

    #[test]
    fn a_tls_value_that_is_not_a_flag_is_refused() {
        let refused = parse("kinetica://h?tls=maybe").expect_err("not a flag");

        assert!(refused.message.contains("tls"), "{}", refused.message);
    }

    #[test]
    fn a_uri_naming_another_uri_is_refused() {
        assert!(parse("kinetica://h?uri=http://other").is_err());
    }
}
