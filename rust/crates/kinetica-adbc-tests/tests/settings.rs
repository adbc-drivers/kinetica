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

//! The connection settings: that a caller can say them, read them back, and
//! open a connection with them on. What each one does to the wire is the
//! client's business and is tested there; what is tested here is that the
//! driver carries them rather than dropping them, and that a value it cannot
//! use is refused at once rather than at connect time.

use adbc_core::error::Status;
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::sync::{Connection, Database, Driver, Statement};
use adbc_core::Optionable;
use adbc_driver_kinetica::{options, KineticaDriver};

/// @return The address, user and password the tests run against
fn credentials() -> (String, String, String) {
    let kinetica_adbc_tests::Login {
        url,
        user,
        password,
    } = kinetica_adbc_tests::login();

    (url, user, password)
}

/// @return A database with the given settings on it, not yet connected
fn database_with<I>(extra: I) -> adbc_driver_kinetica::KineticaDatabase
where
    I: IntoIterator<Item = (OptionDatabase, OptionValue)>,
{
    let (url, user, password) = credentials();

    let mut options = vec![
        (OptionDatabase::Uri, OptionValue::String(url)),
        (OptionDatabase::Username, OptionValue::String(user)),
        (OptionDatabase::Password, OptionValue::String(password)),
    ];
    options.extend(extra);

    KineticaDriver::default()
        .new_database_with_opts(options)
        .expect("database")
}

/// @return The option, named the way a database option is named
fn option(name: &str, value: &str) -> (OptionDatabase, OptionValue) {
    (
        OptionDatabase::Other(name.to_string()),
        OptionValue::String(value.to_string()),
    )
}

/// Runs a query, so that a connection is shown to work rather than only to
/// have been built.
/// @return The number of rows the query answered with
fn rows_from(connection: &mut adbc_driver_kinetica::KineticaConnection) -> usize {
    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_sql_query("SELECT 1 AS value")
        .expect("set query");

    statement
        .execute()
        .expect("execute")
        .map(|batch| batch.expect("batch").num_rows())
        .sum()
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn every_setting_is_read_back_as_it_was_set() {
    let said = [
        (options::PRIMARY_URL, "http://primary:9191"),
        (options::HOSTNAME_REGEX, "^10\\."),
        (options::TRUST_STORE_PATH, "/etc/ssl/truststore.jks"),
        (options::BYPASS_SSL_CERT_CHECK, "true"),
        (options::SNAPPY, "false"),
        (options::FAILOVER, "false"),
        (options::AUTO_DISCOVERY, "false"),
        (options::FAILOVER_ORDER, "random"),
        (options::REQUEST_TIMEOUT, "45"),
        (options::CONNECT_TIMEOUT, "15"),
        (options::INITIAL_CONNECT_TIMEOUT, "5"),
        (options::FAILBACK_POLL_INTERVAL, "60"),
        (options::MAX_RETRIES, "3"),
        (options::OAUTH_TOKEN, "a-token"),
    ];

    let database = database_with(said.iter().map(|(name, value)| option(name, value)));

    for (name, value) in said {
        let read = database
            .get_option_string(OptionDatabase::Other(name.to_string()))
            .unwrap_or_else(|error| panic!("{name} could not be read back: {error}"));

        assert_eq!(read, value, "{name} came back changed");
    }
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_secret_is_write_only() {
    let database = database_with([option(options::TRUST_STORE_PASSWORD, "secret")]);

    assert!(database
        .get_option_string(OptionDatabase::Other(
            options::TRUST_STORE_PASSWORD.to_string()
        ))
        .is_err());
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_value_the_driver_cannot_use_is_refused_where_it_is_said() {
    let (url, user, password) = credentials();

    let error = KineticaDriver::default()
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(url)),
            (OptionDatabase::Username, OptionValue::String(user)),
            (OptionDatabase::Password, OptionValue::String(password)),
            option(options::FAILOVER_ORDER, "round-robin"),
        ])
        .expect_err("a failover order outside the two should be refused");

    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("random or sequential"),
        "the message should say what is accepted: {}",
        error.message
    );
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_timeout_that_is_not_a_number_is_refused() {
    let (url, _, _) = credentials();

    let error = KineticaDriver::default()
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(url)),
            option(options::REQUEST_TIMEOUT, "a while"),
        ])
        .expect_err("a timeout that is not a number should be refused");

    assert_eq!(error.status, Status::InvalidArguments);
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn an_option_the_driver_does_not_know_is_still_refused() {
    let (url, _, _) = credentials();

    let error = KineticaDriver::default()
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(url)),
            option("kinetica.not_a_setting", "1"),
        ])
        .expect_err("an unknown option should not be swallowed");

    assert_eq!(error.status, Status::NotImplemented);
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_connection_opens_with_the_transport_settings_on() {
    let mut connection = database_with([
        option(options::REQUEST_TIMEOUT, "60"),
        option(options::CONNECT_TIMEOUT, "30"),
        option(options::INITIAL_CONNECT_TIMEOUT, "10"),
        option(options::MAX_RETRIES, "2"),
        option(options::SNAPPY, "false"),
        option(options::FAILOVER_ORDER, "random"),
        option(options::BYPASS_SSL_CERT_CHECK, "true"),
    ])
    .new_connection()
    .expect("connection");

    assert_eq!(rows_from(&mut connection), 1);
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_ring_is_given_as_a_comma_separated_uri() {
    let (url, user, password) = credentials();

    // The same cluster twice, which is a list the driver has to split and the
    // client has to accept; a second address that is really another cluster
    // is not something these tests have.
    let mut connection = KineticaDriver::default()
        .new_database_with_opts([
            (
                OptionDatabase::Uri,
                OptionValue::String(format!("{url}, {url}")),
            ),
            (OptionDatabase::Username, OptionValue::String(user)),
            (OptionDatabase::Password, OptionValue::String(password)),
        ])
        .expect("database")
        .new_connection()
        .expect("connection");

    assert_eq!(rows_from(&mut connection), 1);
}

#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_bearer_token_is_offered_to_the_server() {
    // The instance the tests run against authenticates with a password, so a
    // token is not expected to work; what is asserted is that the driver
    // carries it to the server rather than refusing it, which the server's
    // own rejection shows it did.
    let opened = database_with([option(options::OAUTH_TOKEN, "not-a-real-token")]).new_connection();

    match opened {
        Ok(mut connection) => assert_eq!(rows_from(&mut connection), 1),
        Err(error) => assert!(
            !matches!(error.status, Status::NotImplemented),
            "a token should reach the server, not be refused here: {error}"
        ),
    }
}

/// @return How many rows a five-row query answers with on the connection
fn rows_of_five(connection: &mut adbc_driver_kinetica::KineticaConnection) -> usize {
    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_sql_query("SELECT generate_series AS n FROM TABLE(GENERATE_SERIES(1, 5))")
        .expect("set query");

    statement
        .execute()
        .expect("execute")
        .map(|batch| batch.expect("batch").num_rows())
        .sum()
}

/// A `kinetica://` URI reaches the same server as the URL it stands for, and
/// carries options as its query; an option set directly beats the URI's.
#[test]
#[cfg_attr(
    not(kinetica_server),
    ignore = "set KI_URL, KI_USER and KI_PWD to run against a Kinetica"
)]
fn a_kinetica_uri_connects_and_carries_options() {
    let (url, user, password) = credentials();
    let (scheme, rest) = url.split_once("://").expect("KI_URL is a URL");
    let tls = scheme.eq_ignore_ascii_case("https");
    let uri = format!("kinetica://{rest}?tls={tls}&{}=2", options::MAX_ROWS);

    let database_with = |extra: Vec<(OptionDatabase, OptionValue)>| {
        let mut said = vec![
            (OptionDatabase::Uri, OptionValue::String(uri.clone())),
            (OptionDatabase::Username, OptionValue::String(user.clone())),
            (
                OptionDatabase::Password,
                OptionValue::String(password.clone()),
            ),
        ];
        said.extend(extra);

        KineticaDriver::default()
            .new_database_with_opts(said)
            .expect("database")
    };

    let mut connection = database_with(vec![]).new_connection().expect("connection");
    assert_eq!(
        rows_of_five(&mut connection),
        2,
        "the URI's row cap applies"
    );

    let mut connection = database_with(vec![option(options::MAX_ROWS, "3")])
        .new_connection()
        .expect("connection");
    assert_eq!(
        rows_of_five(&mut connection),
        3,
        "an option set directly beats the URI's"
    );
}
