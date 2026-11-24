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

//! Arrow ADBC driver for Kinetica, built on `gpudb-api`.
//!
//! The ADBC traits are synchronous and the client is async, so every call that
//! reaches the server crosses the boundary owned by the `runtime` module.
//!
//! Results arrive as an Arrow IPC stream in the response's binary payload,
//! which is the shape the server produces for `encoding=arrow`; nothing here
//! decodes Avro.

pub mod auth;
pub mod catalog;
pub mod connection;
pub mod context;
pub mod control;
pub mod database;
pub mod ddl;
pub mod driver;
pub mod error;
pub mod file;
pub mod ingest;
pub mod insert;
pub mod kifs;
pub mod loading;
pub mod logging;
pub mod lookup;
pub mod params;
pub mod partition;
pub mod provider;
pub mod query;
pub mod reader;
pub mod session;
pub mod settings;
pub mod statement;
pub mod statistics;
pub mod uri;

pub(crate) mod runtime;

/// The release version, the crate's own SemVer version, e.g. `7.2.0`:
/// `MAJOR.MINOR` is the Kinetica version the driver works with, and `PATCH`
/// counts the driver's own releases for it.
pub const VERSION: &str = env!("KINETICA_ADBC_VERSION");

/// The release version with the build number as SemVer build metadata, e.g.
/// `7.2.0+20261104140513`, which the client's log gives when a connection
/// opens. `get_info` reports the release version alone. The build number is
/// the time of the build, as the Rust API's is.
pub const DRIVER_VERSION: &str = env!("KINETICA_ADBC_DRIVER_VERSION");

/// The version of Arrow the driver was built with, e.g. `v59.3.0`, which
/// `get_info` reports. Empty where the build could not tell.
pub const ARROW_VERSION: &str = env!("KINETICA_ADBC_ARROW_VERSION");

pub use connection::KineticaConnection;
pub use database::KineticaDatabase;
pub use driver::KineticaDriver;
pub use error::{could_not, from_arrow, invalid, to_adbc, unsupported};
pub use statement::KineticaStatement;

// Exports `AdbcDriverKineticaInit` from the shared library, which is how every
// language other than Rust loads this driver: the ADBC driver manager looks up
// that symbol and everything else goes over the C ABI. Rust callers use the
// types above directly and can turn the feature off.
#[cfg(feature = "driver_exporter")]
adbc_ffi::export_driver!(AdbcDriverKineticaInit, driver::KineticaDriver);

/// The field metadata keys a result's columns carry, for what Arrow cannot
/// say about a Kinetica column. Ingest reads them back, so a table read and
/// written again keeps its column types.
pub mod metadata {
    /// The column's type as Kinetica's SQL writes it, such as `INTEGER`,
    /// `VARCHAR(16)`, `DECIMAL(10,2)` or `DATETIME`.
    pub const TYPE: &str = "KINETICA:type";

    /// The column's properties, comma-separated as the server lists them,
    /// such as `char8,nullable`: how wide a string is, and what a number means.
    pub const PROPERTIES: &str = "KINETICA:properties";

    /// Whether the column takes nulls, `true` or `false`.
    pub const NULLABLE: &str = "KINETICA:nullable";

    /// What the server calls them. Results carry the names above instead,
    /// which are Arrow's form for a namespace of one's own.
    pub(crate) const SERVER_PREFIX: &str = "kinetica.";

    /// The prefix of the names above.
    pub(crate) const PREFIX: &str = "KINETICA:";
}

/// Driver-specific option names. ADBC standardizes usernames and URIs but not
/// batch size or anything Kinetica-shaped, so those are namespaced.
pub mod options {
    /// Rows per request, capped at the server's own limit.
    pub const BATCH_SIZE: &str = "kinetica.query.batch_size";

    /// The table a bound stream is written to, which ADBC standardizes.
    pub const INGEST_TARGET: &str = "adbc.ingest.target_table";

    /// Whether ingest appends to the table or requires it to be new.
    pub const INGEST_MODE: &str = "adbc.ingest.mode";

    /// The server session the SQL `#name` temp tables of a connection belong
    /// to. Several connections given one share its temp tables, and the caller drops it
    /// itself, with /drop/session, since this driver has no call that would.
    /// Empty, the default, gives each connection a session of its own, which
    /// closing the connection drops.
    pub const SESSION_ID: &str = "kinetica.session_id";

    /// Whether to accept the transaction calls and do nothing, rather than
    /// refuse them. Kinetica has no transactions, so a rollback undoes
    /// nothing either way; this decides whether a caller is told. Off by
    /// default.
    pub const FAKE_TRANSACTIONS: &str = "kinetica.fake_transactions";

    /// Rows the last statement changed, or -1 if it was a query. Read with
    /// `get_option_int`; the ADBC C ABI has no room for this on a call that
    /// also returns a reader, so it is asked for afterwards.
    pub const COUNT_AFFECTED: &str = "kinetica.count_affected";

    /// How a credential is presented: `password`, the default, or `oauth`.
    pub const AUTH_METHOD: &str = "kinetica.auth_method";

    /// How a bearer token is obtained, when the caller does not already hold
    /// one: `client_credentials`, `authorization_code`, `device_code`,
    /// `refresh_token`, `token-exchange` or `jwt-bearer`, each the grant
    /// type its RFC names.
    ///
    /// Leaving it out means the caller holds its own token, which is what
    /// `kinetica.oauth_token` is for.
    pub const OAUTH_GRANT: &str = "kinetica.oauth_grant";

    /// Which identity provider is in play: `command`, which runs a command
    /// of the caller's and takes what it prints as the token, or `entra` or
    /// `okta`, which rewrite that vendor's own parameters into the generic
    /// ones and leave the flow to run as it would have.
    ///
    /// `entra` and `okta` are each implied by a parameter nothing else reads
    /// -- `tenant_id`, `application_id_uri`, `okta_domain` -- so naming one
    /// is an override rather than a requirement. `command` runs a process, so
    /// it is never implied.
    ///
    /// A driver loaded through the C ABI cannot be handed code to call, so
    /// `command` is how a caller plugs in a way of getting tokens of its own.
    /// Its command line runs under `sh -c`, or `cmd.exe /c` on Windows, and is
    /// written in that shell's syntax.
    pub const AUTH_PROVIDER: &str = "kinetica.auth_provider";

    /// A bearer token the caller acquired itself, presented as it is.
    pub const OAUTH_TOKEN: &str = "kinetica.oauth_token";

    /// The claim a bearer token's user name is read out of, which is `sub`
    /// unless the provider names another: Entra's `sub` is a different value
    /// per application rather than a name, so under `entra` this is
    /// `preferred_username`.
    ///
    /// The name a token gives beats whatever the username option said, since
    /// the one is a guess at the identity and the other is the identity the
    /// server will see. The signature cannot be checked here, so it names the
    /// holder and never decides what the holder may do.
    pub const TOKEN_NAME_CLAIM: &str = "kinetica.token_name_claim";

    /// Where acquired tokens are kept: `memory`, the default, which shares
    /// one token between every connection of this process that would have
    /// asked for the same one, or `none`.
    pub const OAUTH_TOKEN_CACHE: &str = "kinetica.oauth_token_cache";

    /// Everything else a flow needs is named under this prefix, so a provider
    /// with a quirk of its own needs no new option: `kinetica.oauth.issuer`, `.token_url`,
    /// `.authorize_url`, `.device_url`, `.client_id`, `.client_secret`,
    /// `.scope`, `.audience`, `.subject`, `.refresh_token`,
    /// `.subject_token`, `.subject_token_type`, `.private_key`,
    /// `.private_key_file`, `.key_id` and `.algorithm`.
    ///
    /// Given an issuer and nothing else, the endpoints are read from its
    /// OpenID Connect document.
    ///
    /// A provider's own parameters are named here too: `.command` and
    /// `.command_timeout_seconds` for `command`, `.tenant_id`,
    /// `.application_id_uri` (or `.resource`) and `.authority_host` for
    /// `entra`, and `.okta_domain` and `.auth_server_id` for `okta`.
    pub const OAUTH_PARAM_PREFIX: &str = "kinetica.oauth.";

    /// The cluster that failover returns to once it is reachable again. The
    /// URI gives where to start; several
    /// addresses may be given there, comma-separated.
    pub const PRIMARY_URL: &str = "kinetica.primary_url";

    /// Which of a worker's addresses to use where it publishes more than one,
    /// as a regular expression. Multi-head ingest goes to the addresses this
    /// keeps.
    pub const HOSTNAME_REGEX: &str = "kinetica.hostname_regex";

    /// The trust store holding the certificate authority that signed the
    /// server's certificate.
    pub const TRUST_STORE_PATH: &str = "kinetica.trust_store_path";

    /// The trust store's password. Write-only, as the connection password is.
    pub const TRUST_STORE_PASSWORD: &str = "kinetica.trust_store_password";

    /// Whether to accept the server's certificate without checking it. Off by
    /// default, and worth leaving
    /// off anywhere the certificate can be trusted instead.
    pub const BYPASS_SSL_CERT_CHECK: &str = "kinetica.bypass_ssl_cert_check";

    /// Whether bulk record requests are Snappy-compressed. On by default.
    pub const SNAPPY: &str = "kinetica.snappy";

    /// Whether to fail over to another cluster of the ring. On by default.
    pub const FAILOVER: &str = "kinetica.failover";

    /// Whether to discover the rest of the ring from the cluster first
    /// reached. On by default; off, only the addresses the caller gave are
    /// used.
    pub const AUTO_DISCOVERY: &str = "kinetica.auto_discovery";

    /// `sequential`, the default, or `random`: the order clusters are tried
    /// in when one fails.
    pub const FAILOVER_ORDER: &str = "kinetica.failover_order";

    /// Seconds a request may take before it is abandoned, 0 for no limit.
    pub const REQUEST_TIMEOUT: &str = "kinetica.request_timeout";

    /// Seconds to wait for the server to answer.
    pub const CONNECT_TIMEOUT: &str = "kinetica.connect_timeout";

    /// Seconds to spend reaching a cluster for the first time before trying
    /// the next.
    pub const INITIAL_CONNECT_TIMEOUT: &str = "kinetica.initial_connect_timeout";

    /// Seconds between polls of the primary cluster, which is how a
    /// connection that failed away from it comes back.
    pub const FAILBACK_POLL_INTERVAL: &str = "kinetica.failback_poll_interval";

    /// How many times a request is sent again, for those that were not
    /// completely sent or were rejected due to HA processes being down. The
    /// latter will be retried with a back-off delay determined by the Rust API.
    pub const MAX_RETRIES: &str = "kinetica.max_retries";

    /// Whether the planner costs its alternatives. This and the options below
    /// it go on every statement a connection sends, and each takes true or
    /// false.
    pub const COST_BASED_OPTIMIZATION: &str = "kinetica.query.cost_based_optimization";

    /// Whether the rule-based rewrites run.
    pub const RULE_BASED_OPTIMIZATION: &str = "kinetica.query.rule_based_optimization";

    /// Whether scalar subqueries are optimized.
    pub const SSQ_OPTIMIZATION: &str = "kinetica.query.ssq_optimization";

    /// Whether the planner may redistribute data among the ranks, resharding
    /// or replicating it, so that a join, a set operation or an intermediate
    /// result over data not already distributed compatibly can run. Off, such
    /// a query fails instead.
    pub const DISTRIBUTED_OPERATIONS: &str = "kinetica.query.distributed_operations";

    /// Whether the steps of a plan may run at once.
    pub const PARALLEL_EXECUTION: &str = "kinetica.query.parallel_execution";

    /// Whether a plan already worked out may be reused.
    pub const PLAN_CACHE: &str = "kinetica.query.plan_cache";

    /// Whether a result already worked out may be reused.
    pub const RESULTS_CACHING: &str = "kinetica.query.results_caching";

    /// Whether a row whose primary key is already there is passed over
    /// rather than refused.
    pub const IGNORE_EXISTING_PK: &str = "kinetica.query.ignore_existing_pk";

    /// Whether such a row replaces the one already there.
    pub const UPDATE_ON_EXISTING_PK: &str = "kinetica.query.update_on_existing_pk";

    /// Whether a column being changed is checked first.
    pub const VALIDATE_CHANGE_COLUMN: &str = "kinetica.query.validate_change_column";

    /// Whether distinct values may be counted approximately.
    pub const USE_APPROX_COUNT_DISTINCT: &str = "kinetica.query.use_approx_count_distinct";

    /// Minutes to keep what a statement makes.
    pub const TTL: &str = "kinetica.query.ttl";

    /// Minutes to keep the result a paged read works through. The driver sets
    /// one of its own; this replaces it.
    pub const PAGING_TABLE_TTL: &str = "kinetica.query.paging_table_ttl";

    /// How much the client logs: `off`, the default, or one of `error`,
    /// `warn`, `info`, `debug` and `trace`.
    ///
    /// A driver loaded over the C ABI has no main to install a log in, so
    /// asking for one here is what installs it. A host program that has
    /// already installed one of its own keeps it.
    pub const LOG_LEVEL: &str = "kinetica.log_level";

    /// The file the log is appended to. Empty, the default, writes to
    /// standard error.
    pub const LOG_PATH: &str = "kinetica.log_path";

    /// What to do about a row the workers cannot take: `abort`, the default,
    /// which fails the batch it is in; `skip`, which takes the rest of the
    /// batch without it; or `permissive`, which takes the rest and asks the
    /// server to say what was wrong with each one it refused.
    pub const ERROR_MODE: &str = "kinetica.ingest.error_mode";

    /// Whether a string too long for its column is cut to fit rather than
    /// refused.
    pub const TRUNCATE_STRINGS: &str = "kinetica.ingest.truncate_strings";

    /// Rows an ingest sends to a worker at a time.
    pub const ROWS_PER_INSERTION: &str = "kinetica.ingest.rows_per_insertion";

    /// Whether rows go straight to the workers. On by default; off, they go
    /// through the head rank.
    pub const MULTIHEAD_INSERT: &str = "kinetica.ingest.multihead_insert";

    /// The most rows a read hands back, however many the result holds.
    /// Settable on the database, on a connection,
    /// and on one statement; the statement's own wins. Unset, or negative,
    /// hands back all of them.
    pub const MAX_ROWS: &str = "kinetica.query.max_rows";

    /// Rows the last statement's whole result holds, however many the reader
    /// has been given so far, or -1 if it produced no result. A query bound
    /// to several rows of parameters runs once for each, and this adds up the
    /// runs made so far. Read with `get_option_int`, or as the standard
    /// max-progress option.
    pub const TOTAL_RECORDS: &str = "kinetica.total_number_of_records";

    /// How a local file read by `INSERT INTO ... FROM FILE` is parsed. Each
    /// can be said again in the statement's own `WITH OPTIONS (...)`, as
    /// `file read` and its last part: `file read delimiter = '|'`.
    ///
    /// The field separator. Unset, it is taken from the first file's
    /// extension -- `|` for `.psv`, a tab for `.tsv` -- and is otherwise a
    /// comma.
    pub const FILE_READ_DELIMITER: &str = "kinetica.ingest.file.delimiter";

    /// The quote character, `"` by default.
    pub const FILE_READ_QUOTE_CHAR: &str = "kinetica.ingest.file.quote_char";

    /// The escape character. Unset, a quote is escaped by doubling it.
    pub const FILE_READ_ESCAPE_CHAR: &str = "kinetica.ingest.file.escape_char";

    /// A line starting with this is skipped. Unset, no line is a comment.
    pub const FILE_READ_COMMENT_CHAR: &str = "kinetica.ingest.file.comment_char";

    /// Whether the first line names the columns. True by default.
    pub const FILE_READ_HAS_HEADER: &str = "kinetica.ingest.file.has_header";

    /// The text that stands for a null. Unset, an empty field is null.
    pub const FILE_READ_NULL_STRING: &str = "kinetica.ingest.file.null_string";

    /// Rows to pass over at the start of each file, after its header.
    pub const FILE_READ_SKIP: &str = "kinetica.ingest.file.skip";

    /// The most rows to read across every file matched. Unset, all of them.
    pub const FILE_READ_LIMIT: &str = "kinetica.ingest.file.limit";

    /// Whether the table is emptied before the file is loaded into it.
    pub const FILE_READ_INITIAL_CLEAR: &str = "kinetica.ingest.file.initial_clear";

    /// The user the connection acts as, which the server reads as
    /// impersonation. `SET USER` and `EXECUTE AS USER` step into another from
    /// here, and `REVERT` comes back to it.
    pub const IMPERSONATE_USER: &str = "kinetica.impersonate_user";

    /// How far a write has to have replicated before the server answers:
    /// `sync`, `sync_parallel`, `async`, `async_parallel` or `none`, also
    /// accepted as the server spells them (`REPL_SYNC`). Unset, the server
    /// decides.
    pub const REPLICATION: &str = "kinetica.replication";

    /// Shorthand for a replication mode of `none`, which asks the server not
    /// to wait for replication at all. `kinetica.replication` wins where
    /// both are given.
    pub const NO_SYNC: &str = "kinetica.no_sync";

    /// Whether a read that pins every key column goes to the rank holding its
    /// row rather than to the SQL engine. Off by default.
    ///
    /// Settable on the database, on a connection, and on one statement, where
    /// it beats the connection's.
    pub const USE_KEY_LOOKUP: &str = "kinetica.query.use_key_lookup";

    /// Whether the last read was answered by the rank holding its row rather
    /// than by the SQL engine: 1 if it was, 0 if not. Read with
    /// `get_option_int`, like the counts beside it.
    ///
    /// A statement is answered that way when it pins every key column of one
    /// table to a literal; see the `lookup` module for the whole rule. It is
    /// a routing decision and nothing more — the rows are the rows either
    /// way — so a caller need only read this to find out why a query it
    /// expected to be quick was not.
    pub const KEY_LOOKUP: &str = "kinetica.key_lookup";
}
