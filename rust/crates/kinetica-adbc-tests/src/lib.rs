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

//! Helpers the live tests share.

use std::sync::{OnceLock, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// The instance the live tests run against, and the login for it.
pub struct Login {
    pub url: String,
    pub user: String,
    pub password: String,
}

/// @return The value of a variable the live tests need, or a panic naming it
fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!("{name} is not set: the live tests take the instance and its login from KI_URL, KI_USER and KI_PWD")
    })
}

/// The instance and login the live tests run with, from KI_URL, KI_USER and
/// KI_PWD. There are no defaults: no login is kept in this repository.
/// @return The instance's address, user and password
pub fn login() -> Login {
    Login {
        url: required("KI_URL"),
        user: required("KI_USER"),
        password: required("KI_PWD"),
    }
}

/// @return The instance's address alone, for a test that never logs in
pub fn url() -> String {
    required("KI_URL")
}

/// A client of its own, and the runtime it runs on, built once for the run.
///
/// The driver's runtime is private to it, so this cannot borrow it. A second
/// runtime is safe here, where it would not be in the driver: the tests are
/// synchronous, so nothing calls this from inside a runtime, and this one only
/// ever drives this client.
struct Server {
    runtime: tokio::runtime::Runtime,
    client: gpudb_api::Kinetica,
}

/// @return The connection the helpers reach the server through
fn server() -> &'static Server {
    static SERVER: OnceLock<Server> = OnceLock::new();

    SERVER.get_or_init(|| {
        let Login {
            url,
            user,
            password,
        } = login();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let mut options = gpudb_api::KineticaOptions::new();
        options
            .set_primary_url(url.clone())
            .set_username(user)
            .set_password(password);

        let client = runtime
            .block_on(gpudb_api::Kinetica::new(vec![url], options))
            .expect("client");

        Server { runtime, client }
    })
}

/// Counts the paging tables the server is holding for this process.
///
/// They are made in sys_sql_temp, which SQL cannot reach, which
/// information_schema does not list, and which get_objects reports as a
/// schema without its tables. /show/table is the only thing that sees them,
/// so this goes to the server directly rather than through the driver; a test
/// asserting cleanup through any of the others asserts nothing.
///
/// Only this process's are counted, by the prefix the driver names them with,
/// so another session using the server at the same time does not move the
/// count. Tests in this process still share it, so a test that counts holds
/// `exclusive()`, and the tests beside it that might make one hold `shared()`.
///
/// @return How many of this process's paging tables exist
pub fn paging_tables_held() -> usize {
    let server = server();
    let prefix = adbc_driver_kinetica::statement::paging_table_prefix();

    let options = gpudb_api::ShowTableRequestOptions {
        show_children: "true".to_string(),
        get_sizes: "false".to_string(),
        no_error_if_not_exists: "true".to_string(),
        ..Default::default()
    };

    let response = server
        .runtime
        .block_on(
            server
                .client
                .show_table_with_opts("sys_sql_temp".to_string(), options),
        )
        .expect("show_table");

    // sys_sql_temp lists names bare. A qualified one is matched as well,
    // since a filter that matched nothing would let every cleanup assertion
    // pass.
    response
        .table_names
        .iter()
        .filter(|name| name.rsplit('.').next().unwrap_or(name).starts_with(&prefix))
        .count()
}

/// What the tests in one file hold while they run, for those that cannot run
/// beside the others: a test counting this process's paging tables, or tests
/// rebuilding one fixture table.
static RUNNING: RwLock<()> = RwLock::new(());

/// A test that others in its file can run beside.
/// @return A hold, released when it is dropped
pub fn shared() -> RwLockReadGuard<'static, ()> {
    // A test that failed while holding it says nothing about the next one
    RUNNING.read().unwrap_or_else(PoisonError::into_inner)
}

/// A test that runs alone among those in its file that hold `shared()`.
/// @return A hold, released when it is dropped
pub fn exclusive() -> RwLockWriteGuard<'static, ()> {
    RUNNING.write().unwrap_or_else(PoisonError::into_inner)
}

/// The physical tables a session's SQL `#name` temp tables become.
///
/// A `#name` is mangled with the session that made it, so the name to look
/// for is not known up front; what the tests do know is the suffix they gave
/// it. They live in ki_home, which /show/table lists on request.
///
/// @return Every table in ki_home whose name ends with `suffix`
pub fn session_tables(suffix: &str) -> Vec<String> {
    let server = server();

    let options = gpudb_api::ShowTableRequestOptions {
        show_children: "true".to_string(),
        get_sizes: "false".to_string(),
        no_error_if_not_exists: "true".to_string(),
        ..Default::default()
    };

    let response = server
        .runtime
        .block_on(
            server
                .client
                .show_table_with_opts("ki_home".to_string(), options),
        )
        .expect("show_table");

    let suffix = suffix.to_lowercase();

    response
        .table_names
        .into_iter()
        .filter(|name| name.to_lowercase().ends_with(&suffix))
        .collect()
}

/// Drops whatever `session_tables` finds, so a test that leaves temp tables
/// behind does not leave them for the next run.
pub fn clear_session_tables(suffix: &str) {
    let server = server();

    let options = gpudb_api::ClearTableRequestOptions {
        no_error_if_not_exists: "true".to_string(),
    };

    for name in session_tables(suffix) {
        let _ = server.runtime.block_on(server.client.clear_table_with_opts(
            name,
            String::new(),
            options.clone(),
        ));
    }
}

/// A fragment no other run shares, for naming tables and sessions. Letters,
/// digits and '-' only, which is all a session id may hold.
/// @return That fragment
pub fn unique_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();

    format!("{:x}{:x}", std::process::id(), nanos)
}

/// Drops a session by name, which is what the caller of a session it named
/// has to do: the driver will not drop one it was handed, and ADBC has no
/// call that would, so this goes to the server on a client of its own.
///
/// @return Whether the server reported the drop OK
pub fn drop_session(session_id: &str) -> bool {
    let Login {
        url,
        user,
        password,
    } = login();

    let mut options = gpudb_api::KineticaOptions::new();
    options
        .set_primary_url(url.clone())
        .set_username(user)
        .set_password(password)
        .set_session_id(Some(session_id));

    // Its own client, since the session is named in the header every request
    // carries and the shared one carries a different name.
    server()
        .runtime
        .block_on(async {
            let client = gpudb_api::Kinetica::new(vec![url], options).await?;
            let response = client.drop_session().await;
            client.close().await;
            response
        })
        .is_ok()
}

/// The TTL the server is holding a table under, in minutes.
///
/// Read from the server rather than through the driver: a `ttl` asked for on
/// a statement is recorded on what the statement made, and /show/table is
/// what reports it back.
///
/// @return The table's TTL, or None if the server does not name one
pub fn table_ttl(name: &str) -> Option<String> {
    let server = server();

    let options = gpudb_api::ShowTableRequestOptions {
        get_sizes: "false".to_string(),
        no_error_if_not_exists: "true".to_string(),
        ..Default::default()
    };

    let response = server
        .runtime
        .block_on(
            server
                .client
                .show_table_with_opts(name.to_string(), options),
        )
        .expect("show_table");

    response
        .additional_info
        .first()
        .and_then(|info| info.get("table_ttl"))
        .cloned()
}
