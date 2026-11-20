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

//! Statements that change what the connection is, rather than asking the
//! server anything.
//!
//! `SET USER`, `EXECUTE AS USER`, `REVERT`, `SET SCHEMA` and `SET CATALOG`
//! are not SQL the server runs: they say who the connection should act as and
//! where its unqualified names are looked for. ADBC has options for the last
//! two and nothing for the first, so a tool that only sends SQL — which is
//! every SQL console — could not say any of it. So the driver carries out
//! these five statements itself.
//!
//! Impersonation is the `KINETICA_IMPERSONATE` header, which the server reads
//! as "do this as that user". It is held as a stack, so `EXECUTE AS USER`
//! nests and `REVERT` goes back one step; the bottom of the stack is whatever
//! `adbc.kinetica.impersonate_user` named, or nobody.

use std::sync::Mutex;

use adbc_core::error::Result as AdbcResult;
use regex::Regex;
use std::sync::LazyLock;

use gpudb_api::Kinetica;

use crate::error::{invalid, to_adbc};

/// The header the server reads as the user to act as.
const IMPERSONATE_HEADER: &str = "KINETICA_IMPERSONATE";

/// A name, bare or quoted, as these statements take one.
const NAME: &str = r#"(?:'[^']+'|"[^"]+"|[^"'; \t]+)"#;

/// `SET USER [=] [<user>]`, where no user at all means the same as `REVERT`.
static SET_USER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"(?is)^\s*SET\s+USER\s*(?:=\s*)?({NAME})?\s*;?\s*$"
    ))
    .expect("the set user pattern is valid")
});

/// `EXEC[UTE] AS USER [=] <user> [WITH NO REVERT]`.
static EXECUTE_AS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"(?is)^\s*EXEC(?:UTE)?\s+AS\s+USER\s*(?:=\s*)?({NAME})\s*(WITH\s+NO\s+REVERT)?\s*;?\s*$"
    ))
    .expect("the execute as pattern is valid")
});

/// `REVERT`.
static REVERT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)^\s*REVERT\s*;?\s*$").expect("the revert pattern is valid"));

/// `SET [CURRENT] {SCHEMA | SQLID} [=] [<schema>]`, where no schema goes back
/// to whatever the server resolves names against by default.
static SET_SCHEMA: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"(?is)^\s*SET\s+(?:CURRENT\s+)?(?:SCHEMA|SQLID)\s*(?:=\s*)?({NAME})?\s*;?\s*$"
    ))
    .expect("the set schema pattern is valid")
});

/// `SET CATALOG [=] [<catalog>]`.
static SET_CATALOG: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"(?is)^\s*SET\s+CATALOG\s*(?:=\s*)?({NAME})?\s*;?\s*$"
    ))
    .expect("the set catalog pattern is valid")
});

/// A comment can hold anything, including something that would change what
/// the statement means, so they come out before it is read.
static COMMENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)/\*.*?\*/|--[^\n]*").expect("the comment pattern is valid"));

/// One of the statements this driver carries out itself.
#[derive(Debug, PartialEq, Eq)]
pub enum Control {
    /// Act as this user from now on. `None` steps back one user, which is
    /// what `SET USER` with no name means.
    Act {
        user: Option<String>,
        /// Whether a later `REVERT` is allowed: `WITH NO REVERT` says not.
        revertible: bool,
    },
    /// Step back to the user before this one.
    Revert,
    /// Look unqualified names up here. Empty goes back to the default.
    Schema(String),
    /// Resolve against this catalog. Empty goes back to the local one.
    Catalog(String),
}

/// @return The statement, if it is one the connection carries out itself
pub fn recognize(sql: &str) -> Option<Control> {
    let bare = COMMENT.replace_all(sql, " ");

    // SET SCHEMA and SET CATALOG are tried before SET USER only so that each
    // reads its own; the three cannot match the same text.
    if let Some(found) = SET_SCHEMA.captures(&bare) {
        return Some(Control::Schema(name(found.get(1))));
    }

    if let Some(found) = SET_CATALOG.captures(&bare) {
        return Some(Control::Catalog(name(found.get(1))));
    }

    if let Some(found) = SET_USER.captures(&bare) {
        let user = found
            .get(1)
            .map(|_| name(found.get(1)))
            .filter(|u| !u.is_empty());

        return Some(Control::Act {
            user,
            revertible: true,
        });
    }

    if let Some(found) = EXECUTE_AS.captures(&bare) {
        return Some(Control::Act {
            user: Some(name(found.get(1))).filter(|u| !u.is_empty()),
            revertible: found.get(2).is_none(),
        });
    }

    if REVERT.is_match(&bare) {
        return Some(Control::Revert);
    }

    None
}

/// @return The name a match holds, with the quotes taken off
fn name(found: Option<regex::Match<'_>>) -> String {
    let text = found.map(|m| m.as_str().trim()).unwrap_or_default();

    for (open, close) in [('\'', '\''), ('"', '"')] {
        if text.starts_with(open) && text.ends_with(close) && text.len() >= 2 {
            return text[1..text.len() - 1].to_string();
        }
    }

    text.to_string()
}

/// Who the connection is acting as, and who it was acting as before that.
///
/// The stack is the connection's, not a statement's: `EXECUTE AS USER` on one
/// statement is still in force on the next, which is what makes `REVERT` a
/// statement of its own rather than a clause.
#[derive(Debug, Default)]
pub struct Impersonation {
    inner: Mutex<Stack>,
}

#[derive(Debug, Default)]
struct Stack {
    /// Who the connection falls back to with nothing on the stack: what
    /// `adbc.kinetica.impersonate_user` named, or nobody.
    base: String,
    /// Each user stepped into, the last being the one in force.
    stepped: Vec<String>,
    /// Whether a `REVERT` is still allowed.
    revertible: bool,
}

impl Impersonation {
    /// @return A stack whose bottom is the given user, or nobody if empty
    pub(crate) fn new(base: &str) -> Self {
        Self {
            inner: Mutex::new(Stack {
                base: base.to_string(),
                stepped: Vec::new(),
                revertible: true,
            }),
        }
    }

    /// @return Who the connection is acting as, or empty for nobody
    pub(crate) fn current(&self) -> String {
        let stack = self.inner.lock().unwrap();

        stack.stepped.last().cloned().unwrap_or(stack.base.clone())
    }

    /// Steps into a user, or back out of one where no user is named.
    fn step(&self, user: Option<String>, revertible: bool) -> AdbcResult<String> {
        let mut stack = self.inner.lock().unwrap();

        match user {
            Some(user) => {
                stack.stepped.push(user);
                stack.revertible &= revertible;
            }
            // `SET USER` with no name means the same as REVERT
            None => return Self::pop(&mut stack),
        }

        Ok(stack.stepped.last().cloned().unwrap_or(stack.base.clone()))
    }

    /// Steps back to the user before the one in force.
    fn revert(&self) -> AdbcResult<String> {
        Self::pop(&mut self.inner.lock().unwrap())
    }

    /// @return Who is in force after stepping back one
    fn pop(stack: &mut Stack) -> AdbcResult<String> {
        if !stack.revertible {
            return Err(invalid(
                "REVERT after EXECUTE AS USER ... WITH NO REVERT, which gave up the right to it",
            ));
        }

        stack.stepped.pop();

        Ok(stack.stepped.last().cloned().unwrap_or(stack.base.clone()))
    }

    /// Tells the client who to act as, which is a header on every request.
    fn send(&self, client: &Kinetica, user: &str) -> AdbcResult<()> {
        if user.is_empty() {
            client
                .remove_http_header(IMPERSONATE_HEADER)
                .map_err(to_adbc)
        } else {
            client
                .add_http_header(IMPERSONATE_HEADER, user)
                .map_err(to_adbc)
        }
    }

    /// Puts the connection's starting user in force, for a caller that named
    /// one before anything ran.
    pub(crate) fn apply(&self, client: &Kinetica) -> AdbcResult<()> {
        let user = self.current();

        self.send(client, &user)
    }

    /// Carries out a user statement against the client.
    pub(crate) fn carry_out(
        &self,
        client: &Kinetica,
        user: Option<String>,
        revertible: bool,
    ) -> AdbcResult<()> {
        let now = self.step(user, revertible)?;

        self.send(client, &now)
    }

    /// Carries out a REVERT against the client.
    pub(crate) fn carry_out_revert(&self, client: &Kinetica) -> AdbcResult<()> {
        let now = self.revert()?;

        self.send(client, &now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_user_is_taken_bare_or_quoted() {
        for (sql, expected) in [
            ("SET USER alice", "alice"),
            ("SET USER = 'alice'", "alice"),
            ("set user \"alice\"", "alice"),
            ("EXECUTE AS USER alice", "alice"),
            ("EXEC AS USER = 'alice';", "alice"),
        ] {
            match recognize(sql) {
                Some(Control::Act { user, revertible }) => {
                    assert_eq!(user.as_deref(), Some(expected), "{sql}");
                    assert!(revertible, "{sql}");
                }
                other => panic!("{sql} read as {other:?}"),
            }
        }
    }

    #[test]
    fn with_no_revert_gives_up_the_right_to_revert() {
        match recognize("EXECUTE AS USER alice WITH NO REVERT") {
            Some(Control::Act { user, revertible }) => {
                assert_eq!(user.as_deref(), Some("alice"));
                assert!(!revertible);
            }
            other => panic!("read as {other:?}"),
        }
    }

    /// Naming nobody means the same as REVERT.
    #[test]
    fn set_user_with_no_name_steps_back() {
        assert_eq!(
            recognize("SET USER"),
            Some(Control::Act {
                user: None,
                revertible: true
            })
        );
        assert_eq!(recognize("REVERT"), Some(Control::Revert));
        assert_eq!(recognize(" revert ; "), Some(Control::Revert));
    }

    #[test]
    fn a_schema_and_a_catalog_are_read_and_can_be_cleared() {
        assert_eq!(
            recognize("SET SCHEMA sales"),
            Some(Control::Schema("sales".into()))
        );
        assert_eq!(
            recognize("SET CURRENT SQLID = 'sales'"),
            Some(Control::Schema("sales".into()))
        );
        assert_eq!(
            recognize("SET CATALOG remote"),
            Some(Control::Catalog("remote".into()))
        );
        assert_eq!(
            recognize("SET SCHEMA"),
            Some(Control::Schema(String::new()))
        );
        assert_eq!(
            recognize("SET CATALOG"),
            Some(Control::Catalog(String::new()))
        );
    }

    /// Anything the server should see stays the server's.
    #[test]
    fn anything_else_is_left_to_the_server() {
        for sql in [
            "SELECT * FROM t",
            "SET SCHEMA a, b",
            "SET SCHEMA sales; DROP TABLE t",
            "REVERT TABLE t",
            "SET USER alice, bob",
            "EXECUTE AS alice",
            "SETTING = 1",
            "UPDATE t SET user = 'alice'",
        ] {
            assert!(recognize(sql).is_none(), "{sql} should go to the server");
        }
    }

    #[test]
    fn a_stack_steps_in_and_back_out_again() {
        let held = Impersonation::new("base");
        assert_eq!(held.current(), "base");

        held.step(Some("alice".into()), true).expect("alice");
        assert_eq!(held.current(), "alice");

        held.step(Some("bob".into()), true).expect("bob");
        assert_eq!(held.current(), "bob");

        held.revert().expect("back to alice");
        assert_eq!(held.current(), "alice");

        held.revert().expect("back to base");
        assert_eq!(held.current(), "base");

        // Stepping back past the bottom stays at the bottom.
        held.revert().expect("still base");
        assert_eq!(held.current(), "base");
    }

    #[test]
    fn with_no_revert_is_refused_afterwards() {
        let held = Impersonation::new("");
        held.step(Some("alice".into()), false).expect("alice");

        assert!(held.revert().is_err(), "the right was given up");
        assert_eq!(held.current(), "alice");
    }

    /// Nobody named at the bottom means no header at all, not an empty one.
    #[test]
    fn no_base_user_means_nobody() {
        let held = Impersonation::new("");
        assert_eq!(held.current(), "");

        held.step(Some("alice".into()), true).expect("alice");
        held.revert().expect("back");

        assert_eq!(held.current(), "");
    }
}
