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

//! Who the identity provider is: whose vocabulary the flow parameters are
//! written in, or -- for one of them -- where the token comes from instead of
//! a flow.
//!
//! Two kinds. A *vendor* runs no flow of its
//! own. It takes what an administrator was handed -- a tenant ID, an Okta
//! domain -- and assembles the issuer and scope that the vendor's own
//! conventions imply, conventions documented elsewhere and easy to get wrong.
//! That assembly is the whole content of such a provider, so the vendors here
//! are data rather than a type each, and adding one is a row in [`ALL`].
//!
//! An *acquire* provider produces a finished token, so it stands in place of
//! a grant rather than preparing one. `command` is the only one, and it is
//! the extension point that survives this driver's C ABI: a driver loaded
//! from Python or C++ cannot be handed code to call, but it can be told to
//! run a program.

use std::collections::BTreeMap;
use std::process::Stdio;
use std::time::Duration;

use adbc_core::error::Result as AdbcResult;

use crate::error::invalid;
use crate::options;
use crate::runtime;

/// How long a command is given to print a token before it is given up on.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// The most either of a command's streams is read, so that one printing
/// something unexpected cannot be read until memory runs out.
const MAX_OUTPUT: u64 = 1 << 20;

/// Lines of a failed command's diagnostics quoted back, so that a verbose
/// failure does not become the whole error message.
const QUOTED_LINES: usize = 3;

/// `kinetica.oauth.command` -- the command line to run.
const PARAM_COMMAND: &str = "command";

/// `kinetica.oauth.command_timeout_seconds` -- how long to wait for it.
const PARAM_TIMEOUT: &str = "command_timeout_seconds";

/// Parameters that mean the same thing under two names, the second being the
/// one a vendor's own console shows.
const ALIASES: &[(&str, &str)] = &[("resource", "application_id_uri")];

/// The provider a caller named.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    /// Run a command of the caller's and take what it prints as the token.
    Command,
    /// Microsoft Entra ID's vocabulary.
    Entra,
    /// Okta's vocabulary.
    Okta,
}

impl Provider {
    /// @return The option value that names this provider
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Command => "command",
            Self::Entra => "entra",
            Self::Okta => "okta",
        }
    }

    /// @return The vocabulary this provider translates, or None if it
    ///         produces a token outright instead
    pub fn vendor(self) -> Option<&'static Vendor> {
        match self {
            Self::Command => None,
            Self::Entra => ALL.iter().find(|vendor| vendor.name == "entra"),
            Self::Okta => ALL.iter().find(|vendor| vendor.name == "okta"),
        }
    }
}

/// @return The provider the value names, whatever its case
pub fn named(value: &str) -> AdbcResult<Provider> {
    let trimmed = value.trim();

    for provider in [Provider::Command, Provider::Entra, Provider::Okta] {
        if trimmed.eq_ignore_ascii_case(provider.as_str()) {
            return Ok(provider);
        }
    }

    Err(invalid(&format!(
        "{} is '{trimmed}', which names no provider; it takes command, {}",
        options::AUTH_PROVIDER,
        names()
    )))
}

/// @return Every vendor's name, for saying what was expected
fn names() -> String {
    ALL.iter()
        .map(|vendor| vendor.name)
        .collect::<Vec<_>>()
        .join(", ")
}

/// One identity provider's own vocabulary, written as templates over the
/// generic one.
///
/// A template reads `{key}`, or `{key|default}`. Expanding one yields nothing
/// if a key without a default is missing, which simply means the derivation
/// does not apply.
#[derive(Debug)]
pub struct Vendor {
    name: &'static str,
    /// Parameters nothing else reads, so that holding one of them names this
    /// vendor without the caller having to.
    inferred_from: &'static [&'static str],
    issuer: &'static str,
    /// Empty where the vendor's scopes are named rather than derived.
    scope: &'static str,
    /// Added to the scope for an interactive sign-in and no other, since what
    /// it asks for is meaningless with nobody present.
    interactive_scope: &'static str,
    /// The claim this vendor puts a usable name in.
    name_claim: &'static str,
}

/// The vendors recognized today.
///
/// Each is inferable from parameters nothing else reads, so naming one in
/// `kinetica.auth_provider` is an override rather than a requirement: a
/// vendor runs no flow of its own, so leaving its name out costs a reader
/// nothing.
pub static ALL: &[Vendor] = &[
    Vendor {
        name: "entra",
        inferred_from: &["tenant_id", "application_id_uri"],
        // v2.0 is required: v1.0 issues differently shaped tokens and ignores
        // the /.default scope below.
        issuer: "{authority_host|https://login.microsoftonline.com}/{tenant_id}/v2.0",
        scope: "{resource}/.default",
        // Without it there is no refresh token and the user signs in again
        // when this one expires.
        interactive_scope: " offline_access",
        // Entra's sub is pairwise -- a different value per application, and
        // never a name anyone would recognize.
        name_claim: "preferred_username",
    },
    Vendor {
        name: "okta",
        inferred_from: &["okta_domain"],
        // The org server is the bare domain; a custom one lives under
        // /oauth2/{id}, and confusing the two is the usual Okta mistake.
        issuer: "https://{okta_domain}/oauth2/{auth_server_id|default}",
        // Okta's scopes are named outright rather than derived from whatever
        // the token is for.
        scope: "",
        interactive_scope: "",
        name_claim: "preferred_username",
    },
];

impl Vendor {
    /// @return The name that selects this vendor
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// @return The claim this vendor puts a usable name in
    pub fn name_claim(&self) -> &'static str {
        self.name_claim
    }

    /// Works out the generic parameters this vendor's own imply.
    ///
    /// Anything said outright is left alone, so an unusual authority or a
    /// named delegated permission can always be spelled out directly -- which
    /// also makes this idempotent, since a second pass finds its targets
    /// already filled.
    ///
    /// @param params What the caller said
    /// @param interactive Whether the flow has a user in front of it, which
    ///        decides whether a refresh token is worth asking for
    /// @return The parameters to add, which may be none
    pub fn derive(
        &self,
        params: &BTreeMap<String, String>,
        interactive: bool,
    ) -> BTreeMap<String, String> {
        let mut derived = BTreeMap::new();

        if lookup(params, "issuer").is_none() {
            if let Some(issuer) = expand(self.issuer, params) {
                derived.insert("issuer".to_string(), issuer);
            }
        }

        if !self.scope.is_empty() && lookup(params, "scope").is_none() {
            if let Some(scope) = expand(self.scope, params) {
                let asked = match interactive {
                    true => scope + self.interactive_scope,
                    false => scope,
                };

                derived.insert("scope".to_string(), asked);
            }
        }

        derived
    }
}

/// Works out which vendor is in play from the parameters alone.
///
/// Only a vendor is ever inferred, and only from parameters nothing else
/// reads, so this can never change which flow runs -- only the vocabulary its
/// parameters are read in. An acquire provider does something observable, so
/// it is always named outright.
///
/// @return The vendor implied, or None if none is
pub fn infer(params: &BTreeMap<String, String>) -> Option<&'static Vendor> {
    ALL.iter().find(|vendor| {
        vendor
            .inferred_from
            .iter()
            .any(|key| lookup(params, key).is_some())
    })
}

/// @return The parameter under its own name or a vendor's name for it, if it
///         was given and is not empty
fn lookup<'a>(params: &'a BTreeMap<String, String>, key: &str) -> Option<&'a str> {
    let said = |name: &str| {
        params
            .get(name)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
    };

    said(key).or_else(|| {
        ALIASES
            .iter()
            .find(|(generic, _)| *generic == key)
            .and_then(|(_, alias)| said(alias))
    })
}

/// @return The template with its placeholders filled, or None if one has
///         neither a value nor a default
fn expand(template: &str, params: &BTreeMap<String, String>) -> Option<String> {
    let mut out = String::new();
    let mut rest = template;

    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);

        let close = rest[open..].find('}')? + open;
        let placeholder = &rest[open + 1..close];

        let (key, fallback) = match placeholder.split_once('|') {
            Some((key, fallback)) => (key, Some(fallback)),
            None => (placeholder, None),
        };

        let value = lookup(params, key).or(fallback).filter(|v| !v.is_empty())?;

        // A trailing slash on a host or an identifier is a copy-and-paste
        // artifact, and leaving it in gives an issuer nobody's discovery
        // document answers to.
        out.push_str(value.trim_end_matches('/'));
        rest = &rest[close + 1..];
    }

    out.push_str(rest);

    Some(out)
}

/// Runs the caller's command and takes what it prints as a bearer token. The
/// command line goes to `sh -c`, or `cmd.exe /c` on Windows, so it is written
/// in that shell's syntax.
///
/// **The command runs with the privileges of the process holding the
/// connection.** Connection options assembled from untrusted input therefore
/// become arbitrary code execution, so the command must never come from such
/// input.
///
/// @param params What the caller said, which must name a command
/// @return What it printed, with the surrounding whitespace gone
pub fn token_from_command(params: &BTreeMap<String, String>) -> AdbcResult<String> {
    let command = required(params)?;
    let timeout = timeout(params);

    let output = runtime::block_on(run(command, timeout))?;

    let Some(output) = output else {
        return Err(invalid(&format!(
            "{}{PARAM_COMMAND} did not finish within {}s; raise \
             {}{PARAM_TIMEOUT} if it is expected to take longer, but note that a command \
             waiting for input will never finish",
            crate::auth::PARAM_PREFIX,
            timeout.as_secs(),
            crate::auth::PARAM_PREFIX,
        )));
    };

    if !output.ok {
        // Only what it printed on standard error: on the path that works,
        // standard output is a token and does not belong in a log.
        return Err(invalid(&format!(
            "{}{PARAM_COMMAND} exited with status {}{}",
            crate::auth::PARAM_PREFIX,
            output.status,
            match first_lines(&output.stderr) {
                said if said.is_empty() => ".".to_string(),
                said => format!(": {said}"),
            }
        )));
    }

    let token = output.stdout.trim();

    if token.is_empty() {
        return Err(invalid(&format!(
            "{}{PARAM_COMMAND} succeeded but printed no token on standard output",
            crate::auth::PARAM_PREFIX
        )));
    }

    Ok(token.to_string())
}

/// Checks the command is there before anything is run, so that a connection
/// misconfigured this way fails saying what is missing rather than saying a
/// shell could not run an empty string.
///
/// @return The command line
pub fn required(params: &BTreeMap<String, String>) -> AdbcResult<&str> {
    lookup(params, PARAM_COMMAND).ok_or_else(|| {
        invalid(&format!(
            "{}=command needs {}{PARAM_COMMAND} to say what to run",
            options::AUTH_PROVIDER,
            crate::auth::PARAM_PREFIX
        ))
    })
}

/// @return How long to wait, which is the default where nothing usable was
///         said -- a connection is not worth failing over a timeout hint
fn timeout(params: &BTreeMap<String, String>) -> Duration {
    lookup(params, PARAM_TIMEOUT)
        .and_then(|said| said.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_TIMEOUT)
}

/// What a command left behind.
struct Output {
    ok: bool,
    status: String,
    stdout: String,
    stderr: String,
}

/// Runs one command under the platform's shell, so that quoting, pipes and
/// redirection behave as they would in a terminal.
///
/// Both streams are drained while the command runs: reading one after waiting
/// deadlocks as soon as the other's pipe buffer fills, and reading either one
/// to the end before waiting would defeat the timeout.
///
/// @return What it left behind, or None if it ran out of time
async fn run(command: &str, timeout: Duration) -> AdbcResult<Option<Output>> {
    use tokio::io::AsyncReadExt;

    let (shell, flag) = match cfg!(windows) {
        true => ("cmd.exe", "/c"),
        false => ("sh", "-c"),
    };

    let mut child = tokio::process::Command::new(shell)
        .arg(flag)
        .arg(command)
        // Nothing to send, and a command that waits on standard input should
        // see the end of it rather than this connection's lifetime.
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // So that giving up on the command also ends it, rather than leaving
        // it running for as long as the process lives.
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            invalid(&format!(
                "{}{PARAM_COMMAND} could not be started: {error}",
                crate::auth::PARAM_PREFIX
            ))
        })?;

    let mut out = child.stdout.take().map(|pipe| pipe.take(MAX_OUTPUT));
    let mut err = child.stderr.take().map(|pipe| pipe.take(MAX_OUTPUT));

    let finished = tokio::time::timeout(timeout, async {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let read_out = async {
            if let Some(pipe) = out.as_mut() {
                let _ = pipe.read_to_end(&mut stdout).await;
            }
        };

        let read_err = async {
            if let Some(pipe) = err.as_mut() {
                let _ = pipe.read_to_end(&mut stderr).await;
            }
        };

        let (status, (), ()) = tokio::join!(child.wait(), read_out, read_err);

        (status, stdout, stderr)
    })
    .await;

    let Ok((status, stdout, stderr)) = finished else {
        return Ok(None);
    };

    let status = status.map_err(|error| {
        invalid(&format!(
            "{}{PARAM_COMMAND} could not be waited for: {error}",
            crate::auth::PARAM_PREFIX
        ))
    })?;

    Ok(Some(Output {
        ok: status.success(),
        status: match status.code() {
            Some(code) => code.to_string(),
            None => "a signal".to_string(),
        },
        // Whatever arrived, even if the command was cut off at the cap or
        // printed something that is not text.
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
    }))
}

/// @return At most the first few lines, joined, so that a command which fails
///         loudly does not fill the error with its own output
fn first_lines(said: &str) -> String {
    said.trim()
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(QUOTED_LINES)
        .collect::<Vec<_>>()
        .join(" | ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn a_provider_is_named_whatever_its_case() {
        assert_eq!(named("Entra").expect("entra"), Provider::Entra);
        assert_eq!(named("  okta  ").expect("okta"), Provider::Okta);
        assert_eq!(named("COMMAND").expect("command"), Provider::Command);
    }

    #[test]
    fn a_provider_nobody_has_heard_of_is_refused_with_what_there_is() {
        let refused = named("ping-identity").expect_err("no such provider");
        let said = refused.message;

        assert!(said.contains("ping-identity"), "{said}");
        assert!(said.contains("command"), "{said}");
        assert!(said.contains("entra"), "{said}");
        assert!(said.contains("okta"), "{said}");
    }

    #[test]
    fn entra_assembles_an_issuer_and_a_scope_out_of_a_tenant() {
        let said = params(&[
            ("tenant_id", "11111111-2222-3333-4444-555555555555"),
            ("application_id_uri", "api://kinetica"),
        ]);

        let derived = Provider::Entra
            .vendor()
            .expect("entra is a vendor")
            .derive(&said, false);

        assert_eq!(
            derived.get("issuer").map(String::as_str),
            Some("https://login.microsoftonline.com/11111111-2222-3333-4444-555555555555/v2.0")
        );
        assert_eq!(
            derived.get("scope").map(String::as_str),
            Some("api://kinetica/.default")
        );
    }

    #[test]
    fn an_interactive_entra_sign_in_also_asks_to_be_renewable() {
        let said = params(&[("tenant_id", "tenant"), ("resource", "api://kinetica")]);
        let vendor = Provider::Entra.vendor().expect("entra is a vendor");

        assert_eq!(
            vendor.derive(&said, true).get("scope").map(String::as_str),
            Some("api://kinetica/.default offline_access")
        );
        assert_eq!(
            vendor.derive(&said, false).get("scope").map(String::as_str),
            Some("api://kinetica/.default"),
            "nobody is present, so there is nobody for a refresh to spare"
        );
    }

    #[test]
    fn a_parameter_said_outright_is_never_rewritten() {
        let said = params(&[
            ("tenant_id", "tenant"),
            ("resource", "api://kinetica"),
            ("issuer", "https://id.example/realms/kinetica"),
            ("scope", "kinetica.read"),
        ]);

        let derived = Provider::Entra
            .vendor()
            .expect("entra is a vendor")
            .derive(&said, false);

        assert!(derived.is_empty(), "{derived:?}");
    }

    #[test]
    fn a_derivation_with_nothing_to_go_on_simply_does_not_apply() {
        // A tenant names Entra, but with nothing saying what the token is
        // for there is no scope to assemble.
        let said = params(&[("tenant_id", "tenant")]);

        let derived = Provider::Entra
            .vendor()
            .expect("entra is a vendor")
            .derive(&said, false);

        assert!(derived.contains_key("issuer"), "{derived:?}");
        assert!(!derived.contains_key("scope"), "{derived:?}");
    }

    #[test]
    fn okta_falls_back_to_the_org_authorization_server() {
        let vendor = Provider::Okta.vendor().expect("okta is a vendor");

        assert_eq!(
            vendor
                .derive(&params(&[("okta_domain", "example.okta.com")]), false)
                .get("issuer")
                .map(String::as_str),
            Some("https://example.okta.com/oauth2/default")
        );

        assert_eq!(
            vendor
                .derive(
                    &params(&[
                        ("okta_domain", "example.okta.com/"),
                        ("auth_server_id", "aus1")
                    ]),
                    false
                )
                .get("issuer")
                .map(String::as_str),
            Some("https://example.okta.com/oauth2/aus1"),
            "a pasted trailing slash does not become an issuer nobody answers to"
        );
    }

    #[test]
    fn a_vendor_is_known_from_a_parameter_nothing_else_reads() {
        assert_eq!(
            infer(&params(&[("tenant_id", "tenant")])).map(Vendor::name),
            Some("entra")
        );
        assert_eq!(
            infer(&params(&[("okta_domain", "example.okta.com")])).map(Vendor::name),
            Some("okta")
        );
        assert_eq!(
            infer(&params(&[
                ("issuer", "https://id.example"),
                ("client_id", "c")
            ]))
            .map(Vendor::name),
            None,
            "a generic flow implies nobody"
        );
    }

    #[test]
    fn a_command_is_what_it_prints() {
        let token =
            token_from_command(&params(&[("command", "echo not-a-jwt")])).expect("the command ran");

        assert_eq!(token, "not-a-jwt");
    }

    /// @return The command line for whichever shell runs it here: cmd.exe
    ///         separates commands with `&`, where sh runs one in the background
    fn for_shell(sh: &'static str, cmd: &'static str) -> &'static str {
        if cfg!(windows) {
            cmd
        } else {
            sh
        }
    }

    #[test]
    fn a_command_that_fails_says_what_it_complained_about() {
        let command = for_shell("echo nope 1>&2; exit 3", "echo nope 1>&2 & exit 3");
        let refused =
            token_from_command(&params(&[("command", command)])).expect_err("the command failed");

        assert!(refused.message.contains("status 3"), "{}", refused.message);
        assert!(refused.message.contains("nope"), "{}", refused.message);
    }

    #[test]
    fn a_command_that_prints_nothing_is_not_a_token() {
        // cmd.exe has no true, and rem is a comment that prints nothing
        let refused = token_from_command(&params(&[("command", for_shell("true", "rem"))]))
            .expect_err("nothing was printed");

        assert!(refused.message.contains("no token"), "{}", refused.message);
    }

    #[test]
    fn a_command_that_will_never_finish_is_given_up_on() {
        let refused = token_from_command(&params(&[
            // cmd.exe has no sleep, and its timeout refuses to run with
            // standard input closed, as it is here
            (
                "command",
                for_shell("sleep 30", "ping -n 31 127.0.0.1 > nul"),
            ),
            ("command_timeout_seconds", "1"),
        ]))
        .expect_err("it ran out of time");

        assert!(
            refused.message.contains("did not finish within 1s"),
            "{}",
            refused.message
        );
    }

    #[test]
    fn a_command_nobody_named_is_refused_before_a_shell_is_started() {
        let refused = required(&params(&[])).expect_err("no command");

        assert!(refused.message.contains("command"), "{}", refused.message);
    }
}
