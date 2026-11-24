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

//! The OAuth flows, against an identity provider of this test's own.
//!
//! A real provider is not needed to show that a flow is run correctly: what
//! matters is that the right grant goes to the right endpoint with the right
//! fields, and that the token that comes back is the one presented. So these
//! stand up a few lines of HTTP that answer as a provider would, and read
//! back what the driver sent it.
//!
//! The browser and device flows are left out: both wait for a person, and a
//! test that needs someone to click something is not a test that can run.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{mpsc, OnceLock};
use std::time::Duration;

use adbc_driver_kinetica::auth::Auth;
use adbc_driver_kinetica::options;

/// A private key for signing assertions, and nothing else: made when it is
/// first asked for and never stored. The provider here reads an assertion's
/// claims and does not check its signature, so any key will do.
/// @return The key, as the PKCS#8 PEM the driver reads
fn test_key() -> &'static str {
    use rsa::pkcs8::{EncodePrivateKey, LineEnding};

    static KEY: OnceLock<String> = OnceLock::new();

    KEY.get_or_init(|| {
        let key = rsa::RsaPrivateKey::new(&mut rand::rngs::OsRng, 2048).expect("an RSA key");
        key.to_pkcs8_pem(LineEnding::LF)
            .expect("the key as PEM")
            .to_string()
    })
}

/// What one request to the provider looked like.
#[derive(Debug, Clone)]
struct Seen {
    path: String,
    body: String,
}

impl Seen {
    /// @return The form field's value, if the request carried it
    fn field(&self, name: &str) -> Option<String> {
        self.body.split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=')?;

            (key == name).then(|| unescape(value))
        })
    }
}

/// @return The value with its percent escapes read back
fn unescape(value: &str) -> String {
    let bytes = value.replace('+', " ").into_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(&String::from_utf8_lossy(&bytes[i + 1..i + 3]), 16)
            {
                out.push(byte);
                i += 3;
                continue;
            }
        }

        out.push(bytes[i]);
        i += 1;
    }

    String::from_utf8_lossy(&out).into_owned()
}

/// How the provider should answer, beyond the token itself.
#[derive(Clone, Copy)]
struct Answers {
    /// What a token request gets.
    token: &'static str,
    /// What a `refresh_token` grant gets instead of the usual token.
    renewed: Option<&'static str>,
    /// Whether to decline a `refresh_token` grant, as a provider whose
    /// refresh token has been revoked or rotated away would.
    refuses_renewal: bool,
    /// The refresh token handed out with the first token.
    refresh: Option<&'static str>,
    /// The refresh token handed out with a renewed one, which a provider
    /// that does not rotate them leaves out.
    rotated: Option<&'static str>,
    /// Seconds the token lasts. Zero makes it stale the moment it arrives.
    expires_in: u64,
    /// How long to dawdle before answering a token request, so that callers
    /// racing for one are all inside the window at once.
    slow_by: Duration,
}

impl Answers {
    /// @return A provider that hands out one long-lived token and nothing to
    ///         renew it with, which is what most of these tests want
    fn of(token: &'static str) -> Self {
        Self {
            token,
            renewed: None,
            refuses_renewal: false,
            refresh: None,
            rotated: None,
            expires_in: 3600,
            slow_by: Duration::ZERO,
        }
    }

    /// @return The JSON for one token response
    fn body(&self, renewing: bool) -> String {
        let token = match renewing {
            true => self.renewed.unwrap_or(self.token),
            false => self.token,
        };

        let refresh = match renewing {
            true => self.rotated,
            false => self.refresh,
        };

        let carried = match refresh {
            Some(refresh) => format!(r#","refresh_token":"{refresh}""#),
            None => String::new(),
        };

        format!(
            r#"{{"access_token":"{token}","token_type":"Bearer","expires_in":{}{carried}}}"#,
            self.expires_in
        )
    }
}

/// A few lines of HTTP standing in for an identity provider.
struct Provider {
    url: String,
    seen: mpsc::Receiver<Seen>,
}

impl Provider {
    /// Answers every token request with the same token, and serves an OpenID
    /// Connect document pointing back at itself.
    /// @return The provider, listening
    fn new(token: &'static str) -> Self {
        Self::serving(Answers::of(token))
    }

    /// @param answers What it should say, and to what
    /// @return The provider, listening
    fn serving(answers: Answers) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
        let port = listener.local_addr().expect("an address").port();
        let url = format!("http://127.0.0.1:{port}");

        let (sent, seen) = mpsc::channel();
        let served = url.clone();

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let sent = sent.clone();
                let served = served.clone();

                // One thread each: a provider that answers in series would
                // serialize the callers racing for a token and hide the very
                // thing the gate is there to prevent.
                std::thread::spawn(move || {
                    let mut stream = stream;
                    let mut reader =
                        BufReader::new(stream.try_clone().expect("the request can be read"));

                    let mut request = String::new();
                    if reader.read_line(&mut request).is_err() {
                        return;
                    }

                    let path = request
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or_default()
                        .to_string();

                    // Headers, to find how much body there is.
                    let mut length = 0;
                    loop {
                        let mut header = String::new();
                        if reader.read_line(&mut header).unwrap_or(0) == 0 || header == "\r\n" {
                            break;
                        }

                        if let Some(value) = header.to_lowercase().strip_prefix("content-length:") {
                            length = value.trim().parse().unwrap_or(0);
                        }
                    }

                    let mut body = vec![0; length];
                    let _ = reader.read_exact(&mut body);
                    let body = String::from_utf8_lossy(&body).into_owned();

                    let renewing = body.contains("grant_type=refresh_token");

                    let _ = sent.send(Seen {
                        path: path.clone(),
                        body,
                    });

                    let refused = renewing && answers.refuses_renewal;

                    let answer = if path.contains("openid-configuration") {
                        format!(
                            r#"{{"issuer":"{served}","token_endpoint":"{served}/token",
                            "authorization_endpoint":"{served}/authorize",
                            "device_authorization_endpoint":"{served}/device"}}"#
                        )
                    } else if refused {
                        r#"{"error":"invalid_grant"}"#.to_string()
                    } else {
                        answers.body(renewing)
                    };

                    let status = match refused {
                        true => "400 Bad Request",
                        false => "200 OK",
                    };

                    if !path.contains("openid-configuration") {
                        std::thread::sleep(answers.slow_by);
                    }

                    let _ = write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                    answer.len()
                );
                });
            }
        });

        Self { url, seen }
    }

    /// @return Every request the provider was sent
    fn requests(&self) -> Vec<Seen> {
        self.seen.try_iter().collect()
    }
}

/// @return An Auth with the given options on it, and the cache turned off so
///         that one test's token is never handed to another
fn auth(options_set: &[(&str, &str)]) -> Auth {
    let mut auth = Auth::default();

    auth.set(options::OAUTH_TOKEN_CACHE, "none")
        .expect("the cache can be turned off");

    for (name, value) in options_set {
        assert!(
            auth.set(name, value).expect("the option is accepted"),
            "{name} should be an option this driver knows"
        );
    }

    auth
}

#[test]
fn client_credentials_are_sent_to_the_token_endpoint() {
    let provider = Provider::new("a-client-credentials-token");

    let auth = auth(&[
        (options::OAUTH_GRANT, "client_credentials"),
        (
            "kinetica.oauth.token_url",
            &format!("{}/token", provider.url),
        ),
        ("kinetica.oauth.client_id", "analytics"),
        ("kinetica.oauth.client_secret", "a-secret"),
        ("kinetica.oauth.scope", "kinetica read"),
    ]);

    let token = auth.bearer().expect("a token").expect("a token").token;
    let requests = provider.requests();

    assert_eq!(token, "a-client-credentials-token");
    assert_eq!(requests.len(), 1, "one request, to the token endpoint");
    assert_eq!(requests[0].path, "/token");
    assert_eq!(
        requests[0].field("grant_type").as_deref(),
        Some("client_credentials")
    );
    assert_eq!(requests[0].field("scope").as_deref(), Some("kinetica read"));
}

#[test]
fn the_endpoints_are_found_from_the_issuer_alone() {
    let provider = Provider::new("a-discovered-token");

    let auth = auth(&[
        (options::OAUTH_GRANT, "client_credentials"),
        ("kinetica.oauth.issuer", &provider.url),
        ("kinetica.oauth.client_id", "analytics"),
    ]);

    let token = auth.bearer().expect("a token").expect("a token").token;
    let requests = provider.requests();

    assert_eq!(token, "a-discovered-token");
    assert_eq!(requests.len(), 2, "the document, then the token");
    assert!(
        requests[0]
            .path
            .contains("/.well-known/openid-configuration"),
        "the document should be asked for first, got {}",
        requests[0].path
    );
    assert_eq!(requests[1].path, "/token");
}

#[test]
fn a_refresh_token_is_exchanged_for_an_access_token() {
    let provider = Provider::new("a-refreshed-token");

    let auth = auth(&[
        (options::OAUTH_GRANT, "refresh_token"),
        (
            "kinetica.oauth.token_url",
            &format!("{}/token", provider.url),
        ),
        ("kinetica.oauth.client_id", "analytics"),
        ("kinetica.oauth.refresh_token", "the-refresh-token"),
    ]);

    let token = auth.bearer().expect("a token").expect("a token").token;
    let requests = provider.requests();

    assert_eq!(token, "a-refreshed-token");
    assert_eq!(
        requests[0].field("grant_type").as_deref(),
        Some("refresh_token")
    );
    assert_eq!(
        requests[0].field("refresh_token").as_deref(),
        Some("the-refresh-token")
    );
}

#[test]
fn a_token_from_elsewhere_is_exchanged_for_one_scoped_here() {
    let provider = Provider::new("an-exchanged-token");

    let auth = auth(&[
        (options::OAUTH_GRANT, "token-exchange"),
        (
            "kinetica.oauth.token_url",
            &format!("{}/token", provider.url),
        ),
        ("kinetica.oauth.client_id", "analytics"),
        ("kinetica.oauth.subject_token", "a-token-for-somewhere-else"),
        ("kinetica.oauth.audience", "kinetica"),
    ]);

    let token = auth.bearer().expect("a token").expect("a token").token;
    let requests = provider.requests();

    assert_eq!(token, "an-exchanged-token");
    assert_eq!(
        requests[0].field("grant_type").as_deref(),
        Some("urn:ietf:params:oauth:grant-type:token-exchange")
    );
    assert_eq!(
        requests[0].field("subject_token").as_deref(),
        Some("a-token-for-somewhere-else")
    );
    assert_eq!(requests[0].field("audience").as_deref(), Some("kinetica"));
}

#[test]
fn a_jwt_bearer_assertion_is_signed_and_says_who_it_is() {
    let provider = Provider::new("an-asserted-token");

    let auth = auth(&[
        (options::OAUTH_GRANT, "jwt-bearer"),
        (
            "kinetica.oauth.token_url",
            &format!("{}/token", provider.url),
        ),
        ("kinetica.oauth.client_id", "a-service"),
        ("kinetica.oauth.private_key", test_key()),
        ("kinetica.oauth.key_id", "the-key"),
        ("kinetica.oauth.audience", "kinetica"),
    ]);

    let token = auth.bearer().expect("a token").expect("a token").token;
    let requests = provider.requests();

    assert_eq!(token, "an-asserted-token");
    assert_eq!(
        requests[0].field("grant_type").as_deref(),
        Some("urn:ietf:params:oauth:grant-type:jwt-bearer")
    );

    // The assertion is a JWT: three parts, and the claims say who is asking
    // and of whom.
    let assertion = requests[0].field("assertion").expect("an assertion");
    let parts: Vec<&str> = assertion.split('.').collect();

    assert_eq!(parts.len(), 3, "an assertion is a signed JWT: {assertion}");

    let claims = String::from_utf8(decode_part(parts[1])).expect("the claims are text");

    assert!(claims.contains("\"iss\":\"a-service\""), "{claims}");
    assert!(claims.contains("\"aud\":\"kinetica\""), "{claims}");
}

#[test]
fn a_key_that_cannot_be_read_is_reported_rather_than_sent() {
    let auth = auth(&[
        (options::OAUTH_GRANT, "jwt-bearer"),
        ("kinetica.oauth.token_url", "http://127.0.0.1:1/token"),
        ("kinetica.oauth.client_id", "a-service"),
        ("kinetica.oauth.private_key", "not a key at all"),
    ]);

    let error = auth
        .bearer()
        .expect_err("a key that is not one should be refused");

    assert!(
        error.message.contains("private key"),
        "the message should say what was wrong: {}",
        error.message
    );
}

#[test]
fn a_provider_that_cannot_be_reached_is_reported_with_the_flow_that_failed() {
    let auth = auth(&[
        (options::OAUTH_GRANT, "client_credentials"),
        // Port 1 is not something that answers.
        ("kinetica.oauth.token_url", "http://127.0.0.1:1/token"),
        ("kinetica.oauth.client_id", "analytics"),
    ]);

    let error = auth
        .bearer()
        .expect_err("an unreachable provider should fail");

    assert!(
        error.message.contains("client credentials were refused"),
        "the message should name the flow: {}",
        error.message
    );
}

#[test]
fn one_token_is_shared_by_everything_that_would_have_asked_for_it() {
    let provider = Provider::new("a-shared-token");

    let options_set = [
        (options::OAUTH_GRANT, "client_credentials"),
        (
            "kinetica.oauth.token_url",
            &format!("{}/token", provider.url),
        ),
        ("kinetica.oauth.client_id", "shared-identity"),
    ];

    // The cache left on this time, which is the default.
    let mut first = Auth::default();
    for (name, value) in &options_set {
        first.set(name, value).expect("the option is accepted");
    }

    let second = first.clone();

    let taken = |auth: &Auth| auth.bearer().unwrap().map(|held| held.token);

    assert_eq!(taken(&first).as_deref(), Some("a-shared-token"));
    assert_eq!(taken(&second).as_deref(), Some("a-shared-token"));

    assert_eq!(
        provider.requests().len(),
        1,
        "the second should have used the token the first got"
    );
}

/// @return The bytes of one base64url part of a JWT
fn decode_part(part: &str) -> Vec<u8> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

    let mut bits = 0u32;
    let mut held = 0;
    let mut out = Vec::new();

    for byte in part.bytes() {
        let Some(value) = ALPHABET.iter().position(|c| *c == byte) else {
            continue;
        };

        bits = (bits << 6) | value as u32;
        held += 6;

        if held >= 8 {
            held -= 8;
            out.push((bits >> held) as u8);
        }
    }

    out
}

// --------------------------------------------------------------------------
// Providers

/// A token carrying claims and no signature worth the name, since nothing in
/// this driver checks one: `preferred_username` is `alice@example.com` and
/// `sub` is the opaque identifier a vendor would put there instead.
const CLAIMED: &str = concat!(
    "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.",
    "eyJwcmVmZXJyZWRfdXNlcm5hbWUiOiJhbGljZUBleGFtcGxlLmNvbSIsInN1YiI6IjBhMWItMmMzZCJ9.",
    "not-a-signature"
);

#[test]
fn a_vendors_own_parameters_become_the_ones_the_flow_sends() {
    let provider = Provider::new("an-entra-token");

    let auth = auth(&[
        (options::AUTH_METHOD, "oauth"),
        (options::AUTH_PROVIDER, "entra"),
        (options::OAUTH_GRANT, "client_credentials"),
        // Where login.microsoftonline.com would be, so that what the vendor
        // assembles is something this test can answer.
        ("kinetica.oauth.authority_host", &provider.url),
        ("kinetica.oauth.tenant_id", "a-tenant"),
        ("kinetica.oauth.application_id_uri", "api://kinetica"),
        ("kinetica.oauth.client_id", "analytics"),
        ("kinetica.oauth.client_secret", "a-secret"),
    ]);

    let token = auth.bearer().expect("a token").expect("a token").token;
    let requests = provider.requests();

    assert_eq!(token, "an-entra-token");
    assert_eq!(requests.len(), 2, "the document, then the token");
    assert!(
        requests[0].path.starts_with("/a-tenant/v2.0/.well-known/"),
        "the issuer should be assembled from the tenant, got {}",
        requests[0].path
    );
    assert_eq!(
        requests[1].field("scope").as_deref(),
        Some("api://kinetica/.default"),
        "the scope should be assembled from what the token is for"
    );
}

#[test]
fn a_vendor_does_not_have_to_be_named_to_be_used() {
    let provider = Provider::new("an-implied-token");

    // No auth_provider at all: a tenant is a parameter nothing else reads,
    // so it settles the vocabulary on its own.
    let auth = auth(&[
        (options::AUTH_METHOD, "oauth"),
        (options::OAUTH_GRANT, "client_credentials"),
        ("kinetica.oauth.authority_host", &provider.url),
        ("kinetica.oauth.tenant_id", "a-tenant"),
        ("kinetica.oauth.resource", "api://kinetica"),
        ("kinetica.oauth.client_id", "analytics"),
    ]);

    let token = auth.bearer().expect("a token").expect("a token").token;
    let requests = provider.requests();

    assert_eq!(token, "an-implied-token");
    assert!(
        requests[0].path.starts_with("/a-tenant/v2.0/"),
        "got {}",
        requests[0].path
    );
    assert_eq!(
        requests[1].field("scope").as_deref(),
        Some("api://kinetica/.default")
    );
}

#[test]
fn an_issuer_said_outright_wins_over_the_one_a_vendor_would_assemble() {
    let provider = Provider::new("a-named-issuer-token");

    let auth = auth(&[
        (options::AUTH_METHOD, "oauth"),
        (options::AUTH_PROVIDER, "entra"),
        (options::OAUTH_GRANT, "client_credentials"),
        ("kinetica.oauth.tenant_id", "a-tenant"),
        ("kinetica.oauth.issuer", &provider.url),
        ("kinetica.oauth.scope", "kinetica.read"),
        ("kinetica.oauth.client_id", "analytics"),
    ]);

    let token = auth.bearer().expect("a token").expect("a token").token;
    let requests = provider.requests();

    assert_eq!(token, "a-named-issuer-token");
    assert_eq!(
        requests[0].path, "/.well-known/openid-configuration",
        "the tenant should not have been assembled over an issuer that was given"
    );
    assert_eq!(requests[1].field("scope").as_deref(), Some("kinetica.read"));
}

#[test]
fn a_token_from_a_flow_says_who_holds_it() {
    let provider = Provider::new(CLAIMED);

    let auth = auth(&[
        (options::AUTH_METHOD, "oauth"),
        (options::AUTH_PROVIDER, "entra"),
        (options::OAUTH_GRANT, "client_credentials"),
        ("kinetica.oauth.issuer", &provider.url),
        ("kinetica.oauth.client_id", "analytics"),
    ]);

    let held = auth.bearer().expect("a token").expect("a token");

    assert_eq!(
        held.user.as_deref(),
        Some("alice@example.com"),
        "entra names preferred_username, because its sub is not a name"
    );
}

#[test]
fn a_caller_can_name_the_claim_a_vendor_would_have_chosen_for_it() {
    let provider = Provider::new(CLAIMED);

    let auth = auth(&[
        (options::AUTH_METHOD, "oauth"),
        (options::AUTH_PROVIDER, "entra"),
        (options::TOKEN_NAME_CLAIM, "sub"),
        (options::OAUTH_GRANT, "client_credentials"),
        ("kinetica.oauth.issuer", &provider.url),
        ("kinetica.oauth.client_id", "analytics"),
    ]);

    let held = auth.bearer().expect("a token").expect("a token");

    assert_eq!(
        held.user.as_deref(),
        Some("0a1b-2c3d"),
        "a caller that named a claim meant it, vendor or no vendor"
    );
}

#[test]
fn a_command_of_the_callers_own_produces_the_token() {
    let auth = auth(&[
        (options::AUTH_METHOD, "oauth"),
        (options::AUTH_PROVIDER, "command"),
        ("kinetica.oauth.command", &format!("echo {CLAIMED}")),
    ]);

    let held = auth.bearer().expect("a token").expect("a token");

    assert_eq!(held.token, CLAIMED);
    assert_eq!(
        held.user.as_deref(),
        Some("0a1b-2c3d"),
        "nobody named a vendor, so the claim is the subject identifier"
    );
}

#[test]
fn a_command_that_fails_is_reported_with_what_it_complained_about() {
    let auth = auth(&[
        (options::AUTH_METHOD, "oauth"),
        (options::AUTH_PROVIDER, "command"),
        (
            "kinetica.oauth.command",
            // Under cmd.exe, which runs it on Windows, commands are separated
            // with & (where sh would run the first in the background), and
            // quotes are printed rather than taken away
            if cfg!(windows) {
                "echo no such profile 1>&2 & exit 2"
            } else {
                "echo 'no such profile' 1>&2; exit 2"
            },
        ),
    ]);

    let error = auth.bearer().expect_err("the command failed");

    assert!(
        error.message.contains("status 2") && error.message.contains("no such profile"),
        "the message should carry what the command said: {}",
        error.message
    );
}

// --------------------------------------------------------------------------
// Renewal
//
// The flows that make renewal matter are the interactive ones: when a held
// token goes off, signing in again costs whoever is sitting there a browser
// window, where renewing costs a form post nobody sees. Neither of those
// flows can be driven from a test, since both wait for a person, so
// client_credentials stands in for them here -- the renewal path is the same
// one, and what it does is what these read back.

/// Keeps the token store in play, unlike `auth` above: these tests are about
/// what happens to a token that was held.
///
/// @param provider The provider to sign in against
/// @param client_id What keys the held token, so one test's does not reach
///        another's
/// @return A connection against that provider
fn against(provider: &Provider, client_id: &str) -> Auth {
    let mut auth = Auth::default();

    for (name, value) in [
        (options::OAUTH_GRANT, "client_credentials"),
        (
            "kinetica.oauth.token_url",
            &format!("{}/token", provider.url),
        ),
        ("kinetica.oauth.client_id", client_id),
    ] {
        auth.set(name, value).expect("the option is accepted");
    }

    auth
}

#[test]
fn an_expiring_token_is_renewed_rather_than_signed_in_for_again() {
    let provider = Provider::serving(Answers {
        renewed: Some("a-renewed-token"),
        refresh: Some("a-refresh-token"),
        rotated: Some("a-rotated-refresh-token"),
        // Stale the moment it arrives, which is what a token held past its
        // lifetime looks like without the test waiting for one.
        expires_in: 0,
        ..Answers::of("a-first-token")
    });

    let auth = against(&provider, "renewing-identity");

    assert_eq!(
        auth.bearer().unwrap().unwrap().token,
        "a-first-token",
        "the flow runs the first time, because nothing is held"
    );
    assert_eq!(
        auth.bearer().unwrap().unwrap().token,
        "a-renewed-token",
        "the second time it is renewed, not signed in for again"
    );

    let requests = provider.requests();

    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].field("grant_type").as_deref(),
        Some("client_credentials")
    );
    assert_eq!(
        requests[1].field("grant_type").as_deref(),
        Some("refresh_token"),
        "the second request should renew rather than start over"
    );
    assert_eq!(
        requests[1].field("refresh_token").as_deref(),
        Some("a-refresh-token"),
        "with what the expiring token left behind"
    );
}

#[test]
fn a_refresh_token_a_provider_does_not_rotate_is_kept_for_the_next_renewal() {
    let provider = Provider::serving(Answers {
        renewed: Some("a-renewed-token"),
        refresh: Some("the-only-refresh-token"),
        // Nothing rotated back, which is what a provider that reuses one
        // does: it returns no refresh_token at all.
        rotated: None,
        expires_in: 0,
        ..Answers::of("a-first-token")
    });

    let auth = against(&provider, "non-rotating-identity");

    for _ in 0..3 {
        auth.bearer().expect("a token").expect("a token");
    }

    let renewals: Vec<_> = provider
        .requests()
        .into_iter()
        .filter(|seen| seen.field("grant_type").as_deref() == Some("refresh_token"))
        .collect();

    assert_eq!(renewals.len(), 2, "the second and third times renew");
    assert!(
        renewals
            .iter()
            .all(|seen| seen.field("refresh_token").as_deref() == Some("the-only-refresh-token")),
        "dropping it when none came back would make the first renewal the last"
    );
}

#[test]
fn a_renewal_that_is_refused_falls_back_to_signing_in_again() {
    let provider = Provider::serving(Answers {
        refresh: Some("a-revoked-refresh-token"),
        refuses_renewal: true,
        expires_in: 0,
        ..Answers::of("a-token")
    });

    let auth = against(&provider, "revoked-identity");

    assert_eq!(auth.bearer().unwrap().unwrap().token, "a-token");
    assert_eq!(
        auth.bearer().unwrap().unwrap().token,
        "a-token",
        "a refused renewal is not an error; the flow runs instead"
    );

    let grants: Vec<_> = provider
        .requests()
        .into_iter()
        .filter_map(|seen| seen.field("grant_type"))
        .collect();

    assert_eq!(
        grants,
        vec!["client_credentials", "refresh_token", "client_credentials"],
        "renewal is tried, declined, and the sign-in happens anyway"
    );
}

#[test]
fn an_expired_token_with_nothing_to_renew_it_goes_straight_back_to_the_flow() {
    let provider = Provider::serving(Answers {
        // No refresh token issued at all, which is the usual shape of a
        // client-credentials response.
        refresh: None,
        expires_in: 0,
        ..Answers::of("a-token")
    });

    let auth = against(&provider, "unrenewable-identity");

    auth.bearer().expect("a token").expect("a token");
    auth.bearer().expect("a token").expect("a token");

    let grants: Vec<_> = provider
        .requests()
        .into_iter()
        .filter_map(|seen| seen.field("grant_type"))
        .collect();

    assert_eq!(
        grants,
        vec!["client_credentials", "client_credentials"],
        "nothing to renew with, so nothing is tried"
    );
}

#[test]
fn a_pool_opening_many_connections_at_once_signs_in_once() {
    // Slow enough that every thread is inside the window where nothing is
    // held yet, which is the only moment the race exists.
    let provider = Provider::serving(Answers {
        slow_by: Duration::from_millis(300),
        ..Answers::of("a-contended-token")
    });

    let auth = against(&provider, "contended-identity");

    let racing: Vec<_> = (0..8)
        .map(|_| {
            let auth = auth.clone();

            std::thread::spawn(move || auth.bearer().expect("a token").expect("a token").token)
        })
        .collect();

    for thread in racing {
        assert_eq!(
            thread.join().expect("the thread finished"),
            "a-contended-token"
        );
    }

    let tokens = provider
        .requests()
        .into_iter()
        .filter(|seen| seen.field("grant_type").is_some())
        .count();

    assert_eq!(
        tokens, 1,
        "eight callers wanting the same token should sign in once between \
         them, not eight times -- which for an interactive flow would be \
         eight browser windows"
    );
}
