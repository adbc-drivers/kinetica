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

//! How a connection proves who it is.
//!
//! Two axes: the mechanism, and -- for OAuth -- the flow that gets a token.
//! Everything a flow needs beyond that is a free-form parameter, so a provider
//! with a quirk of its own does not need a new option here.
//!
//! The flows themselves are the `oauth2` crate's rather than written again:
//! client credentials, the device flow and the authorization code flow with
//! PKCE are all it, and the two grants it has no request type for -- the JWT
//! bearer assertion and RFC 8693 token exchange -- are form posts built here
//! against the same token endpoint. Assertions are signed with
//! `jsonwebtoken`.
//!
//! Tokens are held for the process, not the connection: a pool opening twenty
//! connections against one identity should sign in once. The cache can be
//! turned off for a caller that would rather it were not.
//!
//! A third axis, the provider, lives in [`crate::provider`]: whose vocabulary
//! the parameters are written in, or -- for `command` -- where the token
//! comes from instead of a flow. It is resolved here, before a flow runs,
//! because what it derives is what the flow and the token cache both read.
//!
//! Whatever a token turns out to be, its claims are read for the name of
//! whoever holds it. The signature is not checkable here -- only the issuing
//! server can do that -- so those claims serve for saying who this is and for
//! knowing when to stop using the token, and never for deciding what the
//! holder may do.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, SystemTime};

use adbc_core::error::Result as AdbcResult;

use base64::Engine as _;

use oauth2::basic::BasicClient;
use oauth2::{
    AuthUrl, ClientId, ClientSecret, DeviceAuthorizationUrl, PkceCodeChallenge, RedirectUrl,
    RefreshToken, Scope, TokenResponse, TokenUrl,
};

use serde_json::{Map, Value};

use crate::error::invalid;
use crate::options;
use crate::provider::{self, Provider, Vendor};
use crate::runtime;

/// The most of a token's life to give back, so that a request is not sent
/// with one that dies on the way. It covers the difference between this
/// clock and the provider's, and the time opening a connection takes.
const MAX_EXPIRY_MARGIN: Duration = Duration::from_secs(300);

/// How much of a token's own lifetime to give back, as a divisor: a fifth.
/// This is what governs for a short-lived token, where a flat five minutes
/// would retire a five-minute token the moment it arrived.
const EXPIRY_FRACTION: u32 = 5;

/// How long to wait for the browser to come back with an authorization code.
const REDIRECT_WAIT: Duration = Duration::from_secs(300);

/// The prefix a flow's own parameters are named under.
pub const PARAM_PREFIX: &str = "kinetica.oauth.";

/// The claim a name is read out of when neither the caller nor the provider
/// names another: OpenID Connect's subject identifier.
pub const DEFAULT_NAME_CLAIM: &str = "sub";

/// How a credential is presented on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Method {
    /// A user name and password, sent as HTTP Basic. The default.
    #[default]
    Password,
    /// A bearer token, however it was obtained.
    OAuth,
}

/// How a bearer token is obtained. Absent means the caller already holds one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Grant {
    /// Machine to machine: no user and no browser.
    ClientCredentials,
    /// Interactive sign-in, through a browser on this host.
    AuthorizationCode,
    /// Interactive sign-in without a browser here: a code is read out and
    /// entered elsewhere.
    DeviceCode,
    /// A refresh token the caller already holds, exchanged for an access one.
    RefreshToken,
    /// A token issued for somewhere else, exchanged for one scoped here.
    TokenExchange,
    /// A key pair: the driver signs its own assertion, so there is no
    /// replayable secret at rest.
    JwtBearer,
}

/// Where acquired tokens are kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Cache {
    /// For the life of the process, shared by every connection that would
    /// have asked for the same token. The default.
    #[default]
    Memory,
    /// Not at all: every connection signs in for itself.
    None,
}

/// A token to present, and whatever it says about who is presenting it.
#[derive(Debug, Clone)]
pub struct Bearer {
    /// What goes in the `Authorization` header.
    pub token: String,
    /// The name its claims give the holder, where it has readable claims and
    /// one of them is the one being read.
    pub user: Option<String>,
}

/// What a caller has said about proving who it is.
#[derive(Debug, Clone, Default)]
pub struct Auth {
    method: Method,
    grant: Option<Grant>,
    /// Whose vocabulary the parameters are written in, or who produces the
    /// token outright.
    provider: Option<Provider>,
    /// A token the caller acquired elsewhere and hands over whole.
    token: String,
    cache: Cache,
    /// The claim a user name is read out of. Left empty rather than defaulted
    /// so that a provider's own claim can still win, which it must not do
    /// over a caller who named one.
    name_claim: String,
    /// Everything a flow needs: the endpoints, the client, the key.
    params: BTreeMap<String, String>,
}

impl Auth {
    /// Takes the option if it is one of these.
    /// @return Whether the name was one of these options
    pub fn set(&mut self, name: &str, value: &str) -> AdbcResult<bool> {
        match name {
            options::AUTH_METHOD => self.method = method(value)?,
            options::OAUTH_GRANT => self.grant = Some(grant(value)?),
            options::AUTH_PROVIDER => self.provider = Some(provider::named(value)?),
            options::OAUTH_TOKEN => self.token = value.to_string(),
            options::OAUTH_TOKEN_CACHE => self.cache = cache(value)?,
            options::TOKEN_NAME_CLAIM => self.name_claim = value.trim().to_string(),
            _ => {
                let Some(param) = name.strip_prefix(PARAM_PREFIX) else {
                    return Ok(false);
                };

                self.params.insert(param.to_string(), value.to_string());
            }
        }

        Ok(true)
    }

    /// @return What was set, or None if the name is not one of these options
    pub fn get(&self, name: &str) -> Option<String> {
        let value = match name {
            options::AUTH_METHOD => match self.method {
                Method::Password => "password".to_string(),
                Method::OAuth => "oauth".to_string(),
            },
            options::OAUTH_GRANT => spelling(self.grant?).to_string(),
            options::AUTH_PROVIDER => self.provider?.as_str().to_string(),
            options::OAUTH_TOKEN => self.token.clone(),
            options::OAUTH_TOKEN_CACHE => match self.cache {
                Cache::Memory => "memory".to_string(),
                Cache::None => "none".to_string(),
            },
            // What will actually be read, rather than what was typed: a
            // provider that names its own claim has already had its say.
            options::TOKEN_NAME_CLAIM => self.claim(),
            _ => {
                let param = name.strip_prefix(PARAM_PREFIX)?;

                self.params.get(param)?.clone()
            }
        };

        Some(value)
    }

    /// The token to present, acquired if the caller asked for a flow rather
    /// than handing one over.
    ///
    /// A password connection has nothing to say here, and neither has an
    /// OAuth one whose caller already holds its token.
    ///
    /// @return The bearer token, or None to authenticate some other way
    pub fn bearer(&self) -> AdbcResult<Option<Bearer>> {
        // A provider is an OAuth thing either way: one vocabulary has nothing
        // to rewrite without a flow to rewrite it for, and the other produces
        // a token that a password connection would not send.
        if self.provider.is_some() && self.method == Method::Password {
            return Err(invalid(&format!(
                "{} is not used with {}=password",
                options::AUTH_PROVIDER,
                options::AUTH_METHOD
            )));
        }

        if self.method == Method::Password && self.grant.is_none() && self.token.is_empty() {
            return Ok(None);
        }

        if self.provider == Some(Provider::Command) {
            return self.run_command().map(Some);
        }

        let vendor = self.vendor();

        let Some(grant) = self.grant else {
            if let Some(vendor) = vendor {
                return Err(invalid(&format!(
                    "{}={} rewrites the parameters of a flow but runs none itself, so {} must \
                     name one: authorization_code for an interactive sign-in, or \
                     client_credentials for a service principal",
                    options::AUTH_PROVIDER,
                    vendor.name(),
                    options::OAUTH_GRANT
                )));
            }

            // Nothing to run: either the caller handed a token over, or it
            // chose OAuth and forgot to say how to get one.
            return match self.token.is_empty() {
                false => Ok(Some(self.bearing(self.token.clone()))),
                true => Err(invalid(&format!(
                    "{} is oauth, so either {} must hold a token or {} must say how to get one",
                    options::AUTH_METHOD,
                    options::OAUTH_TOKEN,
                    options::OAUTH_GRANT
                ))),
            };
        };

        // Before the cache as well as before the flow: what a vendor derives
        // is what distinguishes one identity from another.
        let resolved = self.translated(vendor, grant);

        if resolved.cache == Cache::None {
            return Ok(Some(resolved.bearing(resolved.acquire(grant)?.token)));
        }

        let key = resolved.cache_key(Some(grant));

        if let Some(token) = fresh(&key) {
            return Ok(Some(resolved.bearing(token)));
        }

        // One sign-in per identity at a time. A gate poisoned by a flow that
        // panicked is taken anyway: that someone's sign-in went wrong is no
        // reason for this process never to authenticate again.
        let gate = gate(&key);
        let _signing_in = gate.lock().unwrap_or_else(PoisonError::into_inner);

        // Looked at again now that it is this caller's turn, because whoever
        // went first has very likely just put a token there.
        match held(&key) {
            Holding::Fresh(token) => return Ok(Some(resolved.bearing(token))),
            // Worth trying before the flow: renewing costs a form post, where
            // signing in again costs whoever is sitting there a browser
            // window. Failing costs only that post, since the flow still runs.
            Holding::Stale(refresh) => {
                if let Some(renewed) = resolved.renew(&refresh) {
                    keep(&key, &renewed);

                    return Ok(Some(resolved.bearing(renewed.token)));
                }
            }
            Holding::Nothing => {}
        }

        let taken = resolved.acquire(grant)?;
        keep(&key, &taken);

        Ok(Some(resolved.bearing(taken.token)))
    }

    /// Trades a refresh token for a fresh access token, so that a credential
    /// which cost a sign-in is not replaced by another sign-in.
    ///
    /// Best-effort throughout: a refresh token can be revoked, expired, or
    /// rotated out from under this process, and a provider can simply decline
    /// to renew. None of that is worth surfacing, because the flow runs next
    /// and will say something better if it fails too.
    ///
    /// @param refresh What the expiring token left behind
    /// @return The renewed token, or None to go and get one the long way
    fn renew(&self, refresh: &str) -> Option<Taken> {
        // Nothing to present the renewal as.
        self.param("client_id")?;

        let endpoints = self.endpoints().ok()?;

        let mut renewing = self.clone();
        renewing
            .params
            .insert("refresh_token".to_string(), refresh.to_string());

        let mut renewed = renewing.refresh(&endpoints).ok()?;

        // A provider that rotates refresh tokens returns a replacement; one
        // that does not returns none at all, and dropping the original there
        // would make this renewal the last one.
        if renewed.refresh.is_none() {
            renewed.refresh = Some(refresh.to_string());
        }

        Some(renewed)
    }

    /// Runs the caller's own command and takes what it prints.
    /// @return The token it produced
    fn run_command(&self) -> AdbcResult<Bearer> {
        if let Some(grant) = self.grant {
            return Err(invalid(&format!(
                "{}=command produces a token itself, so {}={} has nothing left to do; drop one \
                 of the two",
                options::AUTH_PROVIDER,
                options::OAUTH_GRANT,
                spelling(grant)
            )));
        }

        // Said before anything is run, so that a connection missing this
        // fails naming it rather than handing a shell an empty string.
        provider::required(&self.params)?;

        if self.cache == Cache::None {
            return Ok(self.bearing(provider::token_from_command(&self.params)?));
        }

        let key = self.cache_key(None);

        if let Some(token) = fresh(&key) {
            return Ok(self.bearing(token));
        }

        let gate = gate(&key);
        let _running = gate.lock().unwrap_or_else(PoisonError::into_inner);

        // A command is its own renewal -- running it again is what a refresh
        // token would have bought -- so a stale entry goes straight back to
        // it rather than trying to trade anything.
        if let Some(token) = fresh(&key) {
            return Ok(self.bearing(token));
        }

        // No endpoint said how long this lasts, so the token's own expiry is
        // the only one there is; an opaque one has none and is held until the
        // process ends.
        let taken = Taken {
            token: provider::token_from_command(&self.params)?,
            expires: None,
            refresh: None,
        };

        keep(&key, &taken);

        Ok(self.bearing(taken.token))
    }

    /// This, with one vendor's vocabulary turned into the generic one.
    ///
    /// Idempotent, because every derivation fires only where its target is
    /// absent: translating twice finds the targets already filled and changes
    /// nothing.
    ///
    /// @param vendor The vocabulary, or None to leave the parameters alone
    /// @param grant The flow about to run, which decides whether a refresh
    ///        token is worth asking for
    /// @return A copy the flows and the cache can read
    fn translated(&self, vendor: Option<&'static Vendor>, grant: Grant) -> Self {
        let Some(vendor) = vendor else {
            return self.clone();
        };

        let mut resolved = self.clone();

        resolved
            .params
            .extend(vendor.derive(&self.params, grant == Grant::AuthorizationCode));

        if resolved.name_claim.is_empty() {
            resolved.name_claim = vendor.name_claim().to_string();
        }

        resolved
    }

    /// Whose vocabulary the parameters are written in: named outright, or
    /// implied by a parameter nothing else reads.
    ///
    /// Only a vocabulary is ever implied, and only where a flow was asked
    /// for, so inferring one settles how the parameters are read and never
    /// which flow runs.
    ///
    /// @return The vendor in play, or None if none is
    fn vendor(&self) -> Option<&'static Vendor> {
        match self.provider.and_then(Provider::vendor) {
            Some(named) => Some(named),
            None => self.grant.and_then(|_| provider::infer(&self.params)),
        }
    }

    /// @return The claim a user name is read out of: the one the caller
    ///         named, else the one this provider puts a usable name in, else
    ///         the subject identifier
    fn claim(&self) -> String {
        if !self.name_claim.is_empty() {
            return self.name_claim.clone();
        }

        self.vendor()
            .map(|vendor| vendor.name_claim().to_string())
            .unwrap_or_else(|| DEFAULT_NAME_CLAIM.to_string())
    }

    /// @return The token together with whatever its claims call the holder
    fn bearing(&self, token: String) -> Bearer {
        let user = claims(&token).and_then(|claims| name_in(&claims, &self.claim()));

        Bearer { token, user }
    }

    /// Everything that decides which token this is. A caller signing in as
    /// two identities, or for two scopes, must not be handed one for the
    /// other.
    /// @return The key this connection's token is held under
    fn cache_key(&self, grant: Option<Grant>) -> String {
        format!(
            "{grant:?}|{}|{}|{}|{}|{}|{}",
            self.provider.map(Provider::as_str).unwrap_or_default(),
            // Two different commands are two different identities, however
            // alike everything else about them looks.
            self.param("command").unwrap_or_default(),
            self.endpoint_hint(),
            self.param("client_id").unwrap_or_default(),
            self.param("scope").unwrap_or_default(),
            self.param("subject").unwrap_or_default(),
        )
    }

    /// @return Whatever names the provider, for telling two apart
    fn endpoint_hint(&self) -> String {
        self.param("issuer")
            .or_else(|| self.param("token_url"))
            .unwrap_or_default()
    }

    /// @return The parameter, if it was given and is not empty
    fn param(&self, name: &str) -> Option<String> {
        self.params
            .get(name)
            .filter(|value| !value.is_empty())
            .cloned()
    }

    /// @return The parameter, or an error naming what is missing
    fn required(&self, name: &str) -> AdbcResult<String> {
        self.param(name).ok_or_else(|| {
            invalid(&format!(
                "{PARAM_PREFIX}{name} is needed for this OAuth flow"
            ))
        })
    }

    /// Runs the flow the caller asked for.
    /// @return What the flow came back with
    fn acquire(&self, grant: Grant) -> AdbcResult<Taken> {
        let endpoints = self.endpoints()?;

        match grant {
            Grant::ClientCredentials => self.client_credentials(&endpoints),
            Grant::RefreshToken => self.refresh(&endpoints),
            Grant::DeviceCode => self.device_code(&endpoints),
            Grant::AuthorizationCode => self.authorization_code(&endpoints),
            Grant::JwtBearer => self.jwt_bearer(&endpoints),
            Grant::TokenExchange => self.token_exchange(&endpoints),
        }
    }

    /// Where the provider takes its requests: said outright, or asked for
    /// once with OpenID Connect discovery.
    /// @return The endpoints this flow needs
    fn endpoints(&self) -> AdbcResult<Endpoints> {
        let mut endpoints = Endpoints {
            token: self.param("token_url"),
            authorize: self.param("authorize_url"),
            device: self.param("device_url"),
        };

        // Discovery is only worth a round trip for what was not said.
        if endpoints.token.is_none() || endpoints.authorize.is_none() || endpoints.device.is_none()
        {
            if let Some(issuer) = self.param("issuer") {
                let found = discover(&issuer)?;

                endpoints.token = endpoints.token.or(found.token);
                endpoints.authorize = endpoints.authorize.or(found.authorize);
                endpoints.device = endpoints.device.or(found.device);
            }
        }

        if endpoints.token.is_none() {
            return Err(invalid(&format!(
                "{PARAM_PREFIX}token_url or {PARAM_PREFIX}issuer is needed to get a token"
            )));
        }

        Ok(endpoints)
    }

    /// @return The oauth2 client this flow talks through
    fn client(&self, endpoints: &Endpoints) -> AdbcResult<ConfiguredClient> {
        let client_id = ClientId::new(self.required("client_id")?);
        let token_url = TokenUrl::new(endpoints.token.clone().unwrap_or_default())
            .map_err(|error| invalid(&format!("the token URL is not a URL: {error}")))?;

        // The builder carries which endpoints are set in the type, so the
        // two that depend on the flow are given as options rather than
        // branching into four different client types.
        let authorize = endpoints
            .authorize
            .as_ref()
            .map(|url| AuthUrl::new(url.clone()))
            .transpose()
            .map_err(|error| invalid(&format!("the authorize URL is not a URL: {error}")))?;

        let device = endpoints
            .device
            .as_ref()
            .map(|url| DeviceAuthorizationUrl::new(url.clone()))
            .transpose()
            .map_err(|error| invalid(&format!("the device URL is not a URL: {error}")))?;

        let mut client = BasicClient::new(client_id)
            .set_token_uri(token_url)
            .set_auth_uri_option(authorize)
            .set_device_authorization_url_option(device);

        if let Some(secret) = self.param("client_secret") {
            client = client.set_client_secret(ClientSecret::new(secret));
        }

        Ok(client)
    }

    /// The scopes the token should carry, as the provider spells them.
    /// @return One scope per word of the scope parameter
    fn scopes(&self) -> Vec<Scope> {
        self.param("scope")
            .into_iter()
            .flat_map(|scope| {
                scope
                    .split_whitespace()
                    .map(|one| Scope::new(one.to_string()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Machine to machine, which is the flow a driver in a pool wants.
    /// @return What the flow came back with
    fn client_credentials(&self, endpoints: &Endpoints) -> AdbcResult<Taken> {
        let client = self.client(endpoints)?;
        let mut request = client.exchange_client_credentials();

        for scope in self.scopes() {
            request = request.add_scope(scope);
        }

        let http = http()?;
        let response = runtime::block_on(async { request.request_async(&http).await })
            .map_err(|error| invalid(&format!("client credentials were refused: {error}")))?;

        Ok(taken(&response))
    }

    /// A refresh token the caller holds, which is also how a long-running
    /// session renews one it was given.
    /// @return What the flow came back with
    fn refresh(&self, endpoints: &Endpoints) -> AdbcResult<Taken> {
        let client = self.client(endpoints)?;
        let refresh = RefreshToken::new(self.required("refresh_token")?);

        let http = http()?;
        let response = runtime::block_on(async {
            client
                .exchange_refresh_token(&refresh)
                .request_async(&http)
                .await
        })
        .map_err(|error| invalid(&format!("the refresh token was refused: {error}")))?;

        Ok(taken(&response))
    }

    /// The device flow: a code is printed for the user to enter on whatever
    /// device has a browser, and this waits.
    /// @return What the flow came back with
    fn device_code(&self, endpoints: &Endpoints) -> AdbcResult<Taken> {
        let client = self.client(endpoints)?;

        let mut request = client
            .exchange_device_code()
            .map_err(|error| invalid(&format!("the device flow is not set up: {error}")))?;

        for scope in self.scopes() {
            request = request.add_scope(scope);
        }

        let http = http()?;
        let details: oauth2::StandardDeviceAuthorizationResponse =
            runtime::block_on(async { request.request_async(&http).await })
                .map_err(|error| invalid(&format!("the device code was refused: {error}")))?;

        // There is nowhere else to put this: the user has to read it, and a
        // driver has no interface of its own.
        eprintln!(
            "Kinetica sign-in: open {} and enter the code {}",
            details.verification_uri().as_str(),
            details.user_code().secret()
        );

        let response = runtime::block_on(async {
            client
                .exchange_device_access_token(&details)
                .request_async(&http, tokio::time::sleep, None)
                .await
        })
        .map_err(|error| invalid(&format!("the device sign-in did not finish: {error}")))?;

        Ok(taken(&response))
    }

    /// The browser flow, with PKCE over a loopback redirect. The listener is
    /// bound before the URL is built, so the port in the redirect is one this
    /// process actually holds.
    /// @return What the flow came back with
    fn authorization_code(&self, endpoints: &Endpoints) -> AdbcResult<Taken> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .map_err(|error| invalid(&format!("cannot listen for the redirect: {error}")))?;

        let port = listener
            .local_addr()
            .map_err(|error| invalid(&format!("cannot read the redirect port: {error}")))?
            .port();

        let redirect = RedirectUrl::new(format!("http://127.0.0.1:{port}"))
            .map_err(|error| invalid(&format!("the redirect is not a URL: {error}")))?;

        let client = self.client(endpoints)?.set_redirect_uri(redirect);
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();

        let mut authorize = client
            .authorize_url(oauth2::CsrfToken::new_random)
            .map_err(|error| invalid(&format!("the browser flow is not set up: {error}")))?;

        for scope in self.scopes() {
            authorize = authorize.add_scope(scope);
        }

        let (url, state) = authorize.set_pkce_challenge(challenge).url();

        open_browser(url.as_str());
        eprintln!("Kinetica sign-in: {url}");

        let code = wait_for_code(&listener, state.secret())?;

        let http = http()?;
        let response = runtime::block_on(async {
            client
                .exchange_code(oauth2::AuthorizationCode::new(code))
                .set_pkce_verifier(verifier)
                .request_async(&http)
                .await
        })
        .map_err(|error| invalid(&format!("the authorization code was refused: {error}")))?;

        Ok(taken(&response))
    }

    /// A key pair rather than a secret: the driver signs an assertion saying
    /// who it is, and the provider trades it for a token.
    /// @return What the flow came back with
    fn jwt_bearer(&self, endpoints: &Endpoints) -> AdbcResult<Taken> {
        let assertion = self.assertion()?;

        self.post_form(
            endpoints,
            &[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", &assertion),
            ],
        )
    }

    /// @return The signed assertion this client presents as itself
    fn assertion(&self) -> AdbcResult<String> {
        let key_text = match self.param("private_key") {
            Some(key) => key,
            None => {
                let path = self.required("private_key_file")?;

                std::fs::read_to_string(&path).map_err(|error| {
                    invalid(&format!("cannot read the private key '{path}': {error}"))
                })?
            }
        };

        let key = jsonwebtoken::EncodingKey::from_rsa_pem(key_text.as_bytes())
            .or_else(|_| jsonwebtoken::EncodingKey::from_ec_pem(key_text.as_bytes()))
            .map_err(|error| invalid(&format!("the private key cannot be used: {error}")))?;

        let algorithm = match self.param("algorithm").as_deref() {
            Some("ES256") => jsonwebtoken::Algorithm::ES256,
            Some("RS512") => jsonwebtoken::Algorithm::RS512,
            Some(other) if other != "RS256" => {
                return Err(invalid(&format!(
                    "{PARAM_PREFIX}algorithm takes RS256, RS512 or ES256, not '{other}'"
                )))
            }
            _ => jsonwebtoken::Algorithm::RS256,
        };

        let mut header = jsonwebtoken::Header::new(algorithm);
        header.kid = self.param("key_id");

        let client_id = self.required("client_id")?;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let claims = serde_json::json!({
            "iss": client_id,
            // The identity being asserted, which for a service is itself.
            "sub": self.param("subject").unwrap_or(client_id.clone()),
            "aud": self.param("audience").or_else(|| self.param("token_url")),
            "iat": now,
            "exp": now + 300,
            "jti": format!("{now}-{}", std::process::id()),
        });

        jsonwebtoken::encode(&header, &claims, &key)
            .map_err(|error| invalid(&format!("the assertion could not be signed: {error}")))
    }

    /// RFC 8693: a token issued for somewhere else, traded for one that is
    /// good here.
    /// @return What the flow came back with
    fn token_exchange(&self, endpoints: &Endpoints) -> AdbcResult<Taken> {
        let subject_token = self.required("subject_token")?;
        let subject_type = self
            .param("subject_token_type")
            .unwrap_or_else(|| "urn:ietf:params:oauth:token-type:access_token".to_string());

        let mut form = vec![
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange",
            ),
            ("subject_token", subject_token.as_str()),
            ("subject_token_type", subject_type.as_str()),
        ];

        let audience = self.param("audience");
        if let Some(audience) = &audience {
            form.push(("audience", audience.as_str()));
        }

        let scope = self.param("scope");
        if let Some(scope) = &scope {
            form.push(("scope", scope.as_str()));
        }

        self.post_form(endpoints, &form)
    }

    /// The two grants the oauth2 crate has no request type for are ordinary
    /// form posts to the same token endpoint, so they are sent as such rather
    /// than bent into one that nearly fits.
    /// @return What the flow came back with
    fn post_form(&self, endpoints: &Endpoints, form: &[(&str, &str)]) -> AdbcResult<Taken> {
        let url = endpoints.token.clone().unwrap_or_default();
        let client_id = self.required("client_id")?;
        let secret = self.param("client_secret");

        let mut fields: Vec<(String, String)> = form
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();

        fields.push(("client_id".to_string(), client_id));

        if let Some(secret) = secret {
            fields.push(("client_secret".to_string(), secret));
        }

        let http = http()?;

        let body: TokenBody = runtime::block_on(async move {
            let response = http
                .post(&url)
                .form(&fields)
                .send()
                .await
                .map_err(|error| invalid(&format!("the token request failed: {error}")))?;

            let status = response.status();
            let text = response.text().await.map_err(|error| {
                invalid(&format!("the token response could not be read: {error}"))
            })?;

            if !status.is_success() {
                return Err(invalid(&format!(
                    "the token request was refused with {status}: {text}"
                )));
            }

            serde_json::from_str(&text)
                .map_err(|error| invalid(&format!("the token response is not a token: {error}")))
        })?;

        Ok(Taken {
            token: body.access_token,
            expires: body
                .expires_in
                .map(|seconds| SystemTime::now() + Duration::from_secs(seconds)),
            refresh: body.refresh_token,
        })
    }
}

/// The three places a provider takes requests.
struct Endpoints {
    token: Option<String>,
    authorize: Option<String>,
    device: Option<String>,
}

/// The client once it has been told where to go, which is the type the
/// builder leaves behind.
type ConfiguredClient = BasicClient<
    oauth2::EndpointMaybeSet,
    oauth2::EndpointMaybeSet,
    oauth2::EndpointNotSet,
    oauth2::EndpointNotSet,
    oauth2::EndpointSet,
>;

/// What a token endpoint answers with, of which only three fields matter
/// here.
#[derive(serde::Deserialize)]
struct TokenBody {
    access_token: String,
    expires_in: Option<u64>,
    refresh_token: Option<String>,
}

/// @return The option value that names this grant
fn spelling(grant: Grant) -> &'static str {
    match grant {
        Grant::ClientCredentials => "client_credentials",
        Grant::AuthorizationCode => "authorization_code",
        Grant::DeviceCode => "device_code",
        Grant::RefreshToken => "refresh_token",
        Grant::TokenExchange => "token-exchange",
        Grant::JwtBearer => "jwt-bearer",
    }
}

/// A token's own claims, read without checking the signature.
///
/// Only the issuing server can check that, so what comes back serves for
/// saying who this is and for knowing when to stop using the token, and never
/// for deciding what the holder may do. A token that is not a readable JWT
/// yields nothing rather than an error, since an opaque one is perfectly
/// usable and the server is the one that has to understand it.
///
/// @return The claims it carries, or None if it carries none to read
fn claims(token: &str) -> Option<Map<String, Value>> {
    let payload = token.split('.').nth(1)?;

    // base64url and unpadded, per RFC 7515. The standard alphabet is
    // translated rather than refused, because tokens in the wild carry both
    // and the difference is two characters.
    let translated: String = payload
        .chars()
        .map(|character| match character {
            '+' => '-',
            '/' => '_',
            other => other,
        })
        .collect();

    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(translated)
        .ok()?;

    serde_json::from_slice(&decoded).ok()
}

/// @return What the named claim calls the holder, if it is there and says
///         anything
fn name_in(claims: &Map<String, Value>, claim: &str) -> Option<String> {
    match claims.get(claim)? {
        Value::Null => None,
        // Anything but a string is rendered as it was written, since a claim
        // holding a number is still a name of sorts and guessing otherwise
        // would lose it.
        Value::String(name) => Some(name.clone()).filter(|name| !name.is_empty()),
        other => Some(other.to_string()),
    }
}

/// When the token stops being accepted.
///
/// What the endpoint said wins over the token's own claim: it is
/// authoritative, it is relative so no clock difference enters into it, and
/// it is there even for a token with no claims at all.
///
/// @param token The token itself
/// @param said What the endpoint gave, if it gave anything
/// @return When to stop handing this one out, if that is knowable
fn expiry(token: &str, said: Option<SystemTime>) -> Option<SystemTime> {
    said.or_else(|| {
        let seconds = claims(token)?.get("exp")?.as_u64()?;

        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(seconds))
    })
}

/// What a flow came back with.
struct Taken {
    token: String,
    /// When the endpoint said it stops being accepted.
    expires: Option<SystemTime>,
    /// What renews it with nobody present, where the provider issued one.
    refresh: Option<String>,
}

/// @return What the response carries, as this module holds it
fn taken<TR: TokenResponse>(response: &TR) -> Taken {
    Taken {
        token: response.access_token().secret().to_string(),
        expires: response
            .expires_in()
            .map(|lifetime| SystemTime::now() + lifetime),
        refresh: response
            .refresh_token()
            .map(|refresh| refresh.secret().to_string()),
    }
}

/// @return The HTTP client the flows use, which follows no redirects because
///         an OAuth response that redirects is not one to follow
fn http() -> AdbcResult<oauth2::reqwest::Client> {
    oauth2::reqwest::ClientBuilder::new()
        .redirect(oauth2::reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| invalid(&format!("cannot talk to the identity provider: {error}")))
}

/// Reads the provider's own description of itself.
/// @return The endpoints it publishes
fn discover(issuer: &str) -> AdbcResult<Endpoints> {
    #[derive(serde::Deserialize)]
    struct Document {
        token_endpoint: Option<String>,
        authorization_endpoint: Option<String>,
        device_authorization_endpoint: Option<String>,
    }

    let url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );

    let http = http()?;

    let document: Document = runtime::block_on(async move {
        let response = http
            .get(&url)
            .send()
            .await
            .map_err(|error| invalid(&format!("cannot reach {url}: {error}")))?;

        let text = response.text().await.map_err(|error| {
            invalid(&format!("the document at {url} could not be read: {error}"))
        })?;

        serde_json::from_str(&text)
            .map_err(|error| invalid(&format!("{url} is not an OpenID Connect document: {error}")))
    })?;

    Ok(Endpoints {
        token: document.token_endpoint,
        authorize: document.authorization_endpoint,
        device: document.device_authorization_endpoint,
    })
}

/// Waits for the browser to come back, and checks that it came back with the
/// state this process sent: a code arriving under someone else's state is not
/// this sign-in.
/// @return The authorization code
fn wait_for_code(listener: &std::net::TcpListener, state: &str) -> AdbcResult<String> {
    use std::io::{BufRead, BufReader, Write};

    listener
        .set_nonblocking(false)
        .map_err(|error| invalid(&format!("cannot wait for the redirect: {error}")))?;

    let deadline = SystemTime::now() + REDIRECT_WAIT;

    loop {
        if SystemTime::now() > deadline {
            return Err(invalid("the browser did not come back in time"));
        }

        let (mut stream, _) = listener
            .accept()
            .map_err(|error| invalid(&format!("the redirect was not received: {error}")))?;

        let mut request = String::new();
        BufReader::new(&stream)
            .read_line(&mut request)
            .map_err(|error| invalid(&format!("the redirect could not be read: {error}")))?;

        // "GET /?code=...&state=... HTTP/1.1"
        let target = request.split_whitespace().nth(1).unwrap_or_default();
        let query = target.split_once('?').map(|(_, query)| query).unwrap_or("");

        let mut code = None;
        let mut returned_state = None;

        for pair in query.split('&') {
            match pair.split_once('=') {
                Some(("code", value)) => code = Some(unescape(value)),
                Some(("state", value)) => returned_state = Some(unescape(value)),
                _ => {}
            }
        }

        let answer = match (&code, &returned_state) {
            (Some(_), Some(returned)) if returned == state => {
                "Signed in. You can close this window."
            }
            _ => "This sign-in could not be completed.",
        };

        let _ = write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{answer}",
            answer.len()
        );

        match (code, returned_state) {
            (Some(code), Some(returned)) if returned == state => return Ok(code),
            (Some(_), _) => {
                return Err(invalid(
                    "the sign-in came back under a state this driver did not send",
                ))
            }
            // Anything else is a browser asking for a favicon; keep waiting.
            _ => continue,
        }
    }
}

/// @return The value with its percent escapes and pluses read back
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

/// Asks the desktop to open the sign-in page. A host with no desktop simply
/// does nothing, which is why the URL is printed as well.
fn open_browser(url: &str) {
    let (command, first) = match std::env::consts::OS {
        "macos" => ("open", None),
        "windows" => ("cmd", Some("/C start")),
        _ => ("xdg-open", None),
    };

    let mut process = std::process::Command::new(command);

    if let Some(first) = first {
        process.args(first.split(' '));
    }

    let _ = process
        .arg(url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

// --------------------------------------------------------------------------
// The held tokens

/// A token and when it stops being worth presenting.
struct Held {
    token: String,
    expires: Option<SystemTime>,
    /// When it was obtained, which is what makes its lifetime knowable: an
    /// expiry alone says when a token dies, not how long it was good for.
    acquired: SystemTime,
    /// What renews it with nobody present, where the provider issued one.
    refresh: Option<String>,
}

/// What the store had to say when asked.
enum Holding {
    /// Good to send as it is.
    Fresh(String),
    /// Gone off, but renewable without anyone being present.
    Stale(String),
    /// Nothing usable, so the flow has to run.
    Nothing,
}

/// @return The tokens this process has acquired
fn tokens() -> &'static Mutex<BTreeMap<String, Held>> {
    static TOKENS: OnceLock<Mutex<BTreeMap<String, Held>>> = OnceLock::new();

    TOKENS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// A look that takes nothing, for the common case: a token already held
/// should never wait behind anyone else's sign-in.
/// @return The token held under that key, if it is still good to send
fn fresh(key: &str) -> Option<String> {
    let tokens = tokens().lock().unwrap();

    tokens
        .get(key)
        .filter(|held| !expired(held))
        .map(|held| held.token.clone())
}

/// @return What is held under that key: the token if it is still good, else
///         whatever would renew it
fn held(key: &str) -> Holding {
    let mut tokens = tokens().lock().unwrap();

    match tokens.get(key) {
        Some(held) if !expired(held) => Holding::Fresh(held.token.clone()),
        Some(held) => {
            let renews = held.refresh.clone();

            // Gone off. Dropped here so the next caller does not look at it
            // again, and so a token that will never be asked for again is not
            // kept for the life of the process. What renews it is handed back
            // rather than dropped with it.
            tokens.remove(key);

            match renews {
                Some(refresh) => Holding::Stale(refresh),
                None => Holding::Nothing,
            }
        }
        None => Holding::Nothing,
    }
}

/// @return Whether the token is too close to its expiry to send
fn expired(held: &Held) -> bool {
    // Nothing said when it stops being accepted, so nothing says it has.
    held.expires
        .is_some_and(|expires| SystemTime::now() + margin(held) >= expires)
}

/// How early to retire a token: a fifth of its lifetime, capped.
///
/// Proportional rather than flat because the two ends want different things.
/// A token good for an hour should be replaced minutes before it dies, so
/// that a request taking its time still finishes; a token good for one minute
/// retired five minutes early would never be usable at all.
///
/// @return How long before its expiry to stop handing it out
fn margin(held: &Held) -> Duration {
    let Some(expires) = held.expires else {
        return Duration::ZERO;
    };

    // It arrived at or past its expiry, so there is nothing to give back.
    let Ok(lifetime) = expires.duration_since(held.acquired) else {
        return Duration::ZERO;
    };

    (lifetime / EXPIRY_FRACTION).min(MAX_EXPIRY_MARGIN)
}

/// Per-identity gates, so that concurrent misses on one identity sign in once
/// rather than racing. Without them a pool opening ten connections runs ten
/// flows -- and an interactive flow opens ten browser windows.
/// @return The gate for that identity, made if this is the first to ask
fn gate(key: &str) -> Arc<Mutex<()>> {
    static GATES: OnceLock<Mutex<BTreeMap<String, Arc<Mutex<()>>>>> = OnceLock::new();

    GATES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap()
        .entry(key.to_string())
        .or_default()
        .clone()
}

/// Holds the token for whoever asks for the same one next.
fn keep(key: &str, taken: &Taken) {
    tokens().lock().unwrap().insert(
        key.to_string(),
        Held {
            token: taken.token.clone(),
            // The one place what the endpoint said is weighed against what
            // the token itself claims.
            expires: expiry(&taken.token, taken.expires),
            acquired: SystemTime::now(),
            refresh: taken.refresh.clone(),
        },
    );
}

// --------------------------------------------------------------------------
// Reading the options

/// @return The mechanism the text names
fn method(value: &str) -> AdbcResult<Method> {
    match value {
        _ if value.eq_ignore_ascii_case("password") => Ok(Method::Password),
        _ if value.eq_ignore_ascii_case("oauth") => Ok(Method::OAuth),
        other => Err(invalid(&format!(
            "{} takes password or oauth, not '{other}'",
            options::AUTH_METHOD
        ))),
    }
}

/// @return The flow the text names, with the RFC's URN accepted for the two
///         that have one
fn grant(value: &str) -> AdbcResult<Grant> {
    let named = value.trim();

    let found = match named {
        _ if named.eq_ignore_ascii_case("client_credentials") => Grant::ClientCredentials,
        _ if named.eq_ignore_ascii_case("authorization_code") => Grant::AuthorizationCode,
        _ if named.eq_ignore_ascii_case("device_code")
            || named == "urn:ietf:params:oauth:grant-type:device_code" =>
        {
            Grant::DeviceCode
        }
        _ if named.eq_ignore_ascii_case("refresh_token") => Grant::RefreshToken,
        _ if named.eq_ignore_ascii_case("token-exchange")
            || named == "urn:ietf:params:oauth:grant-type:token-exchange" =>
        {
            Grant::TokenExchange
        }
        _ if named.eq_ignore_ascii_case("jwt-bearer")
            || named == "urn:ietf:params:oauth:grant-type:jwt-bearer" =>
        {
            Grant::JwtBearer
        }
        other => {
            return Err(invalid(&format!(
                "{} takes client_credentials, authorization_code, device_code, refresh_token, \
                 token-exchange or jwt-bearer, not '{other}'",
                options::OAUTH_GRANT
            )))
        }
    };

    Ok(found)
}

/// @return Where tokens should be kept
fn cache(value: &str) -> AdbcResult<Cache> {
    match value {
        _ if value.eq_ignore_ascii_case("memory") => Ok(Cache::Memory),
        _ if value.eq_ignore_ascii_case("none") => Ok(Cache::None),
        other => Err(invalid(&format!(
            "{} takes memory or none, not '{other}'",
            options::OAUTH_TOKEN_CACHE
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// @param claims The payload, written as JSON
    /// @return A token carrying them, signed with nothing, since nothing here
    ///         checks a signature and the server is what would
    fn jwt(claims: &str) -> String {
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims);

        format!("header.{payload}.signature")
    }

    /// @return A connection holding that token and nothing else
    fn holding(token: &str) -> Auth {
        let mut auth = Auth::default();
        auth.set(options::OAUTH_TOKEN, token).unwrap();

        auth
    }

    #[test]
    fn a_token_says_who_holds_it() {
        let auth = holding(&jwt(r#"{"sub":"alice@example.com","exp":4102444800}"#));

        assert_eq!(
            auth.bearer().unwrap().unwrap().user.as_deref(),
            Some("alice@example.com")
        );
    }

    #[test]
    fn a_caller_can_say_which_claim_the_name_is_in() {
        let mut auth = holding(&jwt(r#"{"sub":"0a1b2c","upn":"alice@example.com"}"#));
        auth.set(options::TOKEN_NAME_CLAIM, "upn").unwrap();

        assert_eq!(
            auth.bearer().unwrap().unwrap().user.as_deref(),
            Some("alice@example.com")
        );
        assert_eq!(
            auth.get(options::TOKEN_NAME_CLAIM).as_deref(),
            Some("upn"),
            "what will be read is what is read back"
        );
    }

    #[test]
    fn a_claim_that_is_not_there_names_nobody() {
        let auth = holding(&jwt(r#"{"aud":"kinetica"}"#));

        assert_eq!(auth.bearer().unwrap().unwrap().user, None);
    }

    #[test]
    fn a_token_that_is_not_a_jwt_is_still_presented() {
        let auth = holding("an-opaque-token");
        let presented = auth.bearer().unwrap().unwrap();

        assert_eq!(presented.token, "an-opaque-token");
        assert_eq!(presented.user, None);
    }

    #[test]
    fn a_provider_names_the_claim_its_subject_is_not_in() {
        let mut auth = Auth::default();
        auth.set(options::AUTH_METHOD, "oauth").unwrap();
        auth.set(options::AUTH_PROVIDER, "entra").unwrap();

        assert_eq!(
            auth.get(options::TOKEN_NAME_CLAIM).as_deref(),
            Some("preferred_username"),
            "Entra's sub is a different value per application, never a name"
        );

        // But a caller who named one meant it.
        auth.set(options::TOKEN_NAME_CLAIM, "oid").unwrap();

        assert_eq!(auth.get(options::TOKEN_NAME_CLAIM).as_deref(), Some("oid"));
    }

    #[test]
    fn a_vendor_rewrites_the_parameters_of_the_flow_that_runs() {
        let mut auth = Auth::default();
        auth.set(options::AUTH_METHOD, "oauth").unwrap();
        auth.set(options::OAUTH_GRANT, "client_credentials")
            .unwrap();
        auth.set("kinetica.oauth.tenant_id", "a-tenant").unwrap();
        auth.set("kinetica.oauth.application_id_uri", "api://kinetica")
            .unwrap();

        let resolved = auth.translated(Provider::Entra.vendor(), Grant::ClientCredentials);

        assert_eq!(
            resolved.param("issuer").as_deref(),
            Some("https://login.microsoftonline.com/a-tenant/v2.0")
        );
        assert_eq!(
            resolved.param("scope").as_deref(),
            Some("api://kinetica/.default")
        );
        assert_eq!(resolved.claim(), "preferred_username");
    }

    #[test]
    fn translating_twice_changes_nothing() {
        let mut auth = Auth::default();
        auth.set("kinetica.oauth.okta_domain", "example.okta.com")
            .unwrap();

        let once = auth.translated(Provider::Okta.vendor(), Grant::AuthorizationCode);
        let twice = once.translated(Provider::Okta.vendor(), Grant::AuthorizationCode);

        assert_eq!(once.param("issuer"), twice.param("issuer"));
        assert_eq!(once.claim(), twice.claim());
    }

    #[test]
    fn two_vendors_derivations_are_held_apart() {
        let mut first = Auth::default();
        first.set(options::AUTH_METHOD, "oauth").unwrap();
        first
            .set(options::OAUTH_GRANT, "client_credentials")
            .unwrap();
        first.set("kinetica.oauth.client_id", "shared").unwrap();
        first.set("kinetica.oauth.tenant_id", "one").unwrap();

        let mut second = first.clone();
        second.set("kinetica.oauth.tenant_id", "two").unwrap();

        let key = |auth: &Auth| {
            auth.translated(Provider::Entra.vendor(), Grant::ClientCredentials)
                .cache_key(Some(Grant::ClientCredentials))
        };

        assert_ne!(
            key(&first),
            key(&second),
            "the same client in two tenants is two identities"
        );
    }

    #[test]
    fn a_vendor_is_implied_by_its_own_parameters_but_only_where_a_flow_runs() {
        let mut auth = Auth::default();
        auth.set(options::AUTH_METHOD, "oauth").unwrap();
        auth.set("kinetica.oauth.okta_domain", "example.okta.com")
            .unwrap();

        assert_eq!(
            auth.vendor().map(Vendor::name),
            None,
            "nothing is being prepared, so there is nothing to imply"
        );

        auth.set(options::OAUTH_GRANT, "authorization_code")
            .unwrap();

        assert_eq!(auth.vendor().map(Vendor::name), Some("okta"));
        assert_eq!(
            auth.claim(),
            "preferred_username",
            "an implied vendor names its claim as a named one would"
        );
    }

    #[test]
    fn a_provider_is_not_used_with_a_password() {
        let mut auth = Auth::default();
        auth.set(options::AUTH_PROVIDER, "okta").unwrap();

        let refused = auth.bearer().unwrap_err().message;

        assert!(refused.contains(options::AUTH_PROVIDER), "{refused}");
        assert!(refused.contains("password"), "{refused}");
    }

    #[test]
    fn a_vendor_needs_a_flow_to_rewrite_the_parameters_of() {
        let mut auth = Auth::default();
        auth.set(options::AUTH_METHOD, "oauth").unwrap();
        auth.set(options::AUTH_PROVIDER, "entra").unwrap();

        let refused = auth.bearer().unwrap_err().message;

        assert!(refused.contains("entra"), "{refused}");
        assert!(refused.contains(options::OAUTH_GRANT), "{refused}");
    }

    #[test]
    fn a_command_and_a_grant_would_both_produce_the_token() {
        let mut auth = Auth::default();
        auth.set(options::AUTH_METHOD, "oauth").unwrap();
        auth.set(options::AUTH_PROVIDER, "command").unwrap();
        auth.set(options::OAUTH_GRANT, "client_credentials")
            .unwrap();
        auth.set("kinetica.oauth.command", "echo t").unwrap();

        let refused = auth.bearer().unwrap_err().message;

        assert!(refused.contains("client_credentials"), "{refused}");
    }

    #[test]
    fn a_command_is_run_and_what_it_prints_is_the_token() {
        let token = jwt(r#"{"sub":"service@example.com"}"#);

        let mut auth = Auth::default();
        auth.set(options::AUTH_METHOD, "oauth").unwrap();
        auth.set(options::AUTH_PROVIDER, "command").unwrap();
        auth.set(options::OAUTH_TOKEN_CACHE, "none").unwrap();
        auth.set("kinetica.oauth.command", &format!("echo {token}"))
            .unwrap();

        let presented = auth.bearer().unwrap().expect("the command produced one");

        assert_eq!(presented.token, token);
        assert_eq!(presented.user.as_deref(), Some("service@example.com"));
    }

    #[test]
    fn a_command_nobody_named_is_refused_before_a_shell_is_started() {
        let mut auth = Auth::default();
        auth.set(options::AUTH_METHOD, "oauth").unwrap();
        auth.set(options::AUTH_PROVIDER, "command").unwrap();

        let refused = auth.bearer().unwrap_err().message;

        assert!(refused.contains("command"), "{refused}");
    }

    #[test]
    fn a_provider_is_read_back_as_the_option_names_it() {
        let mut auth = Auth::default();
        auth.set(options::AUTH_PROVIDER, "ENTRA").unwrap();

        assert_eq!(auth.get(options::AUTH_PROVIDER).as_deref(), Some("entra"));
        assert_eq!(
            Auth::default().get(options::AUTH_PROVIDER),
            None,
            "nobody named one"
        );
    }

    #[test]
    fn what_the_endpoint_said_outlives_what_the_token_claims() {
        // exp is in 2100, so a token claiming it would be held far longer
        // than the ten seconds the endpoint actually gave.
        let token = jwt(r#"{"sub":"alice","exp":4102444800}"#);
        let said = SystemTime::now() + Duration::from_secs(10);

        assert_eq!(expiry(&token, Some(said)), Some(said));

        let claimed = expiry(&token, None).expect("the token says when");

        assert!(
            claimed > SystemTime::now() + Duration::from_secs(60),
            "{claimed:?}"
        );
        assert_eq!(
            expiry("an-opaque-token", None),
            None,
            "nothing says when this one stops working"
        );
    }

    #[test]
    fn a_password_connection_has_no_token_to_present() {
        assert!(Auth::default().bearer().unwrap().is_none());
    }

    #[test]
    fn a_token_the_caller_holds_is_the_one_presented() {
        let mut auth = Auth::default();
        auth.set(options::OAUTH_TOKEN, "handed-over").unwrap();

        let presented = auth.bearer().unwrap().expect("a token was handed over");

        assert_eq!(presented.token, "handed-over");
        assert_eq!(
            presented.user, None,
            "an opaque token says nothing about who holds it"
        );
    }

    #[test]
    fn choosing_oauth_without_saying_how_is_refused_with_what_is_missing() {
        let mut auth = Auth::default();
        auth.set(options::AUTH_METHOD, "oauth").unwrap();

        let error = auth.bearer().unwrap_err();

        assert!(
            error.message.contains(options::OAUTH_GRANT),
            "{}",
            error.message
        );
    }

    #[test]
    fn a_grant_is_read_back_as_the_option_names_it() {
        let mut auth = Auth::default();
        auth.set(options::OAUTH_GRANT, "CLIENT_CREDENTIALS")
            .unwrap();

        assert_eq!(
            auth.get(options::OAUTH_GRANT).unwrap(),
            "client_credentials"
        );
    }

    #[test]
    fn the_urn_form_of_a_grant_is_accepted() {
        let mut auth = Auth::default();
        auth.set(
            options::OAUTH_GRANT,
            "urn:ietf:params:oauth:grant-type:jwt-bearer",
        )
        .unwrap();

        assert_eq!(auth.get(options::OAUTH_GRANT).unwrap(), "jwt-bearer");
    }

    #[test]
    fn a_grant_nobody_has_heard_of_is_refused() {
        let mut auth = Auth::default();

        let error = auth.set(options::OAUTH_GRANT, "implicit").unwrap_err();

        assert!(
            error.message.contains("client_credentials"),
            "{}",
            error.message
        );
    }

    #[test]
    fn a_flow_parameter_is_kept_under_its_own_name() {
        let mut auth = Auth::default();

        assert!(auth.set("kinetica.oauth.client_id", "a-client").unwrap());
        assert_eq!(auth.get("kinetica.oauth.client_id").unwrap(), "a-client");
        assert_eq!(auth.param("client_id").unwrap(), "a-client");
    }

    #[test]
    fn a_name_that_is_not_one_of_these_is_left_to_the_caller() {
        let mut auth = Auth::default();

        assert!(!auth.set("kinetica.query.batch_size", "10").unwrap());
        assert!(auth.get("kinetica.query.batch_size").is_none());
    }

    #[test]
    fn a_flow_with_no_endpoint_says_which_one_it_wanted() {
        let mut auth = Auth::default();
        auth.set(options::OAUTH_GRANT, "client_credentials")
            .unwrap();
        auth.set("kinetica.oauth.client_id", "a-client").unwrap();

        let error = auth.bearer().unwrap_err();

        assert!(error.message.contains("token_url"), "{}", error.message);
    }

    #[test]
    fn a_flow_with_no_client_says_so_rather_than_sending_one_without() {
        let mut auth = Auth::default();
        auth.set(options::OAUTH_GRANT, "client_credentials")
            .unwrap();
        auth.set("kinetica.oauth.token_url", "https://id.example/token")
            .unwrap();

        let error = auth.bearer().unwrap_err();

        assert!(error.message.contains("client_id"), "{}", error.message);
    }

    #[test]
    fn two_identities_do_not_share_one_held_token() {
        let mut first = Auth::default();
        first
            .set(options::OAUTH_GRANT, "client_credentials")
            .unwrap();
        first.set("kinetica.oauth.client_id", "one").unwrap();

        let mut second = first.clone();
        second.set("kinetica.oauth.client_id", "two").unwrap();

        assert_ne!(
            first.cache_key(Some(Grant::ClientCredentials)),
            second.cache_key(Some(Grant::ClientCredentials))
        );
    }

    /// @return What is held, flattened to something a test can compare
    fn token_held(key: &str) -> Option<String> {
        match held(key) {
            Holding::Fresh(token) => Some(token),
            Holding::Stale(_) | Holding::Nothing => None,
        }
    }

    /// @param token What it holds
    /// @param expires When it stops being good, if ever
    /// @param refresh What would renew it, if anything
    /// @return A flow's result, for putting in the store by hand
    fn came_back(token: &str, expires: Option<SystemTime>, refresh: Option<&str>) -> Taken {
        Taken {
            token: token.to_string(),
            expires,
            refresh: refresh.map(str::to_string),
        }
    }

    #[test]
    fn a_token_past_its_expiry_is_not_handed_out_again() {
        let key = "a key of this test's own";

        keep(
            key,
            &came_back(
                "stale",
                Some(SystemTime::now() - Duration::from_secs(1)),
                None,
            ),
        );
        assert_eq!(token_held(key), None);

        keep(
            key,
            &came_back(
                "fresh",
                Some(SystemTime::now() + Duration::from_secs(600)),
                None,
            ),
        );
        assert_eq!(token_held(key).as_deref(), Some("fresh"));
    }

    #[test]
    fn a_token_with_no_expiry_is_held_until_the_process_ends() {
        let key = "another key of this test's own";

        keep(key, &came_back("forever", None, None));

        assert_eq!(token_held(key).as_deref(), Some("forever"));
    }

    /// @param lifetime How long it was good for
    /// @param left How much of that is still to come
    /// @return A held token of that shape
    fn lived(lifetime: Duration, left: Duration) -> Held {
        let now = SystemTime::now();

        Held {
            token: "a token".to_string(),
            expires: Some(now + left),
            acquired: now + left - lifetime,
            refresh: None,
        }
    }

    #[test]
    fn how_early_a_token_is_retired_follows_its_own_lifetime() {
        let base = SystemTime::now();
        let shaped = |lifetime: Duration| Held {
            token: "a token".to_string(),
            expires: Some(base + lifetime),
            acquired: base,
            refresh: None,
        };

        assert_eq!(
            margin(&shaped(Duration::from_secs(60))),
            Duration::from_secs(12),
            "a fifth of a short life"
        );
        assert_eq!(
            margin(&shaped(Duration::from_secs(3600))),
            MAX_EXPIRY_MARGIN,
            "capped once a fifth of it would be more than five minutes"
        );
        assert_eq!(
            margin(&Held {
                expires: None,
                ..shaped(Duration::ZERO)
            }),
            Duration::ZERO,
            "nothing says when this one stops, so nothing is given back"
        );
    }

    #[test]
    fn a_long_lived_token_is_retired_before_it_actually_dies() {
        let hour = Duration::from_secs(3600);

        assert!(
            expired(&lived(hour, Duration::from_secs(240))),
            "four minutes left of an hour is inside the margin, and a request \
             can easily outlast it"
        );
        assert!(
            !expired(&lived(hour, Duration::from_secs(600))),
            "ten minutes left is still worth handing out"
        );
    }

    #[test]
    fn a_short_lived_token_is_usable_the_moment_it_arrives() {
        let brief = Duration::from_secs(300);

        assert!(
            !expired(&lived(brief, brief)),
            "a flat five-minute margin would retire a five-minute token \
             before anyone could use it"
        );
        assert!(
            expired(&lived(brief, Duration::from_secs(30))),
            "but it is still retired a fifth of the way out"
        );
    }

    #[test]
    fn a_held_token_is_looked_at_without_being_taken() {
        let key = "a key for the peek";

        keep(
            key,
            &came_back(
                "peeked at",
                Some(SystemTime::now() + Duration::from_secs(600)),
                None,
            ),
        );

        assert_eq!(fresh(key).as_deref(), Some("peeked at"));
        assert_eq!(
            fresh(key).as_deref(),
            Some("peeked at"),
            "looking does not consume it, so the next caller sees it too"
        );
    }

    #[test]
    fn what_would_renew_an_expired_token_outlives_the_token() {
        let key = "a key for the renewal of this test's own";

        keep(
            key,
            &came_back(
                "stale",
                Some(SystemTime::now() - Duration::from_secs(1)),
                Some("a-refresh-token"),
            ),
        );

        match held(key) {
            Holding::Stale(refresh) => assert_eq!(refresh, "a-refresh-token"),
            Holding::Fresh(_) => panic!("it has expired"),
            Holding::Nothing => panic!("what renews it should have outlived it"),
        }

        assert!(
            matches!(held(key), Holding::Nothing),
            "and it is handed out once, not kept to be tried again"
        );
    }

    #[test]
    fn an_expired_token_with_nothing_to_renew_it_is_simply_gone() {
        let key = "a key for the unrenewable";

        keep(
            key,
            &came_back(
                "stale",
                Some(SystemTime::now() - Duration::from_secs(1)),
                None,
            ),
        );

        assert!(matches!(held(key), Holding::Nothing));
    }

    #[test]
    fn a_redirect_is_read_back_out_of_the_query() {
        assert_eq!(unescape("a%2Bb%20c"), "a+b c");
        assert_eq!(unescape("plain"), "plain");
    }
}
