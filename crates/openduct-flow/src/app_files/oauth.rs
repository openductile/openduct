// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! OAuth 2.0 for apps whose sign-in says `oauth2:`. Two ways to a token:
//!
//! - **authorization code**: a person signs in at the vendor (Connect, in the studio). The install's own
//!   registration with the vendor - its client id and secret, which an admin gives in Settings - is used; OpenDuct
//!   takes no part beyond it. The code that comes back is traded for tokens, kept in the credential, and the access
//!   token is refreshed as it runs out.
//! - **client credentials**: the credential's own client id and secret are traded for a token whenever one is needed.
//!
//! PKCE (S256) guards the code. The token is kept in the credential's `access_token`, `refresh_token` and
//! `expires_at` (seconds since 1970).

use std::collections::BTreeMap;
use std::time::Duration;

use base64::Engine as _;
use openduct_expr::{Env, Value};
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::Value as Json;

use super::definition::{Grant, OAuth2, SignIn, host_matches, secure_address};
use crate::apps::{Credential, MASK};
use crate::run::HttpRequest;

/// How long before a token runs out it is refreshed.
const EARLY: u64 = 60;
const TIMEOUT: Duration = Duration::from_secs(30);

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn b64url(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// A PKCE pair: the verifier (kept by OpenDuct) and its challenge (sent to the vendor).
pub fn pkce() -> Result<(String, String), String> {
    let mut bytes = [0u8; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| "the system gave no random bytes".to_owned())?;
    let verifier = b64url(&bytes);
    let challenge = b64url(ring::digest::digest(&ring::digest::SHA256, verifier.as_bytes()).as_ref());
    Ok((verifier, challenge))
}

/// Where a person is sent to sign in at the vendor, with the install's `client_id`.
pub fn authorize_url(
    oauth: &OAuth2,
    client_id: &str,
    redirect_uri: &str,
    state: &str,
    challenge: &str,
) -> Result<String, String> {
    let mut url = reqwest::Url::parse(&oauth.authorize).map_err(|e| e.to_string())?;
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("response_type", "code")
            .append_pair("client_id", client_id)
            .append_pair("redirect_uri", redirect_uri)
            .append_pair("state", state)
            .append_pair("code_challenge", challenge)
            .append_pair("code_challenge_method", "S256");
        if !oauth.scopes.is_empty() {
            q.append_pair("scope", &oauth.scopes.join(" "));
        }
        for (k, v) in &oauth.extra {
            q.append_pair(k, v);
        }
    }
    Ok(url.to_string())
}

/// Where a token is asked for: the sign-in's token address, filled in from the credential when it names its fields -
/// and then only one of the app's hosts.
fn token_url(oauth: &OAuth2, credential: &Credential) -> Result<String, String> {
    let Some((template, hosts)) = &oauth.token_named else {
        return Ok(oauth.token.clone());
    };
    let fields: serde_json::Map<String, Json> = credential
        .fields
        .iter()
        .map(|(k, v)| (k.clone(), Json::String(v.clone())))
        .collect();
    let names = [("credential".to_owned(), Value::from_json(&Json::Object(fields)))];
    let null = Value::Null;
    let url = template.render(&Env {
        value: &null,
        message: &null,
        names: &names,
    })?;
    let parsed = reqwest::Url::parse(&url)
        .map_err(|_| "its token address is not an address: check its fields".to_owned())?;
    let host = parsed.host_str().unwrap_or_default();
    if !secure_address(&parsed) || !hosts.iter().any(|h| host_matches(h, host)) {
        return Err(format!(
            "its token address names {host}, which is not one of the app's hosts: check its fields"
        ));
    }
    Ok(url)
}

fn token_request(
    oauth: &OAuth2,
    url: String,
    client_id: &str,
    client_secret: &str,
    mut form: Vec<(&str, String)>,
) -> HttpRequest {
    let mut headers = vec![("accept".to_owned(), "application/json".to_owned())];
    if oauth.client_basic {
        let pair = format!("{client_id}:{client_secret}");
        headers.push((
            "authorization".to_owned(),
            format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(pair)),
        ));
    } else {
        form.push(("client_id", client_id.to_owned()));
        form.push(("client_secret", client_secret.to_owned()));
    }
    let body: Vec<String> = form
        .iter()
        .map(|(k, v)| format!("{}={}", encoded(k), encoded(v)))
        .collect();
    HttpRequest {
        // Getting a token changes nothing at the vendor: a preview may ask for one.
        read: true,
        method: "POST".to_owned(),
        url,
        headers,
        query: Vec::new(),
        body: Some((
            "application/x-www-form-urlencoded".to_owned(),
            body.join("&").into_bytes(),
        )),
        timeout: TIMEOUT,
    }
}

/// `text` as a form writes it: letters, digits and `-._~` as they are, everything else as %XX.
fn encoded(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for b in text.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The request that trades a code for tokens, with the install's registration.
pub fn exchange(
    oauth: &OAuth2,
    client_id: &str,
    client_secret: &str,
    code: &str,
    redirect_uri: &str,
    verifier: &str,
) -> HttpRequest {
    token_request(
        oauth,
        oauth.token.clone(),
        client_id,
        client_secret,
        vec![
            ("grant_type", "authorization_code".to_owned()),
            ("code", code.to_owned()),
            ("redirect_uri", redirect_uri.to_owned()),
            ("code_verifier", verifier.to_owned()),
        ],
    )
}

/// Whether a credential of `sign_in` wants a token now: it has none, or it runs out within a minute. Never in a
/// test run, which reads no secret.
pub fn wanted(sign_in: &SignIn, credential: &Credential, force: bool) -> bool {
    if sign_in.oauth2.is_none() || credential.kind == "test" || credential.fields.values().any(|v| v == MASK)
    {
        return false;
    }
    if force {
        return true;
    }
    let token = credential.fields.get("access_token").filter(|t| !t.is_empty());
    let ends = credential
        .fields
        .get("expires_at")
        .and_then(|e| e.parse::<u64>().ok());
    token.is_none() || ends.is_some_and(|e| e <= now() + EARLY)
}

/// The request for a fresh token: a refresh (authorization code, with the install's registration), or the
/// credential's own client id and secret traded (client credentials). None when there is no way to one.
pub fn refresh(
    sign_in: &SignIn,
    credential: &Credential,
    registration: Option<&Credential>,
) -> Result<HttpRequest, String> {
    let oauth = sign_in.oauth2.as_ref().ok_or("not an OAuth sign-in")?;
    match oauth.grant {
        Grant::ClientCredentials => {
            let mut form = vec![("grant_type", "client_credentials".to_owned())];
            if !oauth.scopes.is_empty() {
                form.push(("scope", oauth.scopes.join(" ")));
            }
            Ok(token_request(
                oauth,
                token_url(oauth, credential)?,
                &credential.field("client_id"),
                &credential.field("client_secret"),
                form,
            ))
        }
        Grant::AuthorizationCode => {
            let token = credential
                .fields
                .get("refresh_token")
                .filter(|t| !t.is_empty())
                .ok_or("its sign-in has run out: connect it again under Credentials")?;
            let reg = registration.ok_or(
                "this install has no registration with the vendor: an admin adds its client id and secret under Settings",
            )?;
            Ok(token_request(
                oauth,
                oauth.token.clone(),
                &reg.field("client_id"),
                &reg.field("client_secret"),
                vec![
                    ("grant_type", "refresh_token".to_owned()),
                    ("refresh_token", token.clone()),
                ],
            ))
        }
    }
}

/// The token fields a token answer (`{status, headers, body}`) gives; a refresh token not sent again is kept.
pub fn tokens(answer: &Value, before: &Credential) -> Result<BTreeMap<String, String>, String> {
    let json: Json = serde_json::from_str(&answer.to_json()).unwrap_or(Json::Null);
    let status = json.get("status").and_then(Json::as_u64).unwrap_or(0);
    let body = json.get("body").cloned().unwrap_or(Json::Null);
    if status >= 400 || body.get("access_token").is_none() {
        // A page rather than a token: the vendor sent it to a sign-in or account page (an account closed, a trial run
        // out, a wrong address).
        let page = "it answered with a web page, not a token: is the account still open, and its name right?";
        let why = body
            .get("error_description")
            .or_else(|| body.get("error"))
            .and_then(Json::as_str)
            .unwrap_or(if body.is_string() {
                page
            } else {
                "no token in its answer"
            });
        return Err(format!("the vendor gave no token ({status}): {why}"));
    }
    let text = |k: &str| body.get(k).and_then(Json::as_str).map(str::to_owned);
    let mut out = BTreeMap::new();
    out.insert(
        "access_token".to_owned(),
        text("access_token").unwrap_or_default(),
    );
    if let Some(r) = text("refresh_token").or_else(|| before.fields.get("refresh_token").cloned()) {
        out.insert("refresh_token".to_owned(), r);
    }
    let lasts = body
        .get("expires_in")
        .and_then(|e| e.as_u64().or_else(|| e.as_str().and_then(|s| s.parse().ok())));
    if let Some(secs) = lasts {
        out.insert("expires_at".to_owned(), (now() + secs).to_string());
    }
    Ok(out)
}

/// The request made once connected (`after_connect`), with the new access token.
pub fn after_connect_request(oauth: &OAuth2, access_token: &str) -> Option<HttpRequest> {
    let (url, _) = oauth.after_connect.as_ref()?;
    Some(HttpRequest {
        read: true,
        method: "GET".to_owned(),
        url: url.clone(),
        headers: vec![
            ("authorization".to_owned(), format!("Bearer {access_token}")),
            ("accept".to_owned(), "application/json".to_owned()),
        ],
        query: Vec::new(),
        body: None,
        timeout: TIMEOUT,
    })
}

/// What `after_connect` keeps of its answer (`{status, headers, body}`).
pub fn after_connect_fields(oauth: &OAuth2, answer: &Value) -> Result<BTreeMap<String, String>, String> {
    let Some((_, keep)) = &oauth.after_connect else {
        return Ok(BTreeMap::new());
    };
    let json: Json = serde_json::from_str(&answer.to_json()).unwrap_or(Json::Null);
    let status = json.get("status").and_then(Json::as_u64).unwrap_or(0);
    if status >= 400 {
        return Err(format!("the vendor answered {status} once connected"));
    }
    let body = Value::from_json(&json.get("body").cloned().unwrap_or(Json::Null));
    let mut out = BTreeMap::new();
    for (field, expression) in keep {
        let v = expression.evaluate(&openduct_expr::Env::of(&body))?;
        out.insert(field.clone(), crate::template::as_text(&v));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::app_files::AppDef;
    use serde_json::json;

    const PAY: &str = r#"
app: pay_test
name: Pay
hosts: [api.pay.example]
base_url: https://api.pay.example
sign_in:
  - id: pay_test
    label: A client id and secret
    fields:
      - { name: client_id, label: Client id }
      - { name: client_secret, label: Client secret, secret: true }
    oauth2: { grant: client_credentials, token: "https://api.pay.example/v1/oauth2/token", client_auth: basic }
    headers: { Authorization: "Bearer {{ credential.access_token }}" }
actions:
  balance: { label: Balance, method: GET, path: /v1/balance }
"#;

    #[test]
    fn a_store_s_own_token_address_is_filled_in_and_must_be_on_the_app_s_hosts() {
        let text = PAY
            .replace("hosts: [api.pay.example]", "hosts: [\"*.pay.example\"]")
            .replace(
                "      - { name: client_id, label: Client id }\n",
                "      - { name: shop, label: Shop }\n      - { name: client_id, label: Client id }\n",
            )
            .replace(
                "token: \"https://api.pay.example/v1/oauth2/token\", client_auth: basic",
                "token: \"https://{{ credential.shop }}.pay.example/oauth/token\"",
            );
        let app = AppDef::parse(&text, "pay.yaml").unwrap();
        let sign_in = app.sign_in("pay_test").unwrap();
        let with = |shop: &str| Credential {
            kind: "pay_test".to_owned(),
            fields: [("shop", shop), ("client_id", "cid"), ("client_secret", "cs")]
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
        };
        let r = refresh(sign_in, &with("acme"), None).unwrap();
        assert_eq!(r.url, "https://acme.pay.example/oauth/token");
        let body = String::from_utf8(r.body.unwrap().1).unwrap();
        assert!(body.contains("client_secret=cs"), "{body}");
        // A field that steers the secret elsewhere is refused before anything is sent.
        let e = refresh(sign_in, &with("evil.example/x?"), None).unwrap_err();
        assert!(
            e.contains("evil.example") && e.contains("not one of the app's hosts"),
            "{e}"
        );
        // A person's sign-in at the vendor goes to the vendor's one address.
        let person = text
            .replace("      - { name: shop, label: Shop }\n      - { name: client_id, label: Client id }\n      - { name: client_secret, label: Client secret, secret: true }\n", "")
            .replace("    fields:\n", "    fields: []\n")
            .replace("{ grant: client_credentials,", "{ authorize: \"https://pay.example/connect\",");
        let e = AppDef::parse(&person, "pay.yaml").unwrap_err();
        assert!(e.contains("only a client_credentials sign-in"), "{e}");
    }

    #[test]
    fn a_web_page_for_a_token_says_the_account_may_be_closed() {
        let before = Credential {
            kind: "pay_test".to_owned(),
            fields: BTreeMap::new(),
        };
        let page =
            Value::from_json(&json!({ "status": 200, "body": "<!doctype html><html>trial expired</html>" }));
        let e = tokens(&page, &before).unwrap_err();
        assert!(e.contains("a web page, not a token"), "{e}");
    }

    #[test]
    fn client_credentials_are_traded_for_a_token_when_there_is_none_or_it_runs_out() {
        let app = AppDef::parse(PAY, "pay.yaml").unwrap();
        let sign_in = app.sign_in("pay_test").unwrap();
        let mut c = Credential {
            kind: "pay_test".to_owned(),
            fields: [("client_id", "cid"), ("client_secret", "cs")]
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
        };
        assert!(wanted(sign_in, &c, false));
        let r = refresh(sign_in, &c, None).unwrap();
        assert_eq!(r.url, "https://api.pay.example/v1/oauth2/token");
        assert!(
            r.headers
                .iter()
                .any(|(k, v)| k == "authorization" && v.starts_with("Basic "))
        );
        assert_eq!(
            String::from_utf8(r.body.unwrap().1).unwrap(),
            "grant_type=client_credentials"
        );
        let answer = Value::from_json(
            &json!({ "status": 200, "body": { "access_token": "A21", "expires_in": 32400 } }),
        );
        let got = tokens(&answer, &c).unwrap();
        assert_eq!(got.get("access_token").map(String::as_str), Some("A21"));
        c.fields.extend(got);
        assert!(!wanted(sign_in, &c, false));
        // About to run out: wanted again.
        c.fields.insert("expires_at".to_owned(), (now() + 10).to_string());
        assert!(wanted(sign_in, &c, false));
        // A test run, which reads no secret, never asks for one.
        c.fields.insert("client_secret".to_owned(), MASK.to_owned());
        assert!(!wanted(sign_in, &c, true));
        let refused = Value::from_json(
            &json!({ "status": 401, "body": { "error": "invalid_client", "error_description": "Client Authentication failed" } }),
        );
        assert!(
            tokens(&refused, &c)
                .unwrap_err()
                .contains("Client Authentication failed")
        );
    }

    #[test]
    fn a_sign_in_at_the_vendor_needs_the_install_s_registration_and_its_refresh_token() {
        let text = PAY
            .replace(
                "      - { name: client_id, label: Client id }\n      - { name: client_secret, label: Client secret, secret: true }\n",
                "",
            )
            .replace("    fields:\n", "    fields: []\n")
            .replace(
                "oauth2: { grant: client_credentials, token: \"https://api.pay.example/v1/oauth2/token\", client_auth: basic }",
                "oauth2: { authorize: \"https://pay.example/connect\", token: \"https://api.pay.example/v1/oauth2/token\" }",
            );
        let app = AppDef::parse(&text, "pay.yaml").unwrap();
        let sign_in = app.sign_in("pay_test").unwrap();
        let c = Credential {
            kind: "pay_test".to_owned(),
            fields: [("access_token", "a"), ("refresh_token", "r"), ("expires_at", "1")]
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
        };
        assert!(refresh(sign_in, &c, None).unwrap_err().contains("under Settings"));
        let reg = Credential {
            kind: "oauth_registration".to_owned(),
            fields: [("client_id", "cid"), ("client_secret", "cs")]
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
        };
        let r = refresh(sign_in, &c, Some(&reg)).unwrap();
        let body = String::from_utf8(r.body.unwrap().1).unwrap();
        assert!(
            body.contains("grant_type=refresh_token")
                && body.contains("refresh_token=r")
                && body.contains("client_id=cid"),
            "{body}"
        );
        let (verifier, challenge) = pkce().unwrap();
        assert_ne!(verifier, challenge);
        let url = authorize_url(
            sign_in.oauth2.as_ref().unwrap(),
            "cid",
            "https://flows.example.com/oauth/callback",
            "s",
            &challenge,
        )
        .unwrap();
        assert!(
            url.contains("code_challenge_method=S256")
                && url.contains("redirect_uri=https%3A%2F%2Fflows.example.com"),
            "{url}"
        );
    }
}
