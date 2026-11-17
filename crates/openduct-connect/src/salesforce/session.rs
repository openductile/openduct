// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A session with Salesforce's REST API: an access token got with OAuth 2.0 (client credentials, a JWT signed with
//! the integration's private key, or a refresh token), sent with every call, and got again once when Salesforce
//! says the session has ended. Salesforce's errors become words; no message shows a secret.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::{Client, Method, StatusCode};

use crate::web;

/// How the pipeline signs in.
#[derive(Clone)]
pub(super) enum Auth {
    /// A connected app's own client id and secret (its "client credentials" flow).
    ClientCredentials {
        client_id: String,
        client_secret: String,
    },
    /// A JWT signed with the connected app's private key, for a pre-authorised user.
    Jwt {
        client_id: String,
        username: String,
        key: Arc<ring::signature::RsaKeyPair>,
    },
    /// A refresh token a person granted the connected app.
    Refresh {
        client_id: String,
        client_secret: Option<String>,
        refresh_token: String,
    },
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ClientCredentials { .. } => "ClientCredentials",
            Self::Jwt { .. } => "Jwt",
            Self::Refresh { .. } => "Refresh",
        })
    }
}

#[derive(Debug, Clone)]
struct Token {
    access: String,
    instance: String,
}

#[derive(Debug)]
pub(super) struct Session {
    client: Client,
    /// Where tokens are got: the org's My Domain (`https://acme.my.salesforce.com`) or `https://login.salesforce.com`.
    login_url: String,
    auth: Auth,
    api_version: String,
    token: Option<Token>,
}

/// Salesforce's error answers (`[{"errorCode": ..., "message": ...}]`, or OAuth's `{"error_description": ...}`)
/// in words.
pub(super) fn error_words(body: &[u8]) -> String {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(body) else {
        return String::from_utf8_lossy(body.get(..body.len().min(300)).unwrap_or_default()).into_owned();
    };
    if let Some(d) = v.get("error_description").and_then(|d| d.as_str()) {
        return d.to_owned();
    }
    let items = v.as_array().cloned().unwrap_or_else(|| vec![v.clone()]);
    items
        .iter()
        .map(|e| {
            let code = e
                .get("errorCode")
                .or_else(|| e.get("statusCode"))
                .and_then(|c| c.as_str())
                .unwrap_or("");
            let message = e.get("message").and_then(|m| m.as_str()).unwrap_or("");
            if code.is_empty() {
                message.to_owned()
            } else {
                format!("{code}: {message}")
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

impl Session {
    pub(super) fn new(
        login_url: String,
        auth: Auth,
        api_version: String,
        private_network: bool,
    ) -> Result<Self, String> {
        Ok(Self {
            client: web::client(private_network)?,
            login_url,
            auth,
            api_version,
            token: None,
        })
    }

    async fn sign_in(&mut self) -> Result<Token, String> {
        let mut form: Vec<(&str, String)> = Vec::new();
        match &self.auth {
            Auth::ClientCredentials {
                client_id,
                client_secret,
            } => {
                form.push(("grant_type", "client_credentials".to_owned()));
                form.push(("client_id", client_id.clone()));
                form.push(("client_secret", client_secret.clone()));
            }
            Auth::Jwt {
                client_id,
                username,
                key,
            } => {
                form.push((
                    "grant_type",
                    "urn:ietf:params:oauth:grant-type:jwt-bearer".to_owned(),
                ));
                form.push(("assertion", jwt(client_id, username, &self.login_url, key)?));
            }
            Auth::Refresh {
                client_id,
                client_secret,
                refresh_token,
            } => {
                form.push(("grant_type", "refresh_token".to_owned()));
                form.push(("client_id", client_id.clone()));
                form.push(("refresh_token", refresh_token.clone()));
                if let Some(secret) = client_secret {
                    form.push(("client_secret", secret.clone()));
                }
            }
        }
        let url = format!("{}/services/oauth2/token", self.login_url);
        let encoded: Vec<String> = form
            .iter()
            .map(|(k, v)| {
                format!(
                    "{k}={}",
                    percent_encoding::utf8_percent_encode(v, percent_encoding::NON_ALPHANUMERIC)
                )
            })
            .collect();
        let encoded = encoded.join("&");
        let answer = web::send(
            || {
                self.client
                    .post(&url)
                    .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(encoded.clone())
            },
            "Salesforce's sign-in",
        )
        .await?;
        let status = answer.status();
        let body = web::body(answer, "Salesforce's sign-in").await?;
        if !status.is_success() {
            return Err(format!("Salesforce refused signing in: {}", error_words(&body)));
        }
        let v: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|_| "Salesforce's sign-in answered with something else".to_owned())?;
        let access = v.get("access_token").and_then(|t| t.as_str());
        let instance = v.get("instance_url").and_then(|t| t.as_str());
        match (access, instance) {
            (Some(a), Some(i))
                if i.starts_with("https://")
                    || i.starts_with("http://127.0.0.1")
                    || i.starts_with("http://localhost") =>
            {
                Ok(Token {
                    access: a.to_owned(),
                    instance: i.trim_end_matches('/').to_owned(),
                })
            }
            _ => Err("Salesforce's sign-in gave no access token and instance".to_owned()),
        }
    }

    /// `/services/data/vNN.N/<path>` on the org.
    pub(super) fn data_path(&self, path: &str) -> String {
        format!("/services/data/v{}/{path}", self.api_version)
    }

    /// Call the org's REST API (`path` is from `/services/...`); the status and body. A session that has ended is
    /// begun again once.
    pub(super) async fn call(
        &mut self,
        method: Method,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<(StatusCode, Vec<u8>), String> {
        for attempt in 0..2 {
            let token = match &self.token {
                Some(t) => t.clone(),
                None => {
                    let t = self.sign_in().await?;
                    self.token = Some(t.clone());
                    t
                }
            };
            let url = format!("{}{path}", token.instance);
            let json = body.map(serde_json::Value::to_string);
            let what = format!("Salesforce ({path})");
            let answer = web::send(
                || {
                    let mut r = self
                        .client
                        .request(method.clone(), &url)
                        .bearer_auth(&token.access);
                    if let Some(b) = &json {
                        r = r
                            .header(reqwest::header::CONTENT_TYPE, "application/json")
                            .body(b.clone());
                    }
                    r
                },
                &what,
            )
            .await?;
            let status = answer.status();
            let bytes = web::body(answer, &what).await?;
            if status == StatusCode::UNAUTHORIZED && attempt == 0 {
                self.token = None;
                continue;
            }
            return Ok((status, bytes));
        }
        Err("Salesforce refused the session again after signing in again".to_owned())
    }
}

/// A JWT for Salesforce's JWT bearer flow: the connected app, the user, the login URL, and three minutes to use it.
fn jwt(
    client_id: &str,
    username: &str,
    audience: &str,
    key: &ring::signature::RsaKeyPair,
) -> Result<String, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let claims = serde_json::json!({ "iss": client_id, "sub": username, "aud": audience, "exp": now + 180 });
    crate::jwt::signed(&claims, key)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn errors_are_words() {
        assert_eq!(
            error_words(br#"[{"message":"No such column 'Nme'","errorCode":"INVALID_FIELD"}]"#),
            "INVALID_FIELD: No such column 'Nme'"
        );
        assert_eq!(
            error_words(br#"{"error":"invalid_client","error_description":"invalid client credentials"}"#),
            "invalid client credentials"
        );
        assert_eq!(error_words(b"Service Unavailable"), "Service Unavailable");
    }
}
