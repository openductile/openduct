// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A session with a ServiceNow instance's REST API: basic sign-in, or an OAuth 2.0 token (the password grant or
//! client credentials) from the instance's `oauth_token.do`, got again once when the instance says it has ended.
//! ServiceNow's errors become words; no message shows a secret.

use reqwest::{Client, Method, StatusCode};

use crate::web;

#[derive(Clone)]
pub(super) enum Auth {
    Basic {
        username: String,
        password: String,
    },
    /// OAuth's password grant: an OAuth application's client id and secret, and an integration user.
    Password {
        client_id: String,
        client_secret: String,
        username: String,
        password: String,
    },
    /// OAuth's client credentials grant (an OAuth application allowed it).
    ClientCredentials {
        client_id: String,
        client_secret: String,
    },
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Basic { .. } => "Basic",
            Self::Password { .. } => "Password",
            Self::ClientCredentials { .. } => "ClientCredentials",
        })
    }
}

#[derive(Debug)]
pub(super) struct Session {
    client: Client,
    /// `https://acme.service-now.com`.
    pub(super) instance: String,
    auth: Auth,
    token: Option<String>,
}

/// ServiceNow's error answers (`{"error": {"message": ..., "detail": ...}, "status": "failure"}`, or OAuth's
/// `{"error_description": ...}`) in words.
pub(super) fn error_words(body: &[u8]) -> String {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(body) else {
        return String::from_utf8_lossy(body.get(..body.len().min(300)).unwrap_or_default()).into_owned();
    };
    if let Some(d) = v.get("error_description").and_then(|d| d.as_str()) {
        return d.to_owned();
    }
    let e = v.get("error").unwrap_or(&v);
    let message = e.get("message").and_then(|m| m.as_str()).unwrap_or("");
    match e.get("detail").and_then(|d| d.as_str()).filter(|d| !d.is_empty()) {
        Some(detail) => format!("{message}: {detail}"),
        None => message.to_owned(),
    }
}

fn form(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| {
            format!(
                "{k}={}",
                percent_encoding::utf8_percent_encode(v, percent_encoding::NON_ALPHANUMERIC)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

impl Session {
    pub(super) fn new(instance: String, auth: Auth, private_network: bool) -> Result<Self, String> {
        Ok(Self {
            client: web::client(private_network)?,
            instance,
            auth,
            token: None,
        })
    }

    async fn token(&mut self) -> Result<Option<String>, String> {
        let body = match &self.auth {
            Auth::Basic { .. } => return Ok(None),
            Auth::Password {
                client_id,
                client_secret,
                username,
                password,
            } => form(&[
                ("grant_type", "password"),
                ("client_id", client_id),
                ("client_secret", client_secret),
                ("username", username),
                ("password", password),
            ]),
            Auth::ClientCredentials {
                client_id,
                client_secret,
            } => form(&[
                ("grant_type", "client_credentials"),
                ("client_id", client_id),
                ("client_secret", client_secret),
            ]),
        };
        if let Some(t) = &self.token {
            return Ok(Some(t.clone()));
        }
        let url = format!("{}/oauth_token.do", self.instance);
        let answer = web::send(
            || {
                self.client
                    .post(&url)
                    .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(body.clone())
            },
            "ServiceNow's sign-in",
        )
        .await?;
        let status = answer.status();
        let bytes = web::body(answer, "ServiceNow's sign-in").await?;
        if !status.is_success() {
            return Err(format!("ServiceNow refused signing in: {}", error_words(&bytes)));
        }
        let v: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|_| "ServiceNow's sign-in answered with something else".to_owned())?;
        let token = v
            .get("access_token")
            .and_then(|t| t.as_str())
            .ok_or("ServiceNow's sign-in gave no access token")?
            .to_owned();
        self.token = Some(token.clone());
        Ok(Some(token))
    }

    /// Call the instance's REST API (`path` from `/api/...`); the status and body. An ended session is begun again
    /// once.
    pub(super) async fn call(
        &mut self,
        method: Method,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<(StatusCode, Vec<u8>), String> {
        let json = body.map(serde_json::Value::to_string);
        for attempt in 0..2 {
            let token = self.token().await?;
            let url = format!("{}{path}", self.instance);
            let what = format!("ServiceNow ({})", path.split('?').next().unwrap_or(path));
            let answer = web::send(
                || {
                    let mut r = self
                        .client
                        .request(method.clone(), &url)
                        .header(reqwest::header::ACCEPT, "application/json");
                    r = match (&token, &self.auth) {
                        (Some(t), _) => r.bearer_auth(t),
                        (None, Auth::Basic { username, password }) => r.basic_auth(username, Some(password)),
                        (None, _) => r,
                    };
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
            if status == StatusCode::UNAUTHORIZED {
                if attempt == 0 && token.is_some() {
                    self.token = None;
                    continue;
                }
                return Err(format!(
                    "ServiceNow refused the username or password: {}",
                    error_words(&bytes)
                ));
            }
            return Ok((status, bytes));
        }
        Err("ServiceNow refused the session again after signing in again".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_are_words() {
        assert_eq!(
            error_words(br#"{"error":{"message":"Invalid table","detail":"Table incidnet does not exist"},"status":"failure"}"#),
            "Invalid table: Table incidnet does not exist"
        );
        assert_eq!(
            error_words(br#"{"error":"access_denied","error_description":"access_denied"}"#),
            "access_denied"
        );
        assert_eq!(form(&[("a", "x y"), ("b", "&")]), "a=x%20y&b=%26");
    }
}
