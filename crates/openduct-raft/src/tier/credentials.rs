// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! AWS credentials for object storage: the keys given (from `--tier-s3-access-key-file` and
//! `--tier-s3-secret-key-file`), or else a role, first that
//! answers - a role holds no secret, so nothing secret is ever read from the environment:
//!
//! 1. a web identity (EKS service accounts): `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN`, exchanged with STS;
//! 2. the container endpoint (ECS tasks, EKS pod identity): `AWS_CONTAINER_CREDENTIALS_FULL_URI` or
//!    `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, with `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`;
//! 3. the instance's metadata (EC2), version 2.
//!
//! Credentials that expire are fetched again five minutes before they do.

use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::Client;

/// Keys that sign requests.
#[derive(Clone)]
pub(crate) struct Keys {
    pub(crate) access: String,
    pub(crate) secret: String,
    pub(crate) token: Option<String>,
    expires: Option<DateTime<Utc>>,
}

impl std::fmt::Debug for Keys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never the keys themselves.
        f.debug_struct("Keys")
            .field("expires", &self.expires)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) struct Credentials {
    client: Client,
    region: String,
    given: Option<(String, String)>,
    cached: tokio::sync::Mutex<Option<Keys>>,
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn field(json: &serde_json::Value, name: &str) -> Option<String> {
    json.get(name).and_then(|v| v.as_str()).map(str::to_owned)
}

fn from_json(json: &serde_json::Value) -> Option<Keys> {
    Some(Keys {
        access: field(json, "AccessKeyId")?,
        secret: field(json, "SecretAccessKey")?,
        token: field(json, "Token"),
        expires: field(json, "Expiration").and_then(|e| {
            DateTime::parse_from_rfc3339(&e)
                .ok()
                .map(|d| d.with_timezone(&Utc))
        }),
    })
}

/// The text of the first `<tag>` in an XML answer.
fn xml(text: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let start = text.find(&open)? + open.len();
    let end = text.get(start..)?.find(&format!("</{tag}>"))? + start;
    text.get(start..end).map(str::to_owned)
}

impl Credentials {
    pub(crate) fn new(client: Client, region: String, given: Option<(String, String)>) -> Self {
        Self {
            client,
            region,
            given,
            cached: tokio::sync::Mutex::new(None),
        }
    }

    pub(crate) async fn keys(&self) -> Result<Keys, String> {
        let mut cached = self.cached.lock().await;
        if let Some(k) = cached.as_ref()
            && k.expires
                .is_none_or(|e| e > Utc::now() + chrono::Duration::minutes(5))
        {
            return Ok(k.clone());
        }
        let keys = self.find().await?;
        *cached = Some(keys.clone());
        Ok(keys)
    }

    async fn find(&self) -> Result<Keys, String> {
        if let Some((access, secret)) = &self.given {
            return Ok(Keys {
                access: access.clone(),
                secret: secret.clone(),
                token: None,
                expires: None,
            });
        }
        if let (Some(file), Some(role)) = (env("AWS_WEB_IDENTITY_TOKEN_FILE"), env("AWS_ROLE_ARN")) {
            return self.web_identity(&file, &role).await;
        }
        if let Some(uri) = env("AWS_CONTAINER_CREDENTIALS_FULL_URI").or_else(|| {
            env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI").map(|r| format!("http://169.254.170.2{r}"))
        }) {
            return self.container(&uri).await;
        }
        self.instance().await
    }

    async fn web_identity(&self, file: &str, role: &str) -> Result<Keys, String> {
        let token = std::fs::read_to_string(file)
            .map_err(|e| format!("cannot read the web identity token {file}: {e}"))?;
        let form = [
            ("Action", "AssumeRoleWithWebIdentity"),
            ("Version", "2011-06-15"),
            ("RoleArn", role),
            ("RoleSessionName", "openduct"),
            ("WebIdentityToken", token.trim()),
        ];
        let body: String = form
            .iter()
            .map(|(k, v)| format!("{k}={}", percent(v)))
            .collect::<Vec<_>>()
            .join("&");
        let answer = self
            .client
            .post(format!("https://sts.{}.amazonaws.com/", self.region))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await
            .map_err(|e| format!("STS could not be reached: {e}"))?;
        let status = answer.status();
        let text = answer.text().await.map_err(|e| e.to_string())?;
        if !status.is_success() {
            return Err(format!("STS refused the web identity: {status}"));
        }
        Ok(Keys {
            access: xml(&text, "AccessKeyId").ok_or("STS answered no access key")?,
            secret: xml(&text, "SecretAccessKey").ok_or("STS answered no secret key")?,
            token: xml(&text, "SessionToken"),
            expires: xml(&text, "Expiration").and_then(|e| {
                DateTime::parse_from_rfc3339(&e)
                    .ok()
                    .map(|d| d.with_timezone(&Utc))
            }),
        })
    }

    async fn container(&self, uri: &str) -> Result<Keys, String> {
        let mut request = self.client.get(uri).timeout(Duration::from_secs(5));
        let token = match env("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") {
            Some(file) => {
                Some(std::fs::read_to_string(&file).map_err(|e| format!("cannot read {file}: {e}"))?)
            }
            None => None,
        };
        if let Some(t) = token {
            request = request.header("Authorization", t.trim().to_owned());
        }
        let text = request
            .send()
            .await
            .map_err(|e| format!("the container credentials endpoint could not be reached: {e}"))?
            .text()
            .await
            .map_err(|e| format!("the container credentials endpoint answered: {e}"))?;
        let json: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| format!("the container credentials endpoint answered: {e}"))?;
        from_json(&json).ok_or_else(|| "the container credentials endpoint answered no keys".to_owned())
    }

    async fn instance(&self) -> Result<Keys, String> {
        const IMDS: &str = "http://169.254.169.254/latest";
        let unreachable = |e: reqwest::Error| {
            format!(
                "no AWS credentials: none in the environment, and the instance metadata could not be reached ({e})"
            )
        };
        let token = self
            .client
            .put(format!("{IMDS}/api/token"))
            .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .map_err(unreachable)?
            .text()
            .await
            .map_err(unreachable)?;
        let get = |path: String| {
            self.client
                .get(format!("{IMDS}/meta-data/iam/security-credentials/{path}"))
                .header("X-aws-ec2-metadata-token", token.clone())
                .timeout(Duration::from_secs(2))
                .send()
        };
        let roles = get(String::new())
            .await
            .map_err(unreachable)?
            .text()
            .await
            .map_err(unreachable)?;
        let role = roles
            .lines()
            .next()
            .ok_or("the instance has no role")?
            .trim()
            .to_owned();
        let text = get(role)
            .await
            .map_err(unreachable)?
            .text()
            .await
            .map_err(unreachable)?;
        let json: serde_json::Value =
            serde_json::from_str(&text).map_err(|e| format!("the instance metadata answered: {e}"))?;
        from_json(&json).ok_or_else(|| "the instance metadata answered no keys".to_owned())
    }
}

/// Percent-encodes everything but the unreserved characters, as AWS wants in forms and paths.
pub(crate) fn percent(text: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(text.len());
    for b in text.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_come_out_of_sts_xml_and_metadata_json() {
        let text = "<AssumeRoleWithWebIdentityResponse><Credentials><AccessKeyId>AKIA</AccessKeyId>\
                    <SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken>\
                    <Expiration>2026-11-16T00:00:00Z</Expiration></Credentials></AssumeRoleWithWebIdentityResponse>";
        assert_eq!(xml(text, "AccessKeyId").as_deref(), Some("AKIA"));
        assert_eq!(xml(text, "SessionToken").as_deref(), Some("t"));
        let json = serde_json::json!({"AccessKeyId": "A", "SecretAccessKey": "hidden-secret", "Token": "T", "Expiration": "2026-11-16T00:00:00Z"});
        let keys = from_json(&json).unwrap();
        assert_eq!((keys.access.as_str(), keys.token.as_deref()), ("A", Some("T")));
        assert!(keys.expires.is_some());
        assert!(
            !format!("{keys:?}").contains("hidden-secret"),
            "keys are never printed"
        );
    }
}
