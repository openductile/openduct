// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The webhook a repository calls on a push (`POST /hooks/git/<workspace>`): its signature checked with the secret
//! the studio made (a forged, unsigned or old delivery is refused, and its address slowed down as a wrong password
//! is), then a push to the workspace's branch deployed at the commit pushed. Other events and other branches are
//! answered and left alone.

use base64::Engine as _;
use hyper::StatusCode;
use ring::hmac;
use serde_json::{Value, json};

use super::{GitSettings, deploy, opened};
use crate::State;
use crate::api::Access;
use crate::http::{Answer, Call, json, problem};
use crate::model::{Role, SCHEMA, User, now};

/// How old a GitLab delivery's signed time may be, in seconds.
const MAX_AGE: u64 = 300;

fn header<'c>(call: &'c Call, name: &str) -> Option<&'c str> {
    call.headers
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

/// Whether `given` (hex) is the HMAC-SHA256 of `body` under `secret`, checked in constant time.
fn hex_signed(secret: &str, body: &[u8], given: &str) -> bool {
    let Some(tag) = hex(given.trim()) else {
        return false;
    };
    hmac::verify(&hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()), body, &tag).is_ok()
}

fn hex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) || text.len() > 256 {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| text.get(i..i + 2).and_then(|b| u8::from_str_radix(b, 16).ok()))
        .collect()
}

/// Whether two texts are the same, in constant time.
fn same(a: &str, b: &str) -> bool {
    let key = hmac::Key::new(hmac::HMAC_SHA256, b"compare");
    hmac::verify(&key, a.as_bytes(), hmac::sign(&key, b.as_bytes()).as_ref()).is_ok()
}

/// Whether a delivery comes from the repository: signed as its host signs.
pub(crate) fn signed(call: &Call, provider: &str, secret: &str, signing_token: Option<&str>) -> bool {
    match provider {
        "github" => header(call, "x-hub-signature-256")
            .and_then(|s| s.strip_prefix("sha256="))
            .is_some_and(|s| hex_signed(secret, &call.raw, s)),
        "gitea" => header(call, "x-gitea-signature")
            .map(str::to_owned)
            .or_else(|| {
                header(call, "x-hub-signature-256")
                    .and_then(|s| s.strip_prefix("sha256="))
                    .map(str::to_owned)
            })
            .is_some_and(|s| !s.is_empty() && hex_signed(secret, &call.raw, &s)),
        "gitlab" => match (signing_token, header(call, "webhook-signature")) {
            (Some(token), Some(signatures)) => gitlab_signed(call, token, signatures),
            (Some(_), None) => false,
            (None, _) => header(call, "x-gitlab-token").is_some_and(|t| same(t, secret)),
        },
        _ => false,
    }
}

/// GitLab's signing token: an HMAC-SHA256 of `<webhook-id>.<webhook-timestamp>.<body>`, in Base64 after `v1,`, the
/// key the token's Base64 after `whsec_`; the time within five minutes.
fn gitlab_signed(call: &Call, token: &str, signatures: &str) -> bool {
    let (Some(id), Some(at)) = (header(call, "webhook-id"), header(call, "webhook-timestamp")) else {
        return false;
    };
    let Ok(at_secs) = at.trim().parse::<u64>() else {
        return false;
    };
    if now().abs_diff(at_secs) > MAX_AGE {
        return false;
    }
    let b64 = base64::engine::general_purpose::STANDARD;
    let Ok(key) = b64.decode(token.trim().trim_start_matches("whsec_")) else {
        return false;
    };
    let mut message = format!("{id}.{at}.").into_bytes();
    message.extend_from_slice(&call.raw);
    let key = hmac::Key::new(hmac::HMAC_SHA256, &key);
    signatures
        .split(' ')
        .filter_map(|s| s.strip_prefix("v1,"))
        .filter_map(|s| b64.decode(s).ok())
        .any(|tag| hmac::verify(&key, &message, &tag).is_ok())
}

/// The push a delivery tells of: its branch's ref and the commit pushed; `None` for any other event.
fn pushed(call: &Call, provider: &str) -> Option<(String, String)> {
    let event = match provider {
        "github" => header(call, "x-github-event")?,
        "gitea" => header(call, "x-gitea-event")?,
        _ => header(call, "x-gitlab-event")?,
    };
    if !matches!(event, "push" | "Push Hook") {
        return None;
    }
    if call.body.get("deleted").and_then(Value::as_bool) == Some(true) {
        return None;
    }
    let reference = call.body.get("ref").and_then(Value::as_str)?.to_owned();
    let after = call
        .body
        .get("checkout_sha")
        .or_else(|| call.body.get("after"))
        .and_then(Value::as_str)
        .filter(|s| s.len() >= 7 && s.len() <= 64 && s.chars().all(|c| c.is_ascii_hexdigit()))?
        .to_owned();
    Some((reference, after))
}

pub(crate) async fn webhook(state: &State, call: &Call, workspace: &str) -> Answer {
    let failures = &state.cicd.hook_failures;
    if !failures.may_try(call.peer) {
        return problem(
            StatusCode::TOO_MANY_REQUESTS,
            "too many unsigned deliveries: wait a minute",
        );
    }
    let no_such = || problem(StatusCode::NOT_FOUND, "no such webhook");
    let Some(found) = state
        .model
        .workspaces
        .get(workspace)
        .await
        .filter(|w| w.deleted.is_none())
    else {
        failures.failed(call.peer);
        return no_such();
    };
    let delivery = state.cicd.delivery(workspace).await;
    let Some(git) = delivery.git.clone() else {
        failures.failed(call.peer);
        return no_such();
    };
    let Some(secret) = opened(state, workspace, "hook", &git.hook_secret) else {
        return problem(
            StatusCode::CONFLICT,
            "this workspace's Git webhook has no secret yet: make one in Settings",
        );
    };
    let signing = opened(state, workspace, "signing", &git.signing_token);
    if !signed(call, &git.provider, &secret, signing.as_deref()) {
        failures.failed(call.peer);
        tracing::warn!(workspace, peer = %call.peer, "a Git delivery not signed with the workspace's secret");
        return problem(
            StatusCode::UNAUTHORIZED,
            "the delivery is not signed with this webhook's secret",
        );
    }
    failures.succeeded(call.peer);
    let event = header(call, "x-github-event")
        .or_else(|| header(call, "x-gitea-event"))
        .unwrap_or_default();
    if event == "ping" {
        return json(StatusCode::OK, &json!({ "pong": true }));
    }
    let Some((reference, at)) = pushed(call, &git.provider) else {
        return json(StatusCode::ACCEPTED, &json!({ "ignored": "not a push" }));
    };
    if reference != format!("refs/heads/{}", git.branch) {
        return json(
            StatusCode::ACCEPTED,
            &json!({ "ignored": format!("a push to {reference}, not {}", git.branch) }),
        );
    }
    let access = as_git(found, &git);
    tracing::info!(workspace, commit = %at, "a push to the workspace's branch: deploying it");
    let (status, body) = deploy(state, call, &access, &git, &at, false).await;
    json(status, &body)
}

/// Who a deploy on a push acts as: Git, an editor of the workspace.
fn as_git(workspace: crate::model::Workspace, git: &GitSettings) -> Access {
    let name = format!("Git ({})", git.branch);
    Access {
        user: User {
            schema: SCHEMA,
            id: format!("git:{}", workspace.id),
            email: name.clone(),
            name,
            password: String::new(),
            generation: 0,
            admin: false,
            created: 0,
            updated: 0,
            deleted: None,
        },
        workspace,
        role: Role::Editor,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::net::IpAddr;

    use bytes::Bytes;
    use hyper::Method;

    use super::*;

    fn call(headers: &[(&str, &str)], body: &str) -> Call {
        Call {
            method: Method::POST,
            path: "/hooks/git/w".into(),
            query: String::new(),
            peer: IpAddr::from([127, 0, 0, 1]),
            cookie: None,
            csrf: None,
            workspace: None,
            body: serde_json::from_str(body).unwrap_or(Value::Null),
            raw: Bytes::from(body.to_owned()),
            content_type: "application/json".into(),
            headers: headers
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            host: "localhost".into(),
        }
    }

    fn hmac_hex(secret: &str, body: &str) -> String {
        hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()),
            body.as_bytes(),
        )
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
    }

    #[test]
    fn a_delivery_is_taken_only_signed_with_the_secret() {
        let body = r#"{"ref":"refs/heads/main","after":"1a2b3c4d5e6f7a8b9c0d1a2b3c4d5e6f7a8b9c0d"}"#;
        let good = format!("sha256={}", hmac_hex("s3cret", body));
        assert!(signed(
            &call(&[("x-hub-signature-256", &good)], body),
            "github",
            "s3cret",
            None
        ));
        assert!(!signed(
            &call(&[("x-hub-signature-256", &good)], body),
            "github",
            "other",
            None
        ));
        assert!(!signed(
            &call(&[("x-hub-signature-256", &good)], &body.replace("main", "evil")),
            "github",
            "s3cret",
            None
        ));
        assert!(!signed(&call(&[], body), "github", "s3cret", None), "unsigned");
        let bare = hmac_hex("s3cret", body);
        assert!(signed(
            &call(&[("x-gitea-signature", &bare)], body),
            "gitea",
            "s3cret",
            None
        ));
        assert!(!signed(
            &call(&[("x-gitea-signature", "")], body),
            "gitea",
            "s3cret",
            None
        ));
        assert!(signed(
            &call(&[("x-gitlab-token", "s3cret")], body),
            "gitlab",
            "s3cret",
            None
        ));
        assert!(!signed(
            &call(&[("x-gitlab-token", "s3cre")], body),
            "gitlab",
            "s3cret",
            None
        ));
        let c = call(&[("x-github-event", "push")], body);
        assert_eq!(pushed(&c, "github").unwrap().0, "refs/heads/main");
        assert!(pushed(&call(&[("x-github-event", "issues")], body), "github").is_none());
    }

    #[test]
    fn a_gitlab_signing_token_signs_the_id_the_time_and_the_body() {
        let b64 = base64::engine::general_purpose::STANDARD;
        let key = b"a key of thirty-two bytes, as is";
        let token = format!("whsec_{}", b64.encode(key));
        let body = r#"{"ref":"refs/heads/main"}"#;
        let at = now().to_string();
        let message = format!("msg_1.{at}.{body}");
        let tag =
            b64.encode(hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), message.as_bytes()).as_ref());
        let signature = format!("v1,{tag}");
        let headers = [
            ("webhook-id", "msg_1"),
            ("webhook-timestamp", at.as_str()),
            ("webhook-signature", signature.as_str()),
        ];
        assert!(signed(&call(&headers, body), "gitlab", "unused", Some(&token)));
        let old = (now() - 3600).to_string();
        let stale = [
            ("webhook-id", "msg_1"),
            ("webhook-timestamp", old.as_str()),
            ("webhook-signature", signature.as_str()),
        ];
        assert!(
            !signed(&call(&stale, body), "gitlab", "unused", Some(&token)),
            "an old delivery"
        );
        assert!(
            !signed(
                &call(&[("x-gitlab-token", "unused")], body),
                "gitlab",
                "unused",
                Some(&token)
            ),
            "the signature is needed once a signing token is given"
        );
    }
}
