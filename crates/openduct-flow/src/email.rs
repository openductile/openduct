// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An `email` step: a message sent through the mail server a credential names (host, port, security, who signs in,
//! from), as plain text, HTML, or both. TLS is required unless the credential says `none`, and a password is never
//! sent without it. The server's name is checked, as an HTTP step's is: one that leads to this machine's own network
//! is refused unless the operator allows it, and the address checked is the one connected to (the certificate still
//! checked against the name).

use std::net::SocketAddr;
use std::time::Duration;

use lettre::message::header::ContentType;
use lettre::message::{Mailbox, MultiPart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::transport::smtp::client::{Tls, TlsParameters};
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use openduct_expr::Value;
use serde_json::json;

use crate::effects::private;
use crate::run::EmailRequest;

/// The most addresses one message goes to, in all.
pub(crate) const MAX_RECIPIENTS: usize = 50;
/// The largest body, text and HTML together.
pub(crate) const MAX_BODY: usize = 1024 * 1024;
/// How long the mail server has to take the message.
const WAIT: Duration = Duration::from_secs(30);

fn mailbox(address: &str, what: &str) -> Result<Mailbox, String> {
    address
        .trim()
        .parse()
        .map_err(|e| format!("{what}: {address:?} is not an e-mail address ({e})"))
}

/// The message as a test run or a preview shows it, not sent.
pub(crate) fn shown(test: &str, r: &EmailRequest) -> Value {
    Value::from_json(&json!({
        "test": test,
        "email": {
            "from": r.credential.fields.get("from"),
            "to": r.to, "cc": r.cc, "bcc": r.bcc, "reply_to": r.reply_to,
            "subject": r.subject, "text": r.text, "html": r.html,
            // G6 files: what it carries, by name, type and size.
            "attachments": r.attachments.iter()
                .map(|a| json!({ "name": a.name, "type": a.media_type, "size": a.bytes.len() }))
                .collect::<Vec<_>>(),
        },
    }))
}

/// The most a message's attachments hold, in all: most mail servers take messages of up to 25 MB. // G6 files
pub(crate) const MAX_ATTACHED: usize = 20 * 1024 * 1024;

/// Checks what every message needs, before one is sent or shown.
pub(crate) fn check(r: &EmailRequest) -> Result<(), String> {
    if r.to.is_empty() {
        return Err("to: no address to send it to".to_owned());
    }
    if r.to.len() + r.cc.len() + r.bcc.len() > MAX_RECIPIENTS {
        return Err(format!("at most {MAX_RECIPIENTS} addresses in all"));
    }
    let size = r.text.as_ref().map_or(0, String::len) + r.html.as_ref().map_or(0, String::len);
    if size > MAX_BODY {
        return Err(format!("a body of at most {} KiB", MAX_BODY / 1024));
    }
    // G6 files
    if r.attachments.iter().map(|a| a.bytes.len()).sum::<usize>() > MAX_ATTACHED {
        let most = MAX_ATTACHED / 1024 / 1024;
        return Err(format!("attachments of at most {most} MiB in all"));
    }
    if r.subject.contains(['\r', '\n']) {
        return Err("subject: one line".to_owned());
    }
    for (list, what) in [(&r.to, "to"), (&r.cc, "cc"), (&r.bcc, "bcc")] {
        for address in list {
            mailbox(address, what)?;
        }
    }
    if let Some(reply) = &r.reply_to {
        mailbox(reply, "reply_to")?;
    }
    Ok(())
}

/// A message's body and its files, as one mixed message. // G6 files
fn attached(
    text: Option<String>,
    html: Option<String>,
    files: &[crate::files::FileData],
) -> Result<MultiPart, String> {
    use lettre::message::{Attachment, SinglePart};
    let builder = MultiPart::mixed();
    let mut message = match (text, html) {
        (Some(text), Some(html)) => builder.multipart(MultiPart::alternative_plain_html(text, html)),
        (Some(text), None) => builder.singlepart(SinglePart::plain(text)),
        (None, Some(html)) => builder.singlepart(SinglePart::html(html)),
        (None, None) => return Err("text, html or both".to_owned()),
    };
    for file in files {
        let kind = ContentType::parse(&file.media_type)
            .or_else(|_| ContentType::parse("application/octet-stream"))
            .map_err(|e| format!("{}: {e}", file.name))?;
        message = message.singlepart(Attachment::new(file.name.clone()).body(file.bytes.to_vec(), kind));
    }
    Ok(message)
}

/// Sends the message.
pub(crate) async fn send(r: EmailRequest, allow_private: bool) -> Result<Value, String> {
    check(&r)?;
    let field = |name: &str| {
        r.credential
            .fields
            .get(name)
            .filter(|v| !v.trim().is_empty())
            .cloned()
    };
    let host = field("host").ok_or("the credential has no host")?;
    let security = field("security").unwrap_or_else(|| "starttls".to_owned());
    let port = match field("port") {
        Some(p) => p
            .trim()
            .parse::<u16>()
            .map_err(|_| format!("port: {p:?} is not a port"))?,
        None => match security.as_str() {
            "tls" => 465,
            "none" => 25,
            _ => 587,
        },
    };
    let from = mailbox(
        &field("from").ok_or("the credential has no from address")?,
        "from",
    )?;
    let username = field("username");
    if security == "none" && username.is_some() {
        return Err("a password is sent only over TLS: set security to starttls or tls".to_owned());
    }
    let found: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|e| format!("cannot find the mail server {host}: {e}"))?
        .collect();
    if !allow_private && found.iter().any(|a| private(a.ip())) {
        return Err(format!(
            "{host} is on this machine's own network: start OpenDuct with --allow-private-network to let workflows \
             reach it"
        ));
    }
    let address = found
        .first()
        .ok_or_else(|| format!("cannot find the mail server {host}"))?;

    let mut builder = Message::builder().from(from).subject(r.subject.clone());
    for address in &r.to {
        builder = builder.to(mailbox(address, "to")?);
    }
    for address in &r.cc {
        builder = builder.cc(mailbox(address, "cc")?);
    }
    for address in &r.bcc {
        builder = builder.bcc(mailbox(address, "bcc")?);
    }
    if let Some(reply) = &r.reply_to {
        builder = builder.reply_to(mailbox(reply, "reply_to")?);
    }
    let message = match (r.text.clone(), r.html.clone()) {
        // G6 files: with attachments, the body and each file as parts of a mixed message.
        (text, html) if !r.attachments.is_empty() => builder.multipart(attached(text, html, &r.attachments)?),
        (Some(text), Some(html)) => builder.multipart(MultiPart::alternative_plain_html(text, html)),
        (Some(text), None) => builder.header(ContentType::TEXT_PLAIN).body(text),
        (None, Some(html)) => builder.header(ContentType::TEXT_HTML).body(html),
        (None, None) => return Err("text, html or both".to_owned()),
    }
    .map_err(|e| format!("cannot write the message: {e}"))?;

    let parameters = || TlsParameters::new(host.clone()).map_err(|e| format!("cannot set up TLS: {e}"));
    let tls = match security.as_str() {
        "starttls" => Tls::Required(parameters()?),
        "tls" => Tls::Wrapper(parameters()?),
        "none" => Tls::None,
        other => return Err(format!("security: starttls, tls or none, not {other:?}")),
    };
    let mut transport = AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(address.ip().to_string())
        .port(port)
        .tls(tls)
        .timeout(Some(WAIT));
    if let Some(username) = username {
        transport = transport.credentials(Credentials::new(username, field("password").unwrap_or_default()));
    }
    let answer = transport
        .build()
        .send(message)
        .await
        .map_err(|e| format!("the mail server {host} did not take it: {e}"))?;
    Ok(Value::from_json(&json!({
        "sent": true,
        "to": r.to,
        "server": answer.message().collect::<Vec<_>>().join(" "),
    })))
}
