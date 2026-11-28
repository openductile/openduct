// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Azure Storage's Shared Key, as Microsoft documents it ("Authorize with Shared Key", the Blob service from
//! version 2009-09-19): a request's string-to-sign - its verb, its standard headers in their order (Content-Length
//! empty when zero), its `x-ms-` headers lowercased and sorted, and its resource (`/account/path`, then each query
//! parameter lowercased and sorted, its values decoded) - signed with HMAC-SHA256 under the account's key
//! (base64-decoded) and sent as `Authorization: SharedKey <account>:<signature>`. And a service SAS for one blob
//! ("Create a service SAS", the string-to-sign of version 2020-12-06 and later), for an address that works without
//! the key for a while.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;

/// The standard headers a Shared Key signature takes, in the order it takes them.
const STANDARD: &[&str] = &[
    "content-encoding",
    "content-language",
    "content-length",
    "content-md5",
    "content-type",
    "date",
    "if-modified-since",
    "if-match",
    "if-none-match",
    "if-unmodified-since",
    "range",
];

/// The account's key, decoded from the base64 the portal gives.
pub(crate) fn key(text: &str) -> Result<Vec<u8>, String> {
    let key = B64
        .decode(text.trim())
        .map_err(|_| "shared_key: the account's key as the portal gives it (base64)".to_owned())?;
    if key.is_empty() || key.len() > 256 {
        return Err("shared_key: the account's key as the portal gives it (base64)".to_owned());
    }
    Ok(key)
}

/// `text` with its runs of spaces, tabs and line breaks made one space, and trimmed.
fn collapsed(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// `text` percent-decoded (a query's `+` is a space).
pub(crate) fn decoded(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while let Some(&b) = bytes.get(i) {
        if b == b'%'
            && let Some(v) = bytes
                .get(i + 1..i + 3)
                .and_then(|h| std::str::from_utf8(h).ok())
                .and_then(|h| u8::from_str_radix(h, 16).ok())
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(if b == b'+' { b' ' } else { b });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The string-to-sign of a request: `headers` as sent (names in any case), `path` encoded as in the address, and
/// `query` its parameters as they are in the address (encoded).
pub(crate) fn string_to_sign(
    method: &str,
    headers: &[(String, String)],
    account: &str,
    path: &str,
    query: &[(String, String)],
) -> String {
    let lower: Vec<(String, String)> = headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
        .collect();
    let get = |name: &str| {
        lower
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.trim().to_owned())
            .unwrap_or_default()
    };
    let mut out = format!("{}\n", method.to_ascii_uppercase());
    for name in STANDARD {
        let mut value = get(name);
        if *name == "content-length" && value == "0" {
            value.clear();
        }
        // x-ms-date is given, so Date signs as empty.
        if *name == "date" && lower.iter().any(|(k, _)| k == "x-ms-date") {
            value.clear();
        }
        out.push_str(&value);
        out.push('\n');
    }
    let mut ms: Vec<(String, String)> = lower
        .iter()
        .filter(|(k, _)| k.starts_with("x-ms-"))
        .map(|(k, v)| (k.clone(), collapsed(v)))
        .collect();
    ms.sort();
    for (k, v) in ms {
        out.push_str(&format!("{k}:{v}\n"));
    }
    out.push('/');
    out.push_str(account);
    out.push_str(path);
    let mut params: Vec<(String, Vec<String>)> = Vec::new();
    for (k, v) in query {
        let name = decoded(k).to_ascii_lowercase();
        let value = decoded(v);
        match params.iter_mut().find(|(n, _)| *n == name) {
            Some((_, values)) => values.push(value),
            None => params.push((name, vec![value])),
        }
    }
    params.sort_by(|a, b| a.0.cmp(&b.0));
    for (name, mut values) in params {
        values.sort();
        out.push_str(&format!("\n{name}:{}", values.join(",")));
    }
    out
}

fn sign(key: &[u8], text: &str) -> String {
    let k = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key);
    B64.encode(ring::hmac::sign(&k, text.as_bytes()).as_ref())
}

/// The `Authorization` header of a request.
pub(crate) fn authorization(
    key: &[u8],
    method: &str,
    headers: &[(String, String)],
    account: &str,
    path: &str,
    query: &[(String, String)],
) -> String {
    let signature = sign(key, &string_to_sign(method, headers, account, path, query));
    format!("SharedKey {account}:{signature}")
}

/// What a service SAS for one blob says.
#[derive(Debug, Clone)]
pub(crate) struct Sas<'a> {
    /// `r` to read; `cw` to create or write.
    pub permissions: &'a str,
    /// Until when, as ISO 8601 writes it (`2026-11-27T10:00:00Z`).
    pub expiry: &'a str,
    pub account: &'a str,
    pub container: &'a str,
    /// The blob's name, as it is (not encoded).
    pub blob: &'a str,
    /// `https`, or `https,http` for an emulator on http.
    pub protocol: &'a str,
    pub version: &'a str,
}

/// The query string of a service SAS for one blob, signed with the account's key.
pub(crate) fn service_sas(key: &[u8], sas: &Sas<'_>) -> String {
    let resource = format!("/blob/{}/{}/{}", sas.account, sas.container, sas.blob);
    // permissions, start, expiry, resource, identifier, IP, protocol, version, resource kind, snapshot time,
    // encryption scope, then the five response headers it may override.
    let to_sign = [
        sas.permissions,
        "",
        sas.expiry,
        &resource,
        "",
        "",
        sas.protocol,
        sas.version,
        "b",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
    ]
    .join("\n");
    let signature = sign(key, &to_sign);
    super::web::query(&[
        ("sv", sas.version.to_owned()),
        ("spr", sas.protocol.to_owned()),
        ("se", sas.expiry.to_owned()),
        ("sr", "b".to_owned()),
        ("sp", sas.permissions.to_owned()),
        ("sig", signature),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_string_to_sign_is_the_one_microsoft_shows() {
        // "Authorize with Shared Key": the Get Container Metadata example, line by line.
        let headers = vec![
            ("x-ms-date".to_owned(), "Fri, 26 Jun 2015 23:39:12 GMT".to_owned()),
            ("x-ms-version".to_owned(), "2015-02-21".to_owned()),
        ];
        let query = vec![
            ("restype".to_owned(), "container".to_owned()),
            ("comp".to_owned(), "metadata".to_owned()),
            ("timeout".to_owned(), "20".to_owned()),
        ];
        assert_eq!(
            string_to_sign("GET", &headers, "myaccount", "/mycontainer", &query),
            "GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:Fri, 26 Jun 2015 23:39:12 GMT\nx-ms-version:2015-02-21\n\
             /myaccount/mycontainer\ncomp:metadata\nrestype:container\ntimeout:20"
        );
        // The List Blobs example: several values of one parameter, sorted and joined.
        let query: Vec<(String, String)> = [
            ("restype", "container"),
            ("comp", "list"),
            ("include", "snapshots"),
            ("include", "metadata"),
            ("include", "uncommittedblobs"),
        ]
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
        assert!(
            string_to_sign("GET", &[], "myaccount", "/mycontainer", &query)
                .ends_with("/myaccount/mycontainer\ncomp:list\ninclude:metadata,snapshots,uncommittedblobs\nrestype:container")
        );
        // A zero length signs as empty; a body's type and length sign in their places.
        let put = vec![
            ("Content-Length".to_owned(), "0".to_owned()),
            ("x-ms-date".to_owned(), "d".to_owned()),
            ("x-ms-version".to_owned(), "2015-02-21".to_owned()),
        ];
        assert!(string_to_sign("PUT", &put, "a", "/c", &[]).starts_with("PUT\n\n\n\n\n"));
        let typed = vec![
            ("content-type".to_owned(), "text/csv".to_owned()),
            ("content-length".to_owned(), "12".to_owned()),
            ("if-none-match".to_owned(), "*".to_owned()),
        ];
        assert_eq!(
            string_to_sign("PUT", &typed, "a", "/c/b%20x", &[]),
            "PUT\n\n\n12\n\ntext/csv\n\n\n\n*\n\n\n/a/c/b%20x"
        );
        assert_eq!(decoded("a%2Fb+c"), "a/b c");
        assert!(key("not base64!").is_err() && key("a2V5").is_ok());
    }

    #[test]
    fn a_service_sas_names_what_it_signs() {
        let q = service_sas(
            b"key",
            &Sas {
                permissions: "r",
                expiry: "2026-11-27T10:00:00Z",
                account: "acct",
                container: "c",
                blob: "in/a b.csv",
                protocol: "https",
                version: "2021-12-02",
            },
        );
        assert!(
            q.starts_with("sv=2021-12-02&spr=https&se=2026-11-27T10%3A00%3A00Z&sr=b&sp=r&sig="),
            "{q}"
        );
    }
}
