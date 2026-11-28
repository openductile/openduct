// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What a workflow's SQL steps share, whatever the database: the values a statement is given (each a parameter of
//! its own type, never written into the SQL), the limits a query runs within, and what it answers - its rows as
//! records of JSON values, each column as its type reads.

use serde_json::{Map, Value};

/// A value given to a statement: it goes to the database as a parameter, beside the SQL, so it cannot change what
/// the SQL does.
#[derive(Debug, Clone, PartialEq)]
pub enum Param {
    Null,
    Bool(bool),
    Int(i64),
    /// A number with a fraction, or too large for 64 bits, as its digits: exact.
    Decimal(String),
    Text(String),
    /// A moment, in UTC.
    Time(Civil),
}

/// A date and time of day, as the calendar writes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Civil {
    pub year: i32,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub micros: u32,
}

impl Param {
    /// Its size in bytes, for the limit on what one statement is given.
    #[must_use]
    pub fn size(&self) -> usize {
        match self {
            Self::Decimal(t) | Self::Text(t) => t.len(),
            _ => 8,
        }
    }
}

/// One statement and its values.
#[derive(Debug, Clone, PartialEq)]
pub struct Statement {
    pub sql: String,
    pub params: Vec<Param>,
}

/// What a query may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// The most rows a statement gives back: more is an error.
    pub max_rows: usize,
    /// The most bytes of values all statements together give back.
    pub max_bytes: usize,
    /// How long it all may take, signing in included.
    pub timeout: std::time::Duration,
}

/// How statements run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Mode {
    /// All of them in one transaction: committed together, or (one failing) none.
    pub transaction: bool,
    /// In a transaction the database keeps from writing, where it can, and that is rolled back after.
    pub read_only: bool,
}

/// A statement's answer: the rows of each result it gave (a statement may give several), and how many rows it
/// changed.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Answer {
    pub columns: Vec<String>,
    pub rows: Vec<Map<String, Value>>,
    /// Rows written, when the database says.
    pub affected: Option<u64>,
    /// The id the last row added was given (MySQL's AUTO_INCREMENT), when not 0.
    pub insert_id: Option<u64>,
}

/// Why statements gave no answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// It writes, and it was run to read only.
    Writes,
    Failed(String),
}

impl From<String> for Refusal {
    fn from(reason: String) -> Self {
        Self::Failed(reason)
    }
}

/// Counts what a query gives back against its limits.
#[derive(Debug)]
pub struct Budget {
    limits: Limits,
    bytes: usize,
}

impl Budget {
    #[must_use]
    pub fn new(limits: Limits) -> Self {
        Self { limits, bytes: 0 }
    }

    /// One more row of `bytes` bytes, the `rows`th of its result: an error past a limit.
    pub fn row(&mut self, rows: usize, bytes: usize) -> Result<(), String> {
        if rows > self.limits.max_rows {
            return Err(format!(
                "more than {} rows: add a LIMIT (TOP on SQL Server), or raise max_rows",
                self.limits.max_rows
            ));
        }
        self.bytes = self.bytes.saturating_add(bytes);
        if self.bytes > self.limits.max_bytes {
            return Err(format!(
                "the rows come to more than {} MiB: ask for fewer rows or columns",
                self.limits.max_bytes / (1024 * 1024)
            ));
        }
        Ok(())
    }
}

/// The TLS a database connection uses: none with `disable`; otherwise (`require`, the default) the server's
/// certificate checked against the system's roots, or against `ca` (PEM) - or, given `certificate` (PEM), only that
/// certificate trusted, whatever name it carries: for a server with a certificate of its own making.
pub fn tls_config(
    tls: &str,
    ca: Option<&str>,
    certificate: Option<&str>,
) -> Result<Option<std::sync::Arc<rustls::ClientConfig>>, String> {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;
    use rustls_platform_verifier::BuilderVerifierExt;
    match tls.trim() {
        "disable" => return Ok(None),
        "" | "require" | "strict" => {}
        other => return Err(format!("tls: require (the default) or disable, not {other:?}")),
    }
    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(std::sync::Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("cannot set up TLS: {e}"))?;
    if let Some(pem) = certificate.filter(|c| !c.trim().is_empty()) {
        let certs = CertificateDer::pem_slice_iter(pem.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "certificate: not a PEM certificate".to_owned())?;
        if certs.is_empty() {
            return Err("certificate: not a PEM certificate".to_owned());
        }
        let config = builder
            .dangerous()
            .with_custom_certificate_verifier(std::sync::Arc::new(Pinned { certs, provider }))
            .with_no_client_auth();
        return Ok(Some(std::sync::Arc::new(config)));
    }
    let config = match ca.filter(|c| !c.trim().is_empty()) {
        Some(pem) => {
            let mut roots = rustls::RootCertStore::empty();
            for cert in CertificateDer::pem_slice_iter(pem.as_bytes()) {
                let cert = cert.map_err(|_| "ca: not PEM certificates".to_owned())?;
                roots
                    .add(cert)
                    .map_err(|e| format!("ca: a certificate was not accepted: {e}"))?;
            }
            builder.with_root_certificates(roots).with_no_client_auth()
        }
        None => builder
            .with_platform_verifier()
            .map_err(|e| format!("cannot read the system's certificates: {e}"))?
            .with_no_client_auth(),
    };
    Ok(Some(std::sync::Arc::new(config)))
}

/// Trusts one certificate (or a few), as they are: the handshake is still checked to be signed by its key.
#[derive(Debug)]
struct Pinned {
    certs: Vec<rustls::pki_types::CertificateDer<'static>>,
    provider: std::sync::Arc<rustls::crypto::CryptoProvider>,
}

impl rustls::client::danger::ServerCertVerifier for Pinned {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if self.certs.iter().any(|c| c.as_ref() == end_entity.as_ref()) {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(
                "the server's certificate is not the one the credential trusts".to_owned(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// `YYYY-MM-DD` from days since 1970-01-01 (negative before): the proleptic Gregorian calendar.
#[must_use]
pub fn date_of(days: i64) -> (i64, u8, u8) {
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (
        year,
        u8::try_from(month).unwrap_or(1),
        u8::try_from(day).unwrap_or(1),
    )
}

/// Days since 1970-01-01 of a date.
#[must_use]
pub fn days_of(year: i64, month: u8, day: u8) -> i64 {
    let (m, d) = (i64::from(month), i64::from(day));
    let y = if m <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// A fraction of a second, `digits` long (none: no point), from `units` of 10^-digits seconds.
#[must_use]
pub fn fraction(units: u64, digits: u32) -> String {
    if digits == 0 {
        return String::new();
    }
    format!(".{units:0width$}", width = digits as usize)
}

/// A JSON number from a decimal's text, exact; text when it is not a number JSON can hold.
#[must_use]
pub fn number(text: &str) -> Value {
    match serde_json::from_str::<serde_json::Number>(text) {
        Ok(n) => Value::Number(n),
        Err(_) => Value::String(text.to_owned()),
    }
}

/// Binary data as `\x` and hex, as PostgreSQL writes it.
#[must_use]
pub fn hex(bytes: &[u8]) -> Value {
    let mut out = String::with_capacity(2 + 2 * bytes.len());
    out.push_str("\\x");
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    Value::String(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn days_and_dates_go_both_ways() {
        assert_eq!(date_of(0), (1970, 1, 1));
        assert_eq!(days_of(1970, 1, 1), 0);
        assert_eq!(date_of(days_of(2026, 11, 27)), (2026, 11, 27));
        assert_eq!(date_of(days_of(1, 1, 1)), (1, 1, 1));
        assert_eq!(date_of(days_of(1900, 1, 1)), (1900, 1, 1));
        assert_eq!(date_of(days_of(2000, 2, 29)), (2000, 2, 29));
        assert_eq!(days_of(1900, 1, 1), -25_567);
    }

    #[test]
    fn limits_are_said_in_plain_words() {
        let mut budget = Budget::new(Limits {
            max_rows: 2,
            max_bytes: 1024 * 1024,
            timeout: std::time::Duration::from_secs(1),
        });
        assert!(budget.row(1, 10).is_ok());
        assert!(budget.row(2, 10).is_ok());
        assert!(budget.row(3, 10).unwrap_err().contains("more than 2 rows"));
        let mut budget = Budget::new(Limits {
            max_rows: 100,
            max_bytes: 1024 * 1024,
            timeout: std::time::Duration::from_secs(1),
        });
        assert!(budget.row(1, 2 * 1024 * 1024).unwrap_err().contains("1 MiB"));
        assert_eq!(fraction(5, 3), ".005");
        assert_eq!(fraction(5, 0), "");
        assert_eq!(number("19.90").to_string(), "19.90");
        assert_eq!(hex(&[0xde, 0xad]), Value::String("\\xdead".into()));
    }
}
