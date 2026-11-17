// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! RSA private keys and JWTs signed with them (RS256), for the systems that sign in with a key pair: Salesforce's
//! JWT bearer flow and Snowflake's key-pair sign-in.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::signature::{KeyPair, RsaKeyPair};

/// An RSA private key from PEM (PKCS #8 `PRIVATE KEY`, or PKCS #1 `RSA PRIVATE KEY`).
pub(crate) fn private_key(pem: &str) -> Result<RsaKeyPair, String> {
    use rustls::pki_types::PrivateKeyDer;
    use rustls::pki_types::pem::PemObject;
    let der =
        PrivateKeyDer::from_pem_slice(pem.as_bytes()).map_err(|_| "is not a PEM private key".to_owned())?;
    match der {
        PrivateKeyDer::Pkcs8(k) => RsaKeyPair::from_pkcs8(k.secret_pkcs8_der()),
        PrivateKeyDer::Pkcs1(k) => RsaKeyPair::from_der(k.secret_pkcs1_der()),
        _ => return Err("is not an RSA private key".to_owned()),
    }
    .map_err(|e| format!("is not an RSA private key that can sign ({e})"))
}

/// A DER length.
fn length(n: usize) -> Vec<u8> {
    match n {
        0..=127 => vec![n as u8],
        128..=255 => vec![0x81, n as u8],
        _ => vec![0x82, (n >> 8) as u8, n as u8],
    }
}

/// The key's public half as DER `SubjectPublicKeyInfo`, what `openssl rsa -pubout -outform DER` writes and what a
/// key's fingerprint is taken of.
pub(crate) fn public_key_info(key: &RsaKeyPair) -> Vec<u8> {
    // AlgorithmIdentifier: rsaEncryption (1.2.840.113549.1.1.1), no parameters.
    const ALGORITHM: [u8; 15] = [
        0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00,
    ];
    let pkcs1 = key.public_key().as_ref();
    let mut bits = vec![0x03];
    bits.extend(length(pkcs1.len() + 1));
    bits.push(0);
    bits.extend_from_slice(pkcs1);
    let mut out = vec![0x30];
    out.extend(length(ALGORITHM.len() + bits.len()));
    out.extend_from_slice(&ALGORITHM);
    out.extend(bits);
    out
}

/// A JWT of these claims, signed RS256 with the key.
pub(crate) fn signed(claims: &serde_json::Value, key: &RsaKeyPair) -> Result<String, String> {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","typ":"JWT"}"#);
    let claims = URL_SAFE_NO_PAD.encode(claims.to_string());
    let signing_input = format!("{header}.{claims}");
    let mut signature = vec![0u8; key.public().modulus_len()];
    key.sign(
        &ring::signature::RSA_PKCS1_SHA256,
        &ring::rand::SystemRandom::new(),
        signing_input.as_bytes(),
        &mut signature,
    )
    .map_err(|_| "cannot sign the sign-in JWT with the private key".to_owned())?;
    Ok(format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(signature)))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn lengths_are_der() {
        assert_eq!(length(13), [13]);
        assert_eq!(length(200), [0x81, 200]);
        assert_eq!(length(270), [0x82, 1, 14]);
        assert!(private_key("not pem").is_err());
    }
}
