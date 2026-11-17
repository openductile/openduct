// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Whether a connection may go to this machine's own network - loopback, the private ranges, link-local (a cloud's
//! metadata service among them), shared address space - or only to the internet's. A pipeline or a workflow written
//! in the studio by one workspace must not reach what sits beside the server unless whoever runs OpenDuct says so
//! (`--allow-private-network`). Workflows' HTTP steps and pipelines' connectors share these rules.
//!
//! Web clients are checked as each name is resolved, when a URL names an address, and when a redirect leads to one.
//! Other connectors (databases, MQTT, Modbus, OPC UA, Kafka) have their hosts checked before they connect.

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;

/// The most redirects a web client follows.
pub const MAX_REDIRECTS: usize = 5;

/// Whether `ip` is on this machine's own network rather than the internet's.
#[must_use]
pub fn private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(a) => {
            let [first, second, ..] = a.octets();
            a.is_loopback()
                || a.is_private()
                || a.is_link_local()
                || a.is_unspecified()
                || a.is_broadcast()
                || a.is_multicast()
                || a.is_documentation()
                || first == 0
                || (first == 100 && (second & 0xC0) == 64)
        }
        IpAddr::V6(a) => {
            let first = a.segments().first().copied().unwrap_or_default();
            a.is_loopback()
                || a.is_unspecified()
                || a.is_multicast()
                || (first & 0xfe00) == 0xfc00
                || (first & 0xffc0) == 0xfe80
                || a.to_ipv4_mapped().is_some_and(|v4| private(IpAddr::V4(v4)))
        }
    }
}

/// The address a host names itself, when it is one (`10.0.0.5`, `[::1]`).
#[must_use]
pub fn literal_ip(host: &str) -> Option<IpAddr> {
    host.trim_start_matches('[').trim_end_matches(']').parse().ok()
}

fn refused(host: &str, who: &str) -> String {
    format!(
        "{host} is on this machine's own network: start OpenDuct with --allow-private-network to let {who} reach it"
    )
}

/// Resolves names as the system does, keeping only the internet's addresses.
#[derive(Debug)]
struct PublicOnly {
    who: &'static str,
}

impl reqwest::dns::Resolve for PublicOnly {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let who = self.who;
        Box::pin(async move {
            let host = name.as_str().to_owned();
            let found: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            let public: Vec<SocketAddr> = found.iter().copied().filter(|a| !private(a.ip())).collect();
            if public.is_empty() && !found.is_empty() {
                return Err(refused(&host, who).into());
            }
            let addrs: reqwest::dns::Addrs = Box::new(public.into_iter());
            Ok(addrs)
        })
    }
}

/// `builder`, reaching only the internet unless `allow_private`: names resolved to the internet's addresses alone,
/// and redirects to this machine's network refused (as are more than [`MAX_REDIRECTS`]). `who` is said in what is
/// refused: "workflows", "pipelines".
pub fn guard(
    builder: reqwest::ClientBuilder,
    allow_private: bool,
    who: &'static str,
) -> reqwest::ClientBuilder {
    let builder = builder.redirect(reqwest::redirect::Policy::custom(move |attempt| {
        if attempt.previous().len() >= MAX_REDIRECTS {
            attempt.error(format!("more than {MAX_REDIRECTS} redirects"))
        } else if !allow_private && attempt.url().host_str().and_then(literal_ip).is_some_and(private) {
            attempt.error("a redirect to this machine's own network")
        } else {
            attempt.follow()
        }
    }));
    if allow_private {
        builder
    } else {
        builder.dns_resolver(Arc::new(PublicOnly { who }))
    }
}

/// Why a URL may not be asked: it names an address on this machine's network.
#[must_use]
pub fn url_refused(url: &reqwest::Url, who: &str) -> Option<String> {
    let host = url.host_str()?;
    literal_ip(host)
        .filter(|ip| private(*ip))
        .map(|_| refused(host, who))
}

/// Why `host` may not be connected to: it is, or resolves to, an address on this machine's network. Resolving waits
/// on the system's resolver.
pub fn host_refused(host: &str, port: u16, who: &str) -> Option<String> {
    if let Some(ip) = literal_ip(host) {
        return private(ip).then(|| refused(host, who));
    }
    let found: Vec<SocketAddr> = (host, port).to_socket_addrs().ok()?.collect();
    found.iter().any(|a| private(a.ip())).then(|| refused(host, who))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn this_machines_network_is_told_from_the_internet() {
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "0.0.0.0",
            "100.64.0.1",
            "::1",
            "fd00::1",
            "fe80::1",
            "::ffff:10.0.0.1",
        ] {
            assert!(private(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["1.1.1.1", "8.8.8.8", "2606:4700::1111"] {
            assert!(!private(ip.parse().unwrap()), "{ip}");
        }
        assert!(
            host_refused("127.0.0.1", 5432, "pipelines")
                .unwrap()
                .contains("--allow-private-network")
        );
        assert!(host_refused("localhost", 5432, "pipelines").is_some());
        assert!(host_refused("[::1]", 1883, "pipelines").is_some());
        assert!(host_refused("1.1.1.1", 443, "pipelines").is_none());
    }
}
