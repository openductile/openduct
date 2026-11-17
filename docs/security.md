# Security

OpenDuct trusts no connection for where it comes from. Every client proves who it is on every connection, may do
only what the access rules allow it, and is checked again when its session ends. A broker refuses to listen beyond
the loopback address without TLS, sign-in and access rules, unless it is told in so many words to run insecurely
(`OPENDUCT_ALLOW_INSECURE=true` - for a laptop, never for a deployment).

## TLS on the client port

```sh
openduct serve --listen 0.0.0.0:9092 --advertise broker-1.internal:9092 \
  --tls-cert /etc/openduct/broker.pem --tls-key /etc/openduct/broker.key
```

TLS 1.3 only. With `--tls-client-ca` and `--tls-client-certs optional|required`, clients may (or must) present a
certificate that authority signed; its first URI name (a workload identity such as `spiffe://example.org/hub`), or
else its first DNS name, is the principal: `User:spiffe://example.org/hub`. The subject's common name is never used.

Between brokers, replication has its own mutual TLS (`--replication-tls-*`, see [deployment](deployment.md)).

## Signing in

| Mechanism | Setting | Notes |
|---|---|---|
| SCRAM-SHA-512, SCRAM-SHA-256 | `--credentials-file users.json` | the password never crosses the wire; the broker keeps only salted, iterated keys |
| PLAIN | `--sasl-plain` (with TLS and a credentials file) | checked against the same credentials |
| OAUTHBEARER | `--token-issuer`, `--token-audience`, `--token-jwks-file` | signed tokens (RS256/384/512, ES256/384, EdDSA); `none` and shared secrets are refused; the principal is the `sub` claim (`--token-principal-claim`) |
| Client certificate | `--tls-client-ca`, `--tls-client-certs` | see above |

Names are added with the password on standard input, never as an argument:

```sh
printf '%s' "$PASSWORD" | openduct user add --file /etc/openduct/users.json --name gateway
openduct user remove --file /etc/openduct/users.json --name gateway
```

The file is written readable by its owner only and re-read by the broker when it changes: a name added or removed
takes effect without a restart. A name the broker does not know is answered like a known one until the last step,
so sign-in does not reveal which names exist.

**Sessions end.** A session lasts `--max-session-secs` (an hour by default), or until the token expires if sooner;
then the client signs in again on the same connection (Kafka clients do this themselves) or is closed. A failed
sign-in closes the connection; an address that fails ten times within a minute is kept out for the rest of it.

## Access rules

`--acl-file rules.json`, re-read when it changes (a changed file that does not parse is refused and the rules
before it stay):

```json
{
  "super_users": ["User:ops-admin"],
  "rules": [
    {"principal": "User:tenant-42", "resource": "topic", "pattern": "prefixed", "name": "t42.",
     "operations": ["read", "write", "create", "describe"], "permission": "allow"},
    {"principal": "User:tenant-42", "resource": "group", "pattern": "prefixed", "name": "t42.",
     "operations": ["read"], "permission": "allow"},
    {"principal": "User:spiffe://example.org/hub", "resource": "topic", "name": "pod-events",
     "operations": ["write"], "permission": "allow"}
  ]
}
```

- **Deny by default:** nothing is allowed unless a rule allows it; a `deny` rule wins over every `allow`.
- Resources: `topic`, `group`, `cluster` (name `kafka-cluster`), `transactional_id`. Patterns: `literal` (the name,
  or `*` for all) and `prefixed` (a tenant's name space). `host` holds a rule to one address (default `*`).
- Operations as Kafka names them: `read`, `write`, `create`, `delete`, `alter`, `describe`, `cluster_action`,
  `describe_configs`, `alter_configs`, `idempotent_write`, `all`. Read, write, delete and alter imply describe.
- What each request needs: produce - write on the topic; fetch - read; metadata and offsets - describe; creating a
  topic - create on the cluster or the topic; deleting - delete; consumer groups - read on the group (and read on
  each topic committed); an idempotent producer - write on some topic (or idempotent_write on the cluster); moving
  partitions and removing brokers - alter on the cluster.
- A listing of topics leaves out the topics a principal may not describe; asking for one by name is answered "not
  authorized". With rules on, topics are never created implicitly.

## The `openduct` command against a secured broker

```sh
export OPENDUCT_CLIENT_TLS_CA=/etc/openduct/ca.pem
export OPENDUCT_CLIENT_SASL_MECHANISM=scram-sha-512 OPENDUCT_CLIENT_USER=ops-admin
export OPENDUCT_CLIENT_PASSWORD_FILE=/run/secrets/ops-admin
openduct topic list --bootstrap broker-1.internal:9092
```

`--tls-cert`/`--tls-key` present a client certificate; `--tls-server-name` names the certificate to expect when it
is not the host connected to (a health check on `127.0.0.1`). SCRAM also checks the broker's own proof, so a
broker that only pretends to hold the credential is refused.

## Quotas

Each principal's share of a broker, in the same rules file:

```json
"quotas": {
  "User:*":         {"produce_bytes_per_sec": 10485760, "fetch_bytes_per_sec": 20971520},
  "User:tenant-42": {"produce_bytes_per_sec": 1048576}
}
```

A principal over its share is not refused: its answers are held back as long as its excess needs (a second's burst
is allowed), so a loud tenant slows itself and no one else. `throttle_time_ms` stays 0 - some clients send a request
again when an answer carries one. One address holds at most 1,000 connections at once.


## Checking a release

Every release's packages and image carry signed records, kept by GitHub: how each was built (from which commit, by
which workflow) and what is in it (bills of materials for `openduct`, `openduct-python` and the studio's page). With
the [GitHub CLI](https://cli.github.com):

```sh
gh attestation verify openduct-v0.1.0-x86_64-unknown-linux-gnu.tar.gz --repo openductile/openduct
gh attestation verify oci://ghcr.io/openductile/openduct:v0.1.0 --repo openductile/openduct
```

A package that was changed after it was built, or built anywhere else, fails the check. The bills of materials are
also release files (`*.sbom.cdx.json`, CycloneDX).
