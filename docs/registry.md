# Schema registry

OpenDuct has a schema registry built into the broker: the REST API of Confluent's Schema Registry, which is what
Kafka's Avro serializers and tools speak. A producer registers the schema it writes with and puts the schema's id
in front of each message; a consumer asks the registry what an id is; and a new version of a schema is taken only
if it is compatible with the ones before it.

It takes **Avro, JSON Schema and Protobuf** schemas, and a schema may use the schemas of other subjects
([references](#references)). It has been tested with HTTP clients and its own tests - among them three broker
processes serving one registry through the loss of the one that led its topic - and run against Confluent's own
serializers, in Python (`confluent-kafka`) and in the console producers and consumers of Confluent's image:
Avro, JSON Schema and Protobuf each registered, produced and read back
(`crates/openduct-registry/tests/clients`).

## Turning it on

```
openduct serve --registry-listen 127.0.0.1:8081
```

(`OPENDUCT_REGISTRY_LISTEN`.) It is off unless given. Its schemas are kept in the topic `_schemas` (one partition,
compacted), which it creates.

- **In a cluster**, give the flag to every broker that should serve it. All of them serve the same registry: each
  reads the topic and answers from memory, and a change is written to the topic and answered once the broker has
  read it back - so a client told "registered, id 7" finds id 7 there at once, and on the other brokers as soon as
  they have read that far. Two brokers that take a registration at the same moment never give one id to two
  schemas: the topic puts the changes in one order, and the later of two that cannot both stand is planned again.
- **Beyond this machine** it needs a token: `--registry-token-file <file>` (`OPENDUCT_REGISTRY_TOKEN_FILE`), a file
  only its owner may read whose first line (16 characters or more) is the token. A client sends it as
  `Authorization: Bearer <token>`, or as the password of basic sign-in - for Confluent's clients,
  `basic.auth.credentials.source=USER_INFO` and `basic.auth.user.info=<any name>:<token>`. Without one OpenDuct
  refuses to serve the registry beyond the loopback address (unless `OPENDUCT_ALLOW_INSECURE=true` was said on
  purpose). Whoever holds this token may read and change every subject: keep it for administration, and give
  teams users of their own ([who may do what](#who-may-do-what)).
- **HTTPS.** When the broker has `--tls-cert` and `--tls-key`, the registry serves HTTPS (TLS 1.3) with the same
  certificate; `--registry-tls-cert` and `--registry-tls-key` give it one of its own. TLS says who the registry
  is, not who the client is: the token is still asked for. Without a certificate the token travels in clear, so
  keep the port on a private network.

### Beside a broker that asks for TLS or sign-in

The registry keeps its schemas in a topic, which it reaches as a client of its own broker. When the broker's port
speaks TLS or asks clients to sign in, say how the registry does:

| Flag | |
|---|---|
| `--registry-broker-tls-ca <file>` | the authority that signed the broker's certificate |
| `--registry-broker-tls-server-name <name>` | the name on that certificate, when it is not the address the broker listens on |
| `--registry-broker-tls-cert`, `--registry-broker-tls-key` | a certificate the registry presents as its identity |
| `--registry-broker-sasl-mechanism` | `scram-sha-512`, `scram-sha-256`, `plain` or `oauthbearer` |
| `--registry-broker-user`, `--registry-broker-password-file` | the name and password it signs in with |
| `--registry-broker-token-file` | the token for `oauthbearer` |

(Each also as `OPENDUCT_REGISTRY_BROKER_...`.) If the broker asks for either and these do not say, the program
refuses to start and names the flag to give. With access rules (`--acl-file`), the registry's principal needs to
create, describe, configure, read and write the topic `_schemas`.

## Who may do what

Whoever the broker knows, the registry knows - by a credentials file (`--credentials-file`, the names `openduct
user add` makes), by tokens (`--token-issuer` and the rest), or by client certificates:

- a name and its password by basic sign-in, which is what Confluent's clients send for
  `basic.auth.user.info=<name>:<password>`. A password is only taken over HTTPS, or from this machine;
- a signed token as `Authorization: Bearer <token>`, verified as the broker verifies OAUTHBEARER's;
- a client certificate. Serving with the broker's certificate, the registry takes client certificates as the
  broker's port does (`--tls-client-ca`, `--tls-client-certs`); with a certificate of its own
  (`--registry-tls-cert`), `--registry-tls-client-ca` names the authority whose certificates identify its clients,
  and presenting one is optional. A client that presents a certificate the authority signed is the principal it
  names - its first URI name (a workload identity), else its first DNS name - exactly as on the broker, whatever
  its request's headers say. A certificate from another authority is refused at the handshake.

A signed-in user is held to the broker's access rules (`--acl-file`) for the resource `subject`:

| Operation | Lets its principal |
|---|---|
| `read` | see a subject's versions and schemas, look a schema up, test compatibility, refer to it from another schema |
| `write` | register a version |
| `delete` | delete versions and the subject |
| `describe_configs`, `alter_configs` | see, and set or clear, the subject's compatibility level |

```json
{"principal": "User:team-a", "resource": "subject", "pattern": "prefixed", "name": "team-a.",
 "operations": ["read", "write"], "permission": "allow"}
```

- A subject its caller may not read is left out of what is listed, and answered `403` (`40301`) when asked for.
- A schema is its subjects': by id it is seen by whoever may read one of the subjects that hold it.
- The registry's own compatibility level and its mode belong to the cluster: `describe_configs` and
  `alter_configs` on the resource `cluster`. A super user may do everything.
- Without access rules, every signed-in user may do everything - as on the broker.
- Sign-ins that fail count against their address as the broker's do: after ten in a minute it is kept out for a
  minute.

## What it answers

| | |
|---|---|
| `GET /subjects` | subjects (`?deleted=true`: soft-deleted ones too) |
| `POST /subjects/{subject}/versions` | register a schema (`"schemaType"`: `AVRO` unless it says `JSON` or `PROTOBUF`): `{"id": …}` |
| `GET /subjects/{subject}/versions` | its version numbers |
| `GET /subjects/{subject}/versions/{n or latest}` | one version; `…/schema`: the schema alone; `…/referencedby`: the ids of the schemas that refer to it |
| `POST /subjects/{subject}` | is this schema registered here, and as what? |
| `DELETE /subjects/{subject}`, `DELETE /subjects/{subject}/versions/{n}` | soft delete; with `?permanent=true` after it, for good |
| `GET /schemas/ids/{id}` | a schema by id; `…/schema`, `…/versions`, `…/subjects` |
| `GET /schemas/types` | `["AVRO","JSON","PROTOBUF"]` |
| `POST /compatibility/subjects/{subject}/versions[/{n or latest}]` | would this schema be taken? `?verbose=true`: why not |
| `GET`, `PUT /config`; `GET`, `PUT`, `DELETE /config/{subject}` | compatibility levels |
| `GET`, `PUT /mode` | `READWRITE` or `READONLY` |

Statuses and error codes are Confluent's (`40401` subject not found, `40403` schema not found, `409` incompatible,
`42201` invalid schema, and the rest). A request is at most 4 MiB. A connection is kept for the requests a client
sends one after another on it, and closed after 15 seconds of rest or a thousand requests.

## Compatibility

A subject's level, or the registry's when the subject has none of its own (`BACKWARD` unless changed):

| Level | A new version is taken when |
|---|---|
| `BACKWARD` | it reads data written with the last version |
| `FORWARD` | the last version reads data written with it |
| `FULL` | both |
| `…_TRANSITIVE` | the same against every version, not only the last |
| `NONE` | it is a valid schema |

What "reads" means depends on the kind of schema, and a refusal says where and why:

- **Avro**: the Avro specification's schema resolution. A field added needs a default to read old data, a field
  removed is skipped, `int` widens to `long`, `float` and `double` (and so on), an enum must know every symbol sent
  or have a default, names must match or be listed as aliases.
- **JSON Schema**: the new schema must accept every document the old one did, keyword by keyword as Confluent's
  registry weighs them. Types may widen and not narrow; limits (`maximum`, `minLength`, `pattern`, ...) may loosen
  and not tighten; an `enum` may grow. A property added is taken when the old schema was closed
  (`additionalProperties: false`) and refused when it was open, since an old document may hold anything under that
  name; a property removed is taken when the new schema is open; a property newly `required` needs a `default`.
  `anyOf` and `oneOf` may gain branches, `allOf` may lose parts. `$ref` inside the schema is followed. Where
  keywords it does not weigh differ (`patternProperties`, `dependencies`, `if`, `not`, `format`), it refuses: more
  than it must, never less.
- **Protobuf** (proto2 and proto3): fields may be added, removed and renamed, and enum values changed. Refused: a
  message removed; a field number that changes kind, or to a scalar not written the same way (`int32`, `uint32`,
  `int64`, `uint64`, `bool` and enums are written alike; so are `sint32` and `sint64`; `string` and `bytes`;
  `fixed32` and `sfixed32`; `fixed64` and `sfixed64`); a numeric field changing between single and `repeated`; a
  `required` field added or removed; a field taken out of a `oneof`, or several existing fields moved into one.
  Imports of the well-known types (`google/protobuf/...`) are accepted.

A subject does not change from one kind of schema to another under a level that checks.

## What Confluent's clients send

- A Protobuf schema may come as the text of its `.proto` file (Confluent's console producer sends that) or as the file's
  descriptor in base64 (the Python serializer sends that). The registry reads both, and keeps and gives back the
  text.
- Confluent's console producers expect a topic to appear the first time it is written to. OpenDuct makes topics
  when asked to; start the broker with `--auto-create-topics` for Kafka's behaviour.

## References

A schema may use what another subject defines. The request lists each such use beside the schema:

```json
{
  "schemaType": "PROTOBUF",
  "schema": "syntax = \"proto3\"; import \"common.proto\"; message Invoice { common.Money total = 1; }",
  "references": [{ "name": "common.proto", "subject": "common", "version": 1 }]
}
```

`name` is what the schema itself calls it: for **Avro** the full name of the type (`shop.Money`), for **JSON
Schema** the target of the `$ref` (`address.json`, or `address.json#/definitions/street` for a part of it), for
**Protobuf** the file of the `import`. `subject` and `version` say which registered schema it stands for; that
schema must be registered, not deleted, and of the same kind.

- A schema is checked and compared with what it refers to taken in: a change in a referred-to type counts.
- The same text with other references is another schema, with its own id.
- A version gives its references back with it, and `…/referencedby` lists the ids of the schemas that refer to
  one.
- What others refer to cannot be deleted (`42206`), softly or for good, until every schema that refers to it has
  been deleted for good.

## Deleting

A delete is soft first: the version or subject is no longer listed and takes no part in compatibility, but its
schema id still resolves, so messages already written stay readable. With `?permanent=true`, after the soft delete,
it is gone for good - and a schema with it once no version refers to it.

## Its topic

`_schemas` holds each change as a record, in the keys and values Confluent's registry writes, so the two read each
other's topics. Compaction keeps the newest record of each key: the registry a broker reads at start
is the same whether the topic was compacted or not.
