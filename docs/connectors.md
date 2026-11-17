# Connectors

A connector lets a [pipeline](pipelines.md) read from a system that is not a topic, or write to one. Name the
connector under `from:` or `to:` in place of `topic:`:

```yaml
name: orders-in
from:
  http:
    url: https://api.example.com/orders?updated_since={position}
    every: 30s
    headers: { Authorization: secret:SHOP_API_TOKEN }
    records: value.orders
    position: max(records, r -> r.updated_at)
to:
  topic: orders
```

```yaml
name: orders-search
from:
  topic: orders
to:
  elasticsearch:
    url: https://search.example.com:9200
    index: orders
    api_key: secret:ES_API_KEY
```

**One side of every pipeline is a topic.** To move data from one system to another, use two pipelines with a
topic between them. Everything that came in can then be read again, by another pipeline or after a mistake, and a
slow or broken destination never makes the source wait.

`where`, `let`, `each`, `map`, `key` and `headers` work the same with a connector on either side.

## Secrets

Passwords, tokens and keys never sit in a pipeline file, and are never read from the environment. A setting written
`secret:NAME` names a secret:

- **In the studio:** the workspace's credential named `NAME` (Credentials page). A credential with one field gives
  it; for one with several, name the field: `secret:warehouse.password`. A workspace's pipeline sees only its own
  workspace's credentials, and replacing one there takes effect without a restart.
- **On the command line** (`openduct pipeline run --secrets-file secrets.env`): the line `NAME=value` of that file.
  The file must be readable by its owner alone (`chmod 600`); OpenDuct refuses one others may read.

```yaml
    api_key: secret:ES_API_KEY
    password: secret:warehouse.password
```

On the command line only, `file:/path` also reads a file, trimmed: a certificate, or a secret mounted by Docker or
Kubernetes (`file:/run/secrets/db-password`). A pipeline in the studio reads no file on the server. `env:` is refused
everywhere, with where to put the secret instead.

No message, log line or error ever shows a setting's value. `openduct pipeline check` checks a file without
looking the secrets up, so it can run where they are not present.

## This machine's own network

In the studio, a pipeline's connectors reach only the internet unless OpenDuct is started with
`--allow-private-network`: an address on this machine's network - localhost, the private ranges, link-local (a
cloud's metadata service among them) - is refused, with that flag named, whether a setting gives the address, a name
resolves to it, or a redirect leads to it. So no workspace's pipeline can read what sits beside the server. Turn it on
when your databases, brokers or devices are on that network, as a self-hosted install's often are (the Compose setup
does). On the command line (`openduct pipeline run`), run by whoever runs the machine, every address is reachable.

Web-based connectors are checked at every connection; the others (databases, MQTT, Modbus, OPC UA, Kafka) have their
hosts checked as the pipeline starts.

## Delivery

- **From a connector to a topic:** messages are written to the topic first, then the connector's position (where
  it got to) is saved under the pipeline's group. A restart carries on from the saved position. A message written
  just before a stop can be written again after it.
- **From a topic to a connector:** the topic's progress is committed only once the connector has stored the
  messages. A stop in between means they are stored again. Connectors that can, store a message by its key (an
  Elasticsearch document id, for example), so storing it again replaces it rather than adding a second copy.
- **A message a connector refuses** (Elasticsearch rejects a document, say) goes to the pipeline's
  `on_error.topic` with the reason, as a message an expression could not handle does. Without one, the pipeline stops.
- **Failures that pass**, such as a dropped connection, a timeout, `429 Too Many Requests` or a `5xx` answer, are
  tried six times, with waits of half a second, then 1, 2, 4 and 8 seconds between. If they last longer, the
  pipeline stops without committing anything it has not stored, and starts again from there.

## From a system

### http

Asks a URL on a schedule and turns the answer into messages.

| Setting | Default | Meaning |
|---|---|---|
| `url` | required | The URL. `{position}` in it is replaced by the position, URL-encoded. |
| `method` | `GET` | `GET` or `POST`. |
| `body` | none | The body of a `POST`. `{position}` is replaced here too. |
| `headers` | none | Headers to send, for example `{ Authorization: secret:API_TOKEN }`. |
| `every` | `60s` | How often to ask: `500ms`, `30s`, `5m`, `1h`, `1d`. |
| `records` | the answer | An expression giving the list of records in the answer (the answer is `value`). Each record is one message. Without it, an answer that is a list gives one message per item, and anything else one message. |
| `position` | none | An expression giving where to carry on from next time (the records are `records`, the answer `value`), such as `math.greatest(records.map(r, r.seq))`. It is saved once the messages are written. |
| `start` | empty | The position the first request uses. |
| `next` | none | An expression giving the next page's URL, or null when there is none. Pages are followed within one poll, up to `max_pages`. |
| `max_pages` | `100` | The most pages one poll follows. |
| `format` | `json` | `json`, or `text` for an answer that is not JSON (one message, the whole text). |

```yaml
from:
  http:
    url: https://api.example.com/v2/tickets?since={position}
    every: 1m
    headers: { Authorization: secret:TICKETS_TOKEN }
    records: value.tickets
    position: max(records, t -> t.updated_at)
    next: value.next_page
    start: "2026-01-01T00:00:00Z"
```

### webhook

Receives HTTP POSTs, each body one message. A sender gets `202 Accepted` only once its message is in the topic,
so a delivery that was acknowledged is never lost. A sender that gets anything else can send again.

| Setting | Default | Meaning |
|---|---|---|
| `listen` | `127.0.0.1:8080` | Where to listen. `0.0.0.0:8080` accepts other machines. |
| `path` | `/` | The path deliveries are posted to. |
| `secret` | none | Check each body's HMAC-SHA256 signature against this key, refusing unsigned or wrongly signed deliveries with `401`. |
| `signature_header` | `X-Signature` | The header carrying the signature, as hex, with or without a `sha256=` prefix. |
| `max_body_kib` | `1024` | The largest body accepted. A larger one gets `413`. |
| `keep_headers` | none | Request headers to keep as message headers, for example `[X-Event-Type, X-Request-Id]`. |
| `format` | `json` | `json` (a body that is not JSON gets `400`), or `text`. |

The other answers a sender can get: `404` for another path, `405` for a method other than POST, and `503` when the
pipeline is too far behind or did not store the message within 30 seconds (send again). A connection that has not
finished sending its headers within 10 seconds is closed, and at most 1,024 connections are served at once.

```yaml
from:
  webhook:
    listen: 0.0.0.0:8080
    path: /hooks/payments
    secret: secret:PAYMENTS_WEBHOOK_SECRET
    keep_headers: [X-Event-Type]
```

### mqtt

Subscribes to topics on an MQTT broker (MQTT 3.1.1: Mosquitto, EMQX, HiveMQ, AWS IoT Core, and most gateways
devices and PLCs publish to). Each MQTT message is one message, keyed by its MQTT topic, with the MQTT topic also
in the `mqtt-topic` header.

Messages are read with QoS 1 in a persistent session, and each is acknowledged to the MQTT broker only once it is
in the topic. What devices send while the pipeline is stopped is kept by the MQTT broker for its session and
delivered when it starts again; a message that was stored but not yet acknowledged at a sudden stop is delivered
again.

| Setting | Default | Meaning |
|---|---|---|
| `url` | required | `mqtt://host:port` (port 1883 if none is given), or `mqtts://host:port` for TLS (port 8883). |
| `topics` | required | The MQTT topics to read, for example `[factory/+/temperature, alarms/#]`. `+` is one level, `#` the rest. |
| `qos` | `1` | `1`, or `0` for messages the MQTT broker sends once and does not keep. |
| `client_id` | `openduct-<pipeline name>` | The session's name. Keep it the same between runs, or the MQTT broker forgets what it kept. |
| `clean_session` | `false` | `true` starts a new session each time, so nothing sent while stopped is kept. |
| `username`, `password` | none | The login, for example `secret:MQTT_USER` and `secret:MQTT_PASSWORD`. |
| `keep_alive` | `30s` | How often the connection is checked when nothing else is sent. |
| `ca` | the system's CAs | With `mqtts://`, trust only the CA in this PEM, for an MQTT broker with a private CA: `file:/etc/openduct/ca.pem`. |
| `client_cert`, `client_key` | none | With `mqtts://`, a client certificate and its key, as PEM, as AWS IoT Core and many factory brokers ask for. |

A message larger than 16 MiB cannot be read. A refused login, a refused subscription, an MQTT broker certificate
that is not trusted, or a message too large stops the pipeline with the reason; these do not pass by trying again.

The session keeps its subscriptions. Removing a topic from `topics` does not end its subscription in a session
that already has it: start once with `clean_session: true`, or give a new `client_id`.

```yaml
from:
  mqtt:
    url: mqtts://mqtt.factory.example:8883
    topics: [factory/+/press/+/state]
    username: secret:MQTT_USER
    password: secret:MQTT_PASSWORD
```

Only one copy of a pipeline reading MQTT runs at a time: a second with the same `client_id` takes the session
over. The [machine-states example](../examples/pipelines/machine-states) reads factory presses this way.

### modbus

Reads named values from a machine, meter or PLC over Modbus TCP on a schedule. Each read is one message: a JSON
object of the values by name, keyed by `device`. A device that speaks Modbus RTU over a serial line is read through
a Modbus TCP gateway.

| Setting | Default | Meaning |
|---|---|---|
| `url` | required | `modbus://host:port` (port 502 if none is given). |
| `unit` | `1` | The unit (slave) id, 0 to 255. Behind a gateway, the id of the device on its serial line. |
| `read` | required | The values to read, each a name with its own settings (below). |
| `every` | `5s` | How often to read: `500ms`, `5s`, `1m`. |
| `changes_only` | `false` | `true` writes a reading only when a value differs from the last one written. |
| `device` | `host:port/unit` | The key of each message, and its `modbus-device` header. |
| `word_order` | `big` | `big` (high word first, as the standard has it) or `little`, for values in two or four registers. |
| `timeout` | `3s` | How long the device has to answer. |

Each value in `read`:

| Setting | Default | Meaning |
|---|---|---|
| `table` | `holding` | `holding` or `input` registers, or `coil` or `discrete` inputs (single bits). |
| `address` | required | The address in the request, from 0. A register documented as `40101` is usually holding register `100`. |
| `type` | `u16`; `bool` for coils and discrete inputs | `u16`, `i16`, `u32`, `i32`, `f32` (two registers), `u64`, `i64`, `f64` (four), or `bool`. |
| `bit` | none | For one bit (0 to 15) of a register, as a `bool`: a status word's flags. |
| `scale` | none | Multiply by this, exactly: `0.1` for a value in tenths. |

```yaml
from:
  modbus:
    url: modbus://10.0.4.20:502
    device: press-07
    every: 5s
    changes_only: true
    read:
      temperature: { address: 100, type: i16, scale: 0.1 }
      pressure:    { address: 101, type: f32 }
      door_open:   { address: 103, bit: 3 }
      parts:       { table: input, address: 20, type: u32 }
      running:     { table: coil, address: 3 }
```

That reads `{"temperature":21.5,"pressure":4.25,"door_open":true,"parts":123456,"running":true}`. Values next to
each other in a table are read in one request (up to 125 registers or 2,000 bits); addresses in between are never
read, as a device refuses an address it does not have. A float that is not a number is `null`.

A refused read (an address the device does not have, for example) or an answer that is not Modbus stops the
pipeline with the reason. A busy device, a gateway that cannot reach its device, a timeout or a dropped
connection is asked again, as other failures that pass are. The
[energy-meter example](../examples/pipelines/energy-meter) reads a panel's meter.

### opcua

Named values of a machine, PLC or SCADA server, read over OPC UA as they change. The pipeline subscribes to the
values and the server sends each change; each change is one message, keyed by `device`, so one machine's changes
stay in order:

```json
{"name":"temperature","node":"ns=2;s=Press7.Temperature","reading":81.4,"status":"Good","source_time":"2026-11-15T09:46:40.120Z","server_time":"2026-11-15T09:46:40.125Z"}
```

`reading` is the value: numbers as numbers (a float that is not a number as text), times in ISO 8601 UTC, text as
text, byte strings in base64, node ids and status codes as they are usually written, arrays as lists. A structure
is given by its type, not its fields. `status` is the server's word for the value (`Good`, `UncertainLastUsableValue`,
`BadSensorFailure`); `source_time` is when the machine took it. The `opcua-device` and `opcua-node` headers say
the same as the key and `node`. Every value is sent once when the pipeline starts.

| Setting | Default | Meaning |
|---|---|---|
| `url` | required | `opc.tcp://host:port` (port 4840 if none is given), with the endpoint's path if it has one. |
| `read` | required | The values, each a name and a node id: `temperature: ns=2;s=Press7.Temperature`, `parts: ns=3;i=1001`. |
| `every` | `1s` | How often the server samples the values and sends changes (at least `100ms`). |
| `deadband` | none | A number that changes by less than this is not sent: `0.5`. Values that are not numbers are sent on every change. |
| `device` | `host:port` | The key of each message, and its `opcua-device` header. |
| `security` | `sign_and_encrypt` | `sign_and_encrypt`, `sign`, or `none` (no signing and no encryption: only on a network that is otherwise safe). |
| `policy` | `Basic256Sha256` | With `sign` or `sign_and_encrypt`: `Basic256Sha256`, `Aes128_Sha256_RsaOaep` or `Aes256_Sha256_RsaPss`. |
| `username`, `password` | anonymous | A user on the server: `secret:OPCUA_PASSWORD`. |
| `pki` | `opcua-pki` | The folder of certificates: OpenDuct's own (made on the first run, in `own` and `private`), the servers trusted (`trusted`), and the ones refused (`rejected`). |

A server's certificate is trusted only once it is in `<pki>/trusted`. The first time the pipeline connects to a
server, it puts the server's certificate in `<pki>/rejected` and stops, saying so: check it (with whoever runs the
server) and move it to `<pki>/trusted`. The server must trust OpenDuct's certificate, in `<pki>/own`, the same way.

The first connection must succeed: an address that cannot be reached, a certificate not trusted, a user or
password refused, or a node the server does not have stops the pipeline with the reason. After that, a connection
that drops is made again, with growing waits, and the subscription with it. If the pipeline cannot keep up with
the changes, the newest are left out and the log says how many.

```yaml
from:
  opcua:
    url: opc.tcp://10.0.4.21:4840
    device: press-07
    username: secret:OPCUA_USER
    password: secret:OPCUA_PASSWORD
    every: 500ms
    deadband: 0.5
    read:
      temperature: ns=2;s=Press7.Temperature
      pressure:    ns=2;s=Press7.Pressure
      running:     ns=2;s=Press7.Running
      parts:       ns=2;s=Press7.PartCount
```

The [oven-zones example](../examples/pipelines/oven-zones) watches a curing oven's zones.

### kafka

Reads topics of another cluster that speaks the Kafka protocol: Apache Kafka 2.4 or later, another OpenDuct, or a
hosted service built on Kafka, such as Confluent Cloud, Amazon MSK or Aiven. Each message keeps its key, value and
headers, and gains `kafka-topic`, `kafka-partition` and `kafka-offset` headers saying where it came from. A message
with no value (a deletion, on a topic that keeps the latest value of each key) reads as `null`.

The pipeline reads as a member of a consumer group in that cluster, with Kafka's standard consumer protocol:
copies of the pipeline share the topics' partitions, and so can any other consumer in the same group. Each
partition's offset is committed in the group only once what was read is in the topic, so the cluster's own tools
show how far the pipeline has got, and a restart carries on from there.

| Setting | Default | Meaning |
|---|---|---|
| `brokers` | required | One or more `host:port` to start from (port 9092 if none is given), as a list or as text with commas. The cluster names its other brokers itself. |
| `topics` | required | The topics to read: `[orders, payments]`. |
| `group` | `openduct-<pipeline name>` | The consumer group in the other cluster. |
| `start` | `earliest` | Where a partition the group has never read starts: `earliest` (everything the cluster still keeps) or `latest` (only what comes next). |
| `tls` | `false` | `true` to connect with TLS, as hosted clusters ask for. |
| `ca` | the system's CAs | With `tls: true`, trust only the CA in this PEM, for a cluster with a private CA: `file:/etc/openduct/kafka-ca.pem`. |
| `client_cert`, `client_key` | none | With `tls: true`, a client certificate and its key, as PEM, for a cluster that knows its clients that way. |
| `sasl` | none | Sign in with a username and password: `scram-sha-512`, `scram-sha-256` or `plain`. `plain` sends the password as it is, so it needs `tls: true`. |
| `username`, `password` | none | The login, with `sasl`: `secret:KAFKA_USER` and `secret:KAFKA_PASSWORD`. |
| `client_id` | `openduct-<pipeline name>` | The name the cluster sees in its logs and quotas. |

Batches compressed with gzip, snappy, lz4 or zstd are read. Messages of a transaction that was aborted are read
too, as Kafka's own consumers do unless told otherwise. If the cluster removed messages (its retention) before
the pipeline read them, the pipeline carries on from the oldest message still there, and the log says how many
were lost.

A topic the cluster does not have, a refused password, a certificate that is not trusted, ACLs that do not let the
pipeline read, or a cluster older than Kafka 2.4 stops the pipeline with the reason. A leader that moves or a
broker that restarts is tried again. Amazon MSK's IAM sign-in is not supported yet: use its SASL/SCRAM or TLS
client certificate sign-in.

```yaml
name: orders-from-msk
from:
  kafka:
    brokers: b-1.orders.kafka.us-east-1.amazonaws.com:9096, b-2.orders.kafka.us-east-1.amazonaws.com:9096
    topics: [orders]
    tls: true
    sasl: scram-sha-512
    username: secret:KAFKA_USER
    password: secret:KAFKA_PASSWORD
to:
  topic: orders
```

The [paid-orders example](../examples/pipelines/paid-orders) reads a shop's order events this way.

### postgres_cdc

Every row inserted, updated and deleted in chosen tables of PostgreSQL 10 or later (change data capture), from
the database's own logical replication with its built-in `pgoutput` plugin, so nothing is installed on the
database. Hosted databases (Amazon RDS and Aurora, Google Cloud SQL, Azure Database for PostgreSQL, and others)
call it logical replication and have a setting for it.

Each change is one message, keyed by table and primary key (`public.orders/1002`) so one row's changes stay in
order. The value says what happened:

```json
{"op":"update","table":"public.orders","before":null,"after":{"id":1002,"status":"paid","total":12.50},"lsn":"0/1A2B6D0","committed":"2026-11-15T09:47:02.481Z"}
```

- `op` is `insert`, `update`, `delete` or `truncate`, or `snapshot` for a row that was already there.
- `after` is the row as it is now (`null` after a delete). `before` is what PostgreSQL sends of the row before: by
  default, for a delete its key, and for an update its key only when the key changed (otherwise `null`). For the
  whole row before every update and delete, give the table `ALTER TABLE ... REPLICA IDENTITY FULL`.
- Values keep their types: whole numbers and decimals exact as stored, booleans as `true` and `false`, `json` and
  `jsonb` as JSON. Everything else (dates, times, ids, arrays, bytes) is the text PostgreSQL writes for it.
- A large value an update left unchanged is not sent again by PostgreSQL: it is left out of `after` and named in
  `unchanged`.
- The `pg-table`, `pg-op` and `pg-lsn` headers say the same as `table`, `op` and `lsn`.

The first time the pipeline starts, it reads the rows already in the tables (`op` `snapshot`), as they were at the
moment it began, then the changes made from that moment on, so nothing falls between the two. A stop while it is
reading the tables reads them again at the next start.

The pipeline keeps its place in a **replication slot**, and confirms a transaction to it only once its rows are in
the topic. While the pipeline is stopped, the database keeps the changes for it, which takes disk space. When
you retire a pipeline, remove its slot: `SELECT pg_drop_replication_slot('openduct_orders_in');`.

| Setting | Default | Meaning |
|---|---|---|
| `url` | required | `postgres://host:port/database` (port 5432 if none is given). |
| `username`, `password` | required, none | The login: `secret:PG_USER` and `secret:PG_PASSWORD`. The user needs the `REPLICATION` attribute and may read the tables. |
| `tables` | none | The tables to read, as `schema.table` (`public` if no schema is given): `[public.orders, public.customers]`. Names are matched as PostgreSQL stores them. |
| `publication` | `openduct_<pipeline name>` | The publication naming the tables. Made from `tables` if it is not there; one an administrator made is used as it is, and then `tables` can be left out. |
| `slot` | `openduct_<pipeline name>` | The replication slot keeping the pipeline's place. |
| `snapshot` | `true` | `false` reads only the changes made from the first start on, not the rows already there. |
| `tls` | `false` | `true` to connect with TLS, as hosted databases ask for. |
| `ca` | the system's CAs | With `tls: true`, trust only the CA in this PEM: `file:/etc/openduct/rds-ca.pem`. |
| `client_cert`, `client_key` | none | With `tls: true`, a client certificate and its key, as PEM. |

The database needs `wal_level = logical`. Making the publication needs ownership of the tables; making the slot
needs the `REPLICATION` attribute (`ALTER ROLE ... REPLICATION`). Signing in is with SCRAM-SHA-256 (the
database's default) or MD5, and with a password sent as it is only over TLS. The server's SCRAM proof is checked
too, so a stand-in that does not know the password is found out.

A database without logical replication, a refused password, a table that is not there, or a user without the
rights stops the pipeline with the reason. A dropped connection or a restarting database is tried again, from
the slot's last confirmed place.

```yaml
name: orders-in
from:
  postgres_cdc:
    url: postgres://orders.abc123.us-east-1.rds.amazonaws.com:5432/shop
    username: secret:PG_USER
    password: secret:PG_PASSWORD
    tls: true
    tables: [public.orders, public.order_lines]
to:
  topic: orders
```

Only one copy of a pipeline reading a slot runs at a time. The [order-changes example](../examples/pipelines/order-changes)
turns an orders table into a stream of what happens to each order.

### mysql_cdc

Every row inserted, updated and deleted in chosen tables of MySQL 8.0 or 8.4 (change data capture), read from the
server's binary log as a replica reads it, so nothing is installed on the database. Hosted MySQL (Amazon RDS and
Aurora MySQL, Google Cloud SQL, Azure Database for MySQL) has the same settings. MariaDB is not read yet.

Each change is one message, keyed by table and primary key (`shop.orders/1002`), with the same shape as
`postgres_cdc`'s:

```json
{"op":"update","table":"shop.orders","before":{"id":1002,"status":"new","total":12.50},"after":{"id":1002,"status":"paid","total":12.50},"binlog":"binlog.000412:9086","committed":"2026-11-15T09:47:02.000Z"}
```

- `op` is `insert`, `update` or `delete`, or `snapshot` for a row that was already there. MySQL sends the whole row
  before every update and delete.
- Values keep their types: whole numbers and decimals exact, `JSON` as JSON, `ENUM` and `SET` as their text,
  `DATETIME` and `DATE` as MySQL writes them, `TIMESTAMP` in UTC, and binary columns (`BLOB`, `BINARY`) as `\x` and
  hex. `BOOL` is a number in MySQL (`TINYINT(1)`), so it is `1` or `0`.
- `binlog` is where the change is in the binary log; the `mysql-table`, `mysql-op` and `mysql-binlog` headers say
  the same as `table`, `op` and `binlog`.

MySQL keeps no place for a reader, so the pipeline keeps its own: its saved position is the binary log position
after the last transaction whose rows are in the topic (`binlog:binlog.000412:9086`), and a restart carries on from
there. The server keeps its binary logs for `binlog_expire_logs_seconds` (30 days by default): a pipeline stopped
longer than that cannot carry on, and says so.

The first time the pipeline starts, it notes the binary log position, reads the rows already in the tables in one
consistent snapshot, then reads the log from the noted position. Nothing is locked; a change made while the tables
were being read comes after the row it changed, so the topic ends as the tables are.

| Setting | Default | Meaning |
|---|---|---|
| `url` | required | `mysql://host:port` (port 3306 if none is given). |
| `username`, `password` | required, none | The login: `secret:MYSQL_USER` and `secret:MYSQL_PASSWORD`. The user needs `REPLICATION SLAVE`, `REPLICATION CLIENT` and `SELECT` on the tables. |
| `tables` | required | The tables to read, as `database.table`: `[shop.orders, shop.customers]`. |
| `snapshot` | `true` | `false` reads only the changes made from the first start on, not the rows already there. |
| `server_id` | made from the pipeline name | The server id the pipeline reads as, which MySQL needs to differ from every server and replica. |
| `tls` | `false` | `true` to connect with TLS, as hosted databases ask for. |
| `ca` | the system's CAs | With `tls: true`, trust only the CA in this PEM: `file:/etc/openduct/rds-ca.pem`. |
| `client_cert`, `client_key` | none | With `tls: true`, a client certificate and its key, as PEM. |

The server needs `log_bin` on, `binlog_format = ROW` and `binlog_row_image = FULL` (MySQL 8's defaults), and
`binlog_row_metadata = FULL`, which puts column names in the log: `SET PERSIST binlog_row_metadata = FULL`.
A compressed binary log (`binlog_transaction_compression = ON`) is not read yet. Signing in is with
`caching_sha2_password` (MySQL 8's default; without TLS the password is encrypted to the server's public key) or
`mysql_native_password`.

A setting the server lacks, a refused password, a table that is not there, or a binary log that is gone stops the
pipeline with the reason. A dropped connection or a restarting server is tried again, from the last saved position.

```yaml
name: orders-in
from:
  mysql_cdc:
    url: mysql://orders.abc123.us-east-1.rds.amazonaws.com:3306
    username: secret:MYSQL_USER
    password: secret:MYSQL_PASSWORD
    tls: true
    tables: [shop.orders, shop.order_lines]
to:
  topic: orders
```

Only one copy of a pipeline reading a server's binary log runs at a time. The
[stock-levels example](../examples/pipelines/stock-levels) keeps a shop's stock levels as they change.

### mongodb_cdc

Every document inserted, updated, replaced and deleted in chosen collections of MongoDB 6.0 or later (change data
capture), from MongoDB's change streams, with MongoDB's own Rust driver. The database is a replica set or a sharded
cluster, as MongoDB Atlas is; a single server has no change streams (a single-node replica set will do).

Each change is one message, keyed by collection and document id (`shop.orders/6738a1f2c4e5d6a7b8c9d0e1`), with the
same shape as the other change capture connectors':

```json
{"op":"update","table":"shop.orders","before":{"_id":2,"total":5.00,"status":"new"},"after":{"_id":2,"total":5.00,"status":"paid"},"changes":{"updated":{"status":"paid"},"removed":[]},"committed":"2026-11-15T09:47:02.481Z"}
```

- `op` is `insert`, `update`, `replace` or `delete`, or `snapshot` for a document that was already there; a
  collection dropped or renamed gives `drop` or `rename`.
- `after` is the whole document as it is now (looked up for an update), `null` after a delete. `before` is the
  document as it was, when the collection keeps it
  (`db.runCommand({collMod: "orders", changeStreamPreAndPostImages: {enabled: true}})`); without that, a delete's
  `before` is its key (`{"_id": ...}`) and an update's is `null`.
- `changes` (updates only) is what the update set and removed.
- Values are plain JSON: object ids as their hex, dates as UTC times, decimals exact, UUIDs as UUIDs, other binary
  data as `\x` and hex.
- The `mongodb-collection` and `mongodb-op` headers say the same as `table` and `op`.

The pipeline saves the change stream's resume token once the changes before it are in the topic, and a restart
carries on after the last stored change. The server keeps changes for as long as its oplog holds them: a
pipeline stopped longer than that cannot carry on, and says so.

The first time the pipeline starts, it notes where the change stream begins, reads the documents already in the
collections, then reads the stream from the noted point, so a change made in between comes after the document it
changed.

| Setting | Default | Meaning |
|---|---|---|
| `url` | required | `mongodb+srv://name` (as Atlas gives it) or `mongodb://host:port,host:port/?replicaSet=name`. Options after `?` are MongoDB's own; the login is a setting of its own. |
| `username`, `password` | none | The login: `secret:MONGO_USER` and `secret:MONGO_PASSWORD`. The user needs to read the collections (the `read` role on their database is enough). |
| `auth_source` | `admin` | The database the user is defined in. |
| `collections` | required | The collections to read, as `database.collection`: `[shop.orders, shop.customers]`. |
| `snapshot` | `true` | `false` reads only the changes made from the first start on, not the documents already there. |
| `tls` | as the URL says | `true` to connect with TLS; `mongodb+srv://` turns it on by itself. |
| `ca` | public CAs | Trust only the CA in this PEM: `file:/etc/openduct/mongo-ca.pem`. |
| `client_cert`, `client_key` | none | A client certificate and its key, as PEM. |

A refused password, a collection that is not there, a single server, or a place that is no longer in the oplog
stops the pipeline with the reason. A dropped connection or a new primary is followed by the driver.

```yaml
name: orders-in
from:
  mongodb_cdc:
    url: mongodb+srv://cluster0.abcde.mongodb.net
    username: secret:MONGO_USER
    password: secret:MONGO_PASSWORD
    collections: [shop.orders, shop.customers]
to:
  topic: orders
```

Only one copy of a pipeline reading a change stream runs at a time. The
[catalog-changes example](../examples/pipelines/catalog-changes) keeps the latest of each product for a search index.

### salesforce

The records of one Salesforce object (`Account`, `Opportunity`, a custom `Invoice__c`) as they are created,
changed and deleted, through Salesforce's REST API with OAuth 2.0. Nothing is installed in Salesforce: a connected
app (or an external client app) gives the pipeline its client id.

The pipeline asks with SOQL on a schedule (`every`), through `queryAll`, so records in the recycle bin are seen and
deletions arrive. Records come in the order Salesforce last changed them (`SystemModstamp`, then `Id`), and the
pipeline's position is the time and id of the last record stored, so a restart neither skips nor repeats a
record, even among records changed in the same second. A large backlog is read 2,000 records a call without
waiting.

Each record is one message, keyed by object and id (`Account/001Hp00002AbCdEIAV`):

```json
{"op":"upsert","table":"Account","before":null,"after":{"Id":"001Hp00002AbCdEIAV","Name":"Acme","Industry":"Retail","SystemModstamp":"2026-11-15T09:46:40.000+0000","IsDeleted":false}}
```

`op` is `upsert` for a record created or changed, and `delete` for one deleted (then the record is in `before`).
The `salesforce-object` and `salesforce-op` headers say the same as `table` and `op`.

| Setting | Default | Meaning |
|---|---|---|
| `login_url` | required | Where to sign in: the org's My Domain (`https://acme.my.salesforce.com`), or `https://login.salesforce.com` (`https://test.salesforce.com` for a sandbox). |
| `client_id` | required | The connected app's consumer key: `secret:SF_CLIENT_ID`. |
| `client_secret` | none | The consumer secret, for the client credentials flow (the app runs as its run-as user). |
| `private_key`, `username` | none | Instead of the secret: the app's private key (PEM) and the pre-authorised user, for the JWT bearer flow. |
| `refresh_token` | none | Instead: a refresh token a person granted the app. |
| `object` | required | The object's API name: `Account`, `Invoice__c`. |
| `fields` | every field | The fields to read: `[Id, Name, Industry]`. Every field leaves out files' contents (base64), which Salesforce gives one record at a time only. |
| `where` | none | More of a SOQL `WHERE`: `"Industry = 'Banking'"`. |
| `every` | `1m` | How often to ask (at least `5s`: every call counts against the org's API allowance). |
| `api_version` | `64.0` | The REST API version. |

A refused sign-in, an object or field that is not there, or a query Salesforce refuses stops the pipeline with
Salesforce's reason. A session that has ended is begun again once; `429` and `5xx` answers are tried again.

```yaml
name: accounts-in
from:
  salesforce:
    login_url: https://acme.my.salesforce.com
    client_id: secret:SF_CLIENT_ID
    client_secret: secret:SF_CLIENT_SECRET
    object: Account
    fields: [Id, Name, Industry, AnnualRevenue]
to:
  topic: accounts
```

The [won-deals example](../examples/pipelines/won-deals) keeps a topic of the deals won.

### servicenow

The records of one table of a ServiceNow instance (`incident`, `change_request`, `cmdb_ci_server`, a custom
`u_asset`) as they are created and updated, through the REST Table API. Nothing is installed in ServiceNow: an
integration user, or an OAuth application, is enough.

The pipeline asks on a schedule (`every`), in the order records were last updated (`sys_updated_on`, then
`sys_id`). Its position is the time and id of the last record stored, so a restart neither skips nor repeats a
record, even among records updated in the same second. A large backlog is read 1,000 records a call without
waiting. ServiceNow's API does not show deletions; a record deleted is not seen.

Each record is one message, keyed by table and id (`incident/9d385017c611228701d22104cc95c371`), with values as
ServiceNow keeps them (not display values, and references as plain `sys_id`s):

```json
{"op":"upsert","table":"incident","before":null,"after":{"sys_id":"9d385017c611228701d22104cc95c371","number":"INC0010023","short_description":"Press 7 stopped","priority":"1","sys_updated_on":"2026-11-15 09:46:40"}}
```

The `servicenow-table` header names the table.

| Setting | Default | Meaning |
|---|---|---|
| `instance` | required | The instance's address: `https://acme.service-now.com`. |
| `username`, `password` | none | An integration user, signed in with basic authentication: `secret:SN_USER`, `secret:SN_PASSWORD`. |
| `client_id`, `client_secret` | none | An OAuth application's id and secret: with `username` and `password`, OAuth's password grant; alone, the client credentials grant (where the instance allows it). |
| `table` | required | The table's name: `incident`, `u_asset`. |
| `fields` | every field | The fields to read: `[number, short_description, priority]` (`sys_id` and `sys_updated_on` are always read). |
| `query` | none | More of an encoded query, conditions only: `active=true^priority=1`. |
| `every` | `1m` | How often to ask (at least `5s`). |

Give `username` and `password`, or `client_id` and `client_secret`, or all four. A refused sign-in or a table that
is not there stops the pipeline with ServiceNow's reason; an OAuth token that has ended is got again once, and
`429` and `5xx` answers are tried again. Unless the instance is set otherwise, ServiceNow ignores a condition on
a field the table does not have, so try a `query` in the table's list view first.

```yaml
name: urgent-incidents-in
from:
  servicenow:
    instance: https://acme.service-now.com
    username: secret:SN_USER
    password: secret:SN_PASSWORD
    table: incident
    fields: [number, short_description, priority, assignment_group, state]
    query: priority=1
to:
  topic: urgent-incidents
```

### snowflake

The rows of one Snowflake table or view as they are added or changed, through Snowflake's SQL API. Nothing is
installed in Snowflake: a user who may read the table, and a warehouse to run the queries, are enough.

The pipeline asks on a schedule (`every`), in the order of a column that grows with each change (`track`: an
`UPDATED_AT` time, a sequence number), then of a unique column (`id`) for rows with the same `track` value. Its
position is the last row's values of those two columns, so a restart neither skips nor repeats a row, even among
rows changed at the same moment. A large backlog is read 1,000 rows a query without waiting. A row whose `track`
or `id` is NULL is not read, and Snowflake does not show a deleted row.

Each row is one message, keyed by table and id (`ORDERS/1042`), with the column names as Snowflake keeps them and
values as plain JSON: numbers exact, dates and times in ISO 8601 (`TIMESTAMP_TZ` with its offset, `TIMESTAMP_NTZ`
without one), `VARIANT`, `OBJECT` and `ARRAY` values as JSON, and binary as hex:

```json
{"op":"upsert","table":"ORDERS","before":null,"after":{"ORDER_ID":1042,"CUSTOMER":"Ada","TOTAL":48.50,"UPDATED_AT":"2026-11-15T09:46:40","DETAILS":{"lines":2}}}
```

The `snowflake-table` header names the table.

| Setting | Default | Meaning |
|---|---|---|
| `account` | required | The account identifier, as in the account's address: `myorg-myaccount` (or a locator with its region, `xy12345.us-east-2.aws`). |
| `user`, `private_key` | none | Key-pair sign-in: the user, and the private key whose public half is set on the user (PEM, unencrypted): `file:/etc/openduct/snowflake.p8`. |
| `token` | none | Instead: a programmatic access token, or an OAuth token: `secret:SNOWFLAKE_TOKEN`. |
| `warehouse`, `database`, `schema`, `role` | the user's defaults | Where the statements run, as `SHOW` lists them (usually in upper case). |
| `url` | `https://<account>.snowflakecomputing.com` | Another address for the account, such as its private link. |
| `table` | required | The table or view: `ORDERS`, or `SHOP.PUBLIC.ORDERS`. |
| `track` | required | The column that grows with each change. |
| `id` | none | A unique column for rows with the same `track` value. Without it, `track` must itself be unique, as a sequence is. |
| `columns` | every column | The columns to read: `[ORDER_ID, STATUS]` (`track` and `id` are always read). |
| `where` | none | More of the `WHERE`, conditions only: `STATUS = 'paid'`. |
| `every` | `1m` | How often to ask (at least `5s`; each query runs on the warehouse). |

A refused sign-in, or a table or column that is not there, stops the pipeline with Snowflake's reason. A key-pair
sign-in is renewed before its hour ends, and a query still running after 45 seconds is waited for.

```yaml
name: orders-in
from:
  snowflake:
    account: myorg-myaccount
    user: READER
    private_key: file:/etc/openduct/snowflake.p8
    warehouse: REPORTING
    database: SHOP
    schema: PUBLIC
    table: ORDERS
    track: UPDATED_AT
    id: ORDER_ID
to:
  topic: orders-from-warehouse
```

## To a system

### elasticsearch

Stores each message as a document, through the bulk API. A message with a key becomes the document with that id,
so a message stored again replaces it. Without a key, Elasticsearch gives each document an id of its own.

| Setting | Default | Meaning |
|---|---|---|
| `url` | required | Where Elasticsearch (or OpenSearch) answers. |
| `index` | required | The index the documents go to. |
| `api_key` | none | An API key. |
| `username`, `password` | none | Basic authentication, instead of an API key. |
| `action` | `index` | `index` (create or replace) or `create` (refuse a document whose id exists). |

A value that is not a JSON object, or a document Elasticsearch refuses (a field that does not match the index's
mapping, for example), goes to `on_error.topic` with Elasticsearch's reason.

### s3

Gathers messages into objects in Amazon S3, or any store that speaks its API: MinIO, Ceph, Cloudflare R2, Wasabi.
Each object holds one message value per line, gzip-compressed unless `compress: none` is set. A batch's progress
is committed only once its object is stored.

| Setting | Default | Meaning |
|---|---|---|
| `bucket` | required | The bucket. |
| `prefix` | none | Where in the bucket objects go, for example `orders/`. |
| `region` | `us-east-1` | The bucket's region. |
| `endpoint` | Amazon S3 for the region | Another store's address, for example `http://minio:9000`. |
| `path_style` | `true` with an `endpoint` | Address the bucket in the path (`host/bucket/key`), as most other stores want. |
| `access_key`, `secret_key` | required | The credentials, each written `secret:NAME`. |
| `session_token` | none | For temporary credentials. |
| `every` | `60s` | Write an object at least this often when there is something to write. |
| `max_mib` | `64` | Or as soon as one reaches this size. |
| `compress` | `gzip` | `gzip` or `none`. |

Objects are named by date, pipeline, time and where their first message came from, so they sort in the order they
were written and none is ever overwritten:

```text
orders/2026/11/15/orders-lake-20261115T103000Z-0-1200.jsonl.gz
```

### mqtt

Publishes each message to an MQTT broker, with QoS 1. A batch's progress is committed only once the MQTT broker has
acknowledged every message in it. The value is published as it is, with no headers (MQTT 3.1.1 has none).

| Setting | Default | Meaning |
|---|---|---|
| `url` | required | `mqtt://host:port` or `mqtts://host:port`, as for reading. |
| `topic` | required | The MQTT topic to publish to. `{key}` in it is replaced by the message's key: `devices/{key}/commands`. |
| `qos` | `1` | `1`, or `0` to send without waiting for acknowledgements (nothing is then known to be delivered). |
| `retain` | `false` | `true` asks the MQTT broker to keep the latest message on each topic for whoever subscribes next. |
| `client_id` | `openduct-<pipeline name>-<unique>` | Each copy of a pipeline has its own, so several can run at once. |
| `username`, `password`, `keep_alive`, `ca`, `client_cert`, `client_key` | | As for reading. |

A message with no key when `topic` has `{key}`, a key that would put `+` or `#` in the MQTT topic, or a message
larger than 16 MiB goes to `on_error.topic` with the reason.

```yaml
name: valve-commands
from:
  topic: valve-commands
to:
  mqtt:
    url: mqtts://mqtt.factory.example:8883
    topic: "factory/valves/{key}/set"
    username: secret:MQTT_USER
    password: secret:MQTT_PASSWORD
on_error:
  topic: valve-commands-refused
```

### kafka

Writes to a topic of another Kafka-protocol cluster. A message with a key goes to the partition Kafka's own
clients choose for that key, so it lands with what they write under it; messages without a key go to the
partitions in turn. The key, value and headers are written as they are.

A batch counts as stored once every replica the topic asks for has it (`acks=all`), and only then is the
pipeline's progress committed. A batch the cluster did not confirm in time is sent again, so after a failure a
message can arrive twice.

| Setting | Default | Meaning |
|---|---|---|
| `brokers`, `tls`, `ca`, `client_cert`, `client_key`, `sasl`, `username`, `password`, `client_id` | | As for reading. |
| `topic` | required | The topic to write to. It has to exist: the pipeline does not create it. |
| `compression` | `none` | `gzip`, `snappy`, `lz4` or `zstd`, for smaller batches on a cluster billed by the byte. |

A message larger than the topic takes (its `max.message.bytes`), or one the topic refuses (one with no key, on a
topic that keeps the latest value of each key), goes to `on_error.topic` with the cluster's reason.

```yaml
name: orders-to-confluent
from:
  topic: orders
to:
  kafka:
    brokers: pkc-12345.us-east-1.aws.confluent.cloud:9092
    topic: orders
    tls: true
    sasl: plain
    username: secret:CONFLUENT_API_KEY
    password: secret:CONFLUENT_API_SECRET
    compression: lz4
on_error:
  topic: orders-refused
```

### salesforce

Writes each message as a record of one Salesforce object, through the sObject Collections API, 200 records a call.
The message is the record: a JSON object of its fields by API name (`{"Email": "ada@acme.test", "LastName":
"Lovelace"}`).

- `upsert` (the default) matches records by `external_id` (an external id field such as `Email` or
  `Customer_Number__c`, or `Id`): a message written again changes the same record.
- `insert` creates a record for every message; `update` changes the record each message's `Id` names.

A record Salesforce refuses (a required field missing, a value too long, a duplicate rule) goes to
`on_error.topic` with Salesforce's reason, and the rest are written. So does a message that is not a JSON object,
or that lacks the field it is matched by.

| Setting | Default | Meaning |
|---|---|---|
| `login_url`, `client_id`, `client_secret`, `private_key`, `username`, `refresh_token`, `api_version` | | As for reading. |
| `object` | required | The object's API name: `Contact`, `Invoice__c`. |
| `mode` | `upsert` | `upsert`, `insert` or `update`. |
| `external_id` | required to upsert | The field records are matched by. |

```yaml
name: contacts-out
from:
  topic: contacts
to:
  salesforce:
    login_url: https://acme.my.salesforce.com
    client_id: secret:SF_CLIENT_ID
    client_secret: secret:SF_CLIENT_SECRET
    object: Contact
    external_id: Email
on_error:
  topic: contacts-refused
```

### servicenow

Writes each message as a record of one ServiceNow table, through the REST Table API. The message is the record:
a JSON object of its fields by name (`{"short_description": "Press 7 stopped", "urgency": "1"}`).

- `insert` (the default) creates a record for every message.
- `upsert` finds the record whose `match` field (`correlation_id`, `number`, a custom `u_order_id`) has the
  message's value, and changes it, or creates one: a message written again changes the same record.
- `update` changes the record each message's `sys_id` names.

A record ServiceNow refuses (a mandatory field missing, a data policy, a business rule stopping it) goes to
`on_error.topic` with ServiceNow's reason, and the rest are written. So does a message that is not a JSON object,
or that lacks the field it is matched by. A user not allowed to write the table (an ACL), or a table that is not
there, stops the pipeline.

| Setting | Default | Meaning |
|---|---|---|
| `instance`, `username`, `password`, `client_id`, `client_secret` | | As for reading. |
| `table` | required | The table's name: `incident`, `u_asset`. |
| `mode` | `insert` | `insert`, `upsert` or `update`. |
| `match` | required to upsert | The field records are found by. |

```yaml
name: alerts-out
from:
  topic: alerts
to:
  servicenow:
    instance: https://acme.service-now.com
    username: secret:SN_USER
    password: secret:SN_PASSWORD
    table: incident
    mode: upsert
    match: correlation_id
on_error:
  topic: alerts-refused
```

The [fault-tickets example](../examples/pipelines/fault-tickets) opens an incident for each press stopped by a
fault.

### snowflake

Writes each message as a row of one Snowflake table, many rows a statement, through Snowflake's SQL API. The
message is the row: a JSON object of its fields by column name (matched as Snowflake matches an unquoted name, so
`order_id` is `ORDER_ID`). Each value is cast to its column's type: numbers, `true` and `false`, and dates and
times as ISO 8601 text; any JSON into a `VARIANT`, `OBJECT` or `ARRAY` column.

- `insert` (the default) adds a row for every message.
- `merge` changes the row whose `match` columns have the message's values, or adds one: a message written again
  changes the same row. Messages of one key in the same batch are folded together in order.

A field the message leaves out is left as it is (a new row gets the column's default); a field that is `null` is
written as NULL. Each statement carries a request id, so one sent again after an answer is lost is not run twice.

A row Snowflake refuses (a value not of its column's type, too long, NULL where a value is required) goes to
`on_error.topic` with Snowflake's reason, and the other rows are written. So does a message that is not a JSON
object, that has a field the table has no column for, or that lacks a `match` column. A table that is not there,
or a user not allowed to write it, stops the pipeline. The user needs `SELECT` on the table as well as `INSERT`
(and `UPDATE` to merge): the pipeline reads the table's columns before it writes.

| Setting | Default | Meaning |
|---|---|---|
| `account`, `user`, `private_key`, `token`, `warehouse`, `database`, `schema`, `role`, `url` | | As for reading. |
| `table` | required | The table: `ORDERS`, or `SHOP.PUBLIC.ORDERS`. |
| `mode` | `insert` | `insert` or `merge`. |
| `match` | required to merge | The columns a row is found by: `ORDER_ID`, or `[ORDER_ID, LINE]`. |

```yaml
name: orders-out
from:
  topic: orders-clean
to:
  snowflake:
    account: myorg-myaccount
    user: LOADER
    private_key: secret:SNOWFLAKE_PRIVATE_KEY
    warehouse: LOADING
    database: SHOP
    schema: SALES
    table: ORDERS
    mode: merge
    match: ORDER_ID
on_error:
  topic: orders-refused
```

The [sales-warehouse example](../examples/pipelines/sales-warehouse) loads a shop's orders for its sales reports.

## Coming next

Change capture from SQL Server.
