# Command reference

```text
openduct start                  run the broker and the studio (workflows in the browser)
openduct serve                  run the broker alone until Ctrl-C or SIGTERM
openduct topic create <name>    create a topic
openduct topic list             list topics and their partition counts
openduct topic delete <name>    delete a topic and everything in it
openduct send <topic> [msg...]  send messages, or one per line of standard input
openduct read <topic>           print the messages a topic holds
openduct pipeline check <file>  check a pipeline file and every expression in it
openduct pipeline test <file> --input <messages>   try a pipeline on sample messages, with no broker
openduct pipeline run <file>    run a pipeline until Ctrl-C or SIGTERM
openduct workflow check <file>  check a workflow file: its trigger, every step, every expression
openduct workflow test <file> --input <events>   run a workflow on sample events, sending nothing
openduct app new|check|test|try|list            build an app (see apps.md)
openduct cluster status|rebalance|remove-broker  a cluster's brokers and partitions
openduct user add|remove <name> a name that may sign in with SCRAM (password from standard input)
openduct health                 exit 0 if a broker answers, 1 if not
openduct --version              the version
openduct <command> --help       the help for one command
```

Every command exits with status 0 when it succeeds and 1 when it does not, printing the reason on standard error.

## openduct start

Runs the broker - with every `serve` setting below - and, beside it, the studio ([studio](../studio.md)). Its own
settings:

| Setting | What it does |
|---|---|
| `--studio-listen <addr>` | Where the studio listens: `127.0.0.1:5480` by default, this machine only. `0.0.0.0:5480` accepts others: put it behind HTTPS, and name it with `--studio-host`. |
| `--studio-host <name>` | A name the studio is reached by (`flows.example.com`), beside localhost and its address. A request naming any other is refused. |
| `--studio-secure-cookies` | Mark session cookies Secure: the studio is reached over HTTPS, through a proxy that ends TLS. |
| `--allow-private-network` | Let workflows and the studio's pipelines reach this machine's own network (localhost, private ranges, link-local) - a model server or a database beside OpenDuct. Off by default. |
| `--apps-dir <dir>` | A folder of apps of your own (`<dir>/<id>/app.yaml`), beside those OpenDuct comes with ([apps](../apps.md)). |

## openduct workflow, app, cluster, user

| Command | What it does |
|---|---|
| `workflow check <file>` | Read a workflow file (YAML or JSON) and say what is wrong, and where, without running it. |
| `workflow test <file> --input <events>` | Run it on sample events, one JSON value per line, sending nothing: each run, step by step. |
| `app new <id>` | Start an app: a folder with its definition, a README and a recorded test, ready to change. |
| `app check [dirs]` / `app test [dirs]` | Check apps' definitions / run their recorded exchanges, with no account and no network. |
| `app try <app> <action> --credential-file <file> [--send]` | Show the request one action makes, or send it as a workflow step would ([apps](../apps.md)). |
| `app list` | The apps this build carries. |
| `cluster status` | The brokers, how many partitions each holds and leads, and the moves under way. |
| `cluster rebalance` | Spread the partitions evenly over the brokers, moving as few as possible. |
| `cluster remove-broker <id>` | Take a broker out: its partitions move to the others first. |
| `user add <name>` / `user remove <name>` | A name that may sign in with SCRAM; the password is read from standard input, never from an argument. |

## openduct serve

Each setting can be given as a flag or as an environment variable. A flag wins over the environment.

### Where it listens

| Flag | Environment | Default | Meaning |
|---|---|---|---|
| `--listen` | `OPENDUCT_LISTEN` | `127.0.0.1:9092` | The address and port to listen on. `0.0.0.0:9092` accepts every interface (and then needs `--advertise`). |
| `--advertise` | `OPENDUCT_ADVERTISE` | the listen address | The `host:port` clients are told to connect to. It must be reachable by them. |
| `--node-id` | `OPENDUCT_NODE_ID` | `0` | The broker id clients see. |
| `--cluster-id` | `OPENDUCT_CLUSTER_ID` | `openduct` | The cluster id clients see. |

### Where it keeps data

| Flag | Environment | Default | Meaning |
|---|---|---|---|
| `--data-dir` | `OPENDUCT_DATA_DIR` | `openduct-data` | The directory for the durable log. It is created if missing, and only one broker may use it at a time. |
| `--memory` | `OPENDUCT_MEMORY` | off | Keep everything in memory instead: fast, and gone when the broker stops. |
| `--fsync` | `OPENDUCT_FSYNC` | `always` | `always`: every message is on disk before it is confirmed. `never`: left to the operating system, which is faster, but a machine crash can lose recent messages. |
| `--segment-mib` | `OPENDUCT_SEGMENT_MIB` | `64` | The size at which a log file (segment) is closed and a new one started. Retention removes whole segments. |
| `--retention-check-secs` | `OPENDUCT_RETENTION_CHECK_SECS` | `60` | How often expired messages are removed. |

### Topics

| Flag | Environment | Default | Meaning |
|---|---|---|---|
| `--auto-create-topics` | `OPENDUCT_AUTO_CREATE_TOPICS` | off | Create a topic when a client writes to or asks for one that does not exist. |
| `--default-partitions` | `OPENDUCT_DEFAULT_PARTITIONS` | `1` | Partitions of a topic created automatically or without a count. |

### Consumer groups

| Flag | Environment | Default | Meaning |
|---|---|---|---|
| `--group-initial-rebalance-delay-ms` | `OPENDUCT_GROUP_INITIAL_REBALANCE_DELAY_MS` | `3000` | How long a new group waits for more members before handing out partitions, so members starting together are not reshuffled one by one. |

### Limits

Each of these protects the broker from a client that sends too much, too slowly or too often.

| Flag | Environment | Default | Meaning |
|---|---|---|---|
| `--max-request-mib` | `OPENDUCT_MAX_REQUEST_MIB` | `100` | The largest single request. A larger one closes that client's connection. |
| `--max-request-memory-mib` | `OPENDUCT_MAX_REQUEST_MEMORY_MIB` | `256` | All requests in flight together. When it is spent, reading waits. At least `--max-request-mib`. |
| `--request-read-timeout-secs` | `OPENDUCT_REQUEST_READ_TIMEOUT_SECS` | `60` | How long a request may take to arrive once it has started. |
| `--max-fetch-mib` | `OPENDUCT_MAX_FETCH_MIB` | `55` | The most data one read returns. The first batch is always sent whole. |
| `--max-connections` | `OPENDUCT_MAX_CONNECTIONS` | `10000` | Client connections at once. More wait for a free slot. |
| `--idle-timeout-secs` | `OPENDUCT_IDLE_TIMEOUT_SECS` | `600` | A connection that sends nothing, or does not read its answers, for this long is closed. |

### Logs

| Flag | Environment | Default | Meaning |
|---|---|---|---|
| `--log-format` | `OPENDUCT_LOG_FORMAT` | `text` | `text` for people, `json` for log collectors. |
| | `RUST_LOG` | `info` | How much to log: `error`, `warn`, `info`, `debug` or `trace`. |

## openduct topic create

```sh
openduct topic create <name> [--partitions N] [--retention-ms MS]
```

| Option | Default | Meaning |
|---|---|---|
| `<name>` | | 1 to 249 letters, digits, `.`, `_` and `-`. |
| `--partitions` | `1` | 1 to 10,000. |
| `--retention-ms` | keep forever | Remove messages older than this many milliseconds. |

## openduct send

```sh
openduct send <topic> [message...] [--key KEY] [--header name=value]... [--partition N]
```

Each message given on the command line is sent. Without any, each line of standard input is one message, and blank
lines are skipped. `send` prints how many messages it sent.

| Option | Default | Meaning |
|---|---|---|
| `--key` | none | The key every message carries. Messages with the same key go to the same partition, chosen as other Kafka clients choose it, so they stay in order. |
| `--header` | none | A header for every message, as `name=value`. Repeat it for more. |
| `--partition` | chosen | Send to this partition. Without it and without a key, batches take the partitions in turn. |

## openduct read

```sh
openduct read <topic> [--from earliest|latest|OFFSET] [--follow] [--max N] [--partition N] [--group NAME] [--format value|json]
```

| Option | Default | Meaning |
|---|---|---|
| `--from` | `earliest` | Where to start: `earliest` (every message kept), `latest` (only messages that arrive from now on), or an offset. |
| `--follow` | off | Keep waiting for new messages instead of stopping at the end. Ctrl-C stops it. |
| `--max` | all | Stop after this many messages. |
| `--partition` | every partition | Read only this partition. |
| `--group` | none | Remember how far the reading got under this name, and carry on from there next time. |
| `--format` | `value` | `value`: each message's value on a line. `json`: one JSON line per message with its topic, partition, offset, timestamp, key, headers and value. |

`read` reads messages that any Kafka client sent, compressed or not (gzip, snappy, lz4 and zstd).

## openduct pipeline

```sh
openduct pipeline check <file>
openduct pipeline test <file> --input <messages>
openduct pipeline run <file> [--bootstrap host:port]
```

| Command | What it does |
|---|---|
| `check` | Reads the pipeline file and compiles every expression. Any mistake is shown with its place. |
| `test` | Runs the pipeline over a file of sample messages (one value per line) with no broker, and prints one line per message it would write and one per message that would fail. |
| `run` | Runs the pipeline against a broker until Ctrl-C or SIGTERM. Copies started with the same file share the topic's partitions. Every 10 seconds, and when it stops, it prints how many messages it read, wrote and set aside. |

[Pipelines](../pipelines.md) describes the file.

## openduct topic list, topic delete, send, read, pipeline run, health

| Option | Environment | Default | Meaning |
|---|---|---|---|
| `--bootstrap` | `OPENDUCT_BOOTSTRAP` | `127.0.0.1:9092` | The broker to talk to. |
| `--timeout-secs` | | `10` | Give up after this many seconds. |

`topic create` takes the same two options. `topic delete` cannot be undone.

## Security settings

| Flag | Environment | Default | What it does |
|---|---|---|---|
| `--tls-cert`, `--tls-key` | `OPENDUCT_TLS_CERT`, `OPENDUCT_TLS_KEY` | - | TLS 1.3 on the client port. |
| `--tls-client-ca` | `OPENDUCT_TLS_CLIENT_CA` | - | The authority whose certificates identify clients. |
| `--tls-client-certs` | `OPENDUCT_TLS_CLIENT_CERTS` | `none` | `none`, `optional` or `required` (mutual TLS). |
| `--credentials-file` | `OPENDUCT_CREDENTIALS_FILE` | - | SCRAM-SHA-256/512 sign-in (`openduct user add`). |
| `--sasl-plain` | `OPENDUCT_SASL_PLAIN` | off | Also PLAIN, over TLS only. |
| `--token-issuer`, `--token-audience`, `--token-jwks-file` | `OPENDUCT_TOKEN_*` | - | OAUTHBEARER sign-in with signed tokens. |
| `--acl-file` | `OPENDUCT_ACL_FILE` | - | The access rules (deny by default). |
| `--max-session-secs` | `OPENDUCT_MAX_SESSION_SECS` | `3600` | Sign in again after this long. |
| `--allow-anonymous` | `OPENDUCT_ALLOW_ANONYMOUS` | off | Let clients that do not sign in through as `User:ANONYMOUS`. |
| `--allow-insecure` | `OPENDUCT_ALLOW_INSECURE` | off | Listen beyond loopback without TLS, sign-in or rules. Only on purpose. |

Client commands (`topic`, `send`, `read`, `cluster`, `health`) take `--tls-ca`, `--tls-cert`, `--tls-key`,
`--tls-server-name`, `--sasl-mechanism`, `--user`, `--password-file`, `--token-file`
(environment `OPENDUCT_CLIENT_*`). See [security](../security.md).

