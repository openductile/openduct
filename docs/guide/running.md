# Running OpenDuct for real

## Accepting other machines

By default OpenDuct listens on `127.0.0.1`, so only programs on the same machine can reach it. To accept other
machines, listen on every interface and say which address clients should use:

```sh
openduct serve --listen 0.0.0.0:9092 --advertise 10.0.0.5:9092 --data-dir /var/lib/openduct
```

`--advertise` is required here: OpenDuct refuses to start without it rather than tell clients to connect to
`0.0.0.0`, which they cannot do.

> **OpenDuct has no TLS or authentication yet.** Anyone who can reach the port can read and write every topic. Keep
> it on a private network, behind a firewall, or behind something that adds encryption and sign-in (a VPN or an
> authenticating TLS proxy).

## As a service (systemd)

```ini
[Unit]
Description=OpenDuct streaming broker
After=network-online.target
Wants=network-online.target

[Service]
User=openduct
ExecStart=/usr/local/bin/openduct serve --listen 0.0.0.0:9092 --advertise 10.0.0.5:9092 --data-dir /var/lib/openduct
Restart=on-failure
TimeoutStopSec=30
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

```sh
sudo systemctl enable --now openduct
openduct health --bootstrap 10.0.0.5:9092
```

OpenDuct stops cleanly on SIGTERM: it stops accepting, answers what is in flight, and releases its data directory.

## Settings from the environment

Every `serve` setting can come from an environment variable instead of a flag, which suits containers and service
managers: `--data-dir` is `OPENDUCT_DATA_DIR`, `--listen` is `OPENDUCT_LISTEN`, and so on. The
[reference](reference.md) lists them all. Secrets are the exception: they are only ever read from files (a variable
may name the file, as `OPENDUCT_CLUSTER_SECRET_FILE` does, never hold the secret). See
[deployment](../deployment.md#secrets-are-files).

## Small devices and edge gateways

OpenDuct builds for ARM boards (Raspberry Pi, NVIDIA Jetson and similar), on the board itself or cross-compiled.
To cross-compile, add the board's Rust target and a C cross-compiler for it. The cross-compiler links the binary,
and with pipelines and connectors it also builds the part of ring (the cryptography for TLS and signing) that is
written in C; the broker alone has no C to build. On Debian or Ubuntu, for 64-bit ARM:

```sh
sudo apt install gcc-aarch64-linux-gnu
rustup target add aarch64-unknown-linux-gnu
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
  cargo build --release --target aarch64-unknown-linux-gnu -p openduct
```

For a device that only needs the broker, leave pipelines out: `--no-default-features` gives an 11 MB binary that
idles in about 9 MiB of memory, against 26 MB and 11 MiB with pipelines and connectors.

On a small device:

- **Bound the memory.** Lower the request and fetch limits, for example on a board with 512 MiB:
  `--max-request-mib 8 --max-request-memory-mib 32 --max-fetch-mib 8 --max-connections 256`.
- **Bound the disk.** Give each topic a retention (`openduct topic create ... --retention-ms ...`), and use smaller
  segments (`--segment-mib 8`) so old data is freed sooner.
- **Look after flash storage.** Let producers batch (`linger.ms`) rather than switching off `--fsync`.

## Logs

OpenDuct logs to standard error. Choose the detail with `RUST_LOG` (`info` by default; `debug` adds a line for
each connection that closes), and the format with `--log-format text` (for people) or `--log-format json` (for log collectors).

## Backups

The data directory is the whole state of the broker. Back it up in one of two ways:

- **A filesystem snapshot** (LVM, ZFS, a cloud disk snapshot) taken while OpenDuct runs. It captures one instant,
  and OpenDuct recovers from it as it would from a power cut.
- **Stop, copy, start**: `systemctl stop openduct`, copy the directory, `systemctl start openduct`.

Copying the files while OpenDuct runs can catch a segment mid-write. OpenDuct cuts off such an unfinished write when
it opens the copy, so the copy is usable but may lack the last moments.

## Upgrading

Stop OpenDuct, replace the binary, start it again with the same data directory. The data directory records its
format version. A version of OpenDuct that cannot read it refuses to open it and says so, rather than guess.

## Monitoring

- `openduct health --bootstrap <host:port>`: exit status 0 when the broker answers.
- In Kubernetes:

```yaml
livenessProbe:
  tcpSocket: { port: 9092 }
readinessProbe:
  exec: { command: ["openduct", "health", "--bootstrap", "127.0.0.1:9092"] }
```

[Deployment](../deployment.md) has the full sizing table and more detail on each of these.
