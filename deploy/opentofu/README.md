# OpenDuct with OpenTofu

| Module | What it sets up |
|---|---|
| [local](local) | OpenDuct, an S3 store and PostgreSQL on this machine's Docker, as `deploy/compose` does |
| [aws-vm](aws-vm) | One virtual machine on AWS (EC2) |
| [gcp-vm](gcp-vm) | One virtual machine on Google Cloud (Compute Engine) |
| [azure-vm](azure-vm) | One virtual machine on Azure |

## One virtual machine

Each `*-vm` module starts Debian 12 and installs the released program on it with
[the same script](vm-install.sh.tftpl): the release downloaded and its checksum checked, a user of its own, a systemd
service (`openduct start`, data in `/var/lib/openduct`, stopped with SIGTERM). Nothing else is installed.

```sh
cd deploy/opentofu/aws-vm
tofu init
tofu apply -var 'ssh_cidrs=["203.0.113.7/32"]' -var key_name=my-key
tofu output studio
```

- **Closed by default.** Kafka's port listens on the machine itself only; the studio's port (5480) and SSH are open
  only to the addresses given (`studio_cidrs`, `ssh_cidrs`). With none, reach the studio through an SSH tunnel, as
  `tofu output studio` says.
- **TLS.** The studio speaks plain HTTP on 5480. Before people or webhooks reach it from the internet, put a TLS
  proxy or the cloud's load balancer in front of it, give the name it is reached by (`studio_hosts`) and set
  `secure_cookies = true`.
- **Upgrading.** On the machine, `sudo openduct-upgrade v0.1.3`: the release downloaded, its checksum checked,
  installed, and the service restarted; the one before stays as `/usr/local/bin/openduct.previous` for a rollback.
  Set `openduct_version` too, for the next machine made (Google's machines also run the start script at every
  boot). See [upgrading and rolling back](../../docs/deployment.md#upgrading-and-rolling-back).
- **Data.** It lives on the machine's disk (encrypted on AWS; Google and Azure encrypt theirs by default). Snapshot
  the disk for backups.
- **Arm.** `arch = "arm64"` runs the ARM64 release on Graviton, Tau T2A or Ampere machines.

The modules were written against each provider's documented resources (AWS `~> 6.0`, Google `~> 7.0`, AzureRM
`~> 4.0`); run `tofu validate` after `tofu init` in each before you rely on one.
