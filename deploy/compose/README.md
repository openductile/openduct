# OpenDuct with Docker Compose

OpenDuct on one machine, with two stores beside it:

- **The S3 store:** SeaweedFS, Apache-2.0. OpenDuct keeps its older records there (tiered storage), and your
  workflows and pipelines can use it as well.
- **Postgres with pgvector:** for workflows' `postgres` steps, and for answers from your own texts (the AI
  templates).

```sh
cp .env.example .env    # set every value: openssl rand -hex 32 makes a good secret
docker compose up -d --build
```

Then open <http://localhost:5480> and make the first account. Everything listens on this machine only.

## What runs

| Service | What it is | Reached |
| --- | --- | --- |
| `openduct` | the broker and the studio (`openduct start`), with `openduct-python` for Python steps | studio <http://localhost:5480>; Kafka `localhost:9092` |
| `s3` | SeaweedFS | `http://s3:8333`, inside the Compose network |
| `postgres` | Postgres 18 with pgvector, with a `notes` table made for the AI templates | `postgres:5432`, database `openduct`, inside the network |

## Settings

- **Tiered storage:** OpenDuct runs as a cluster of one, so closed log segments go to the store's `openduct` bucket.
  The bucket is made at start (`OPENDUCT_TIER_S3_CREATE_BUCKET`), and the disk keeps only the recent segments.
- **Reaching the database from a workflow:** add a `postgres` credential in the studio. Its URL is
  `postgres://postgres:5432/openduct`, the user is `openduct`, the password is `POSTGRES_PASSWORD` from `.env`, and
  `tls` is `disable`. Workflows may reach the private network these services share
  (`OPENDUCT_ALLOW_PRIVATE_NETWORK`).
- **Another S3 store:** any S3-compatible store works: RustFS (Apache-2.0), MinIO, or AWS S3 itself. Point
  `OPENDUCT_TIER_S3_ENDPOINT` at it, put its keys in `.env` as `S3_ACCESS_KEY` and `S3_SECRET_KEY` (they reach
  OpenDuct as files), and drop the `s3` service. On AWS, leave the endpoint out and make the bucket beforehand.
- **A ready-made image:** set `OPENDUCT_IMAGE` to an image from your registry and run `docker compose up -d`
  without `--build`.

## Beyond this machine

This setup speaks plain Kafka and HTTP on `127.0.0.1`. For other machines:

- put the studio behind HTTPS (`--studio-host`, `--studio-secure-cookies`);
- turn on Kafka TLS and sign-in ([docs/security.md](../../docs/security.md));
- see [docs/deployment.md](../../docs/deployment.md) for more nodes.
