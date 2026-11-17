# OpenDuct on this machine, with OpenTofu

The same services as [deploy/compose](../../compose/README.md), set up by OpenTofu on this machine's Docker:

- OpenDuct, with tiered storage;
- the S3 store (SeaweedFS);
- Postgres with pgvector.

Every secret is made at random and kept in OpenTofu's state, so keep the state private.

```sh
tofu init
tofu apply
tofu output studio                       # http://localhost:5480
tofu output -json postgres_credential    # what a workflow's postgres credential holds
```

`tofu destroy` takes it all away, the volumes too.

## Variables

- **`openduct_image`, `build_openduct`:** by default OpenDuct's image is built from this repository. To use one
  from your registry, name it and set `build_openduct = false`.
- **`s3_image`, `postgres_image`:** the stores' images.
- **`studio_port`, `kafka_port`:** the ports on this machine (5480 and 9092).
- **`name`:** what the containers, volumes and network are named after.
