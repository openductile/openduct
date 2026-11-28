# OpenDuct's Helm chart

The chart runs OpenDuct as a StatefulSet: one node by default, or a cluster of three (or five) with
`values-cluster.yaml`. Each node is a broker with the studio beside it, and together they are one OpenDuct.
It is made from [the Kubernetes manifest](../../kubernetes/openduct.yaml) and keeps its choices.

```sh
# One node
helm install openduct deploy/helm/openduct --set studio.hosts={flows.example.com}

# A cluster of three
helm install openduct deploy/helm/openduct -f deploy/helm/openduct/values-cluster.yaml \
  --set studio.hosts={flows.example.com}
kubectl rollout status statefulset/openduct
```

Then put an ingress with TLS in front of the `<release>-studio` service, or set `ingress.enabled=true`,
`ingress.className` and `ingress.tlsSecret`. Webhooks go to the same address.

## Settings

| Value | Default | What it sets |
|---|---|---|
| `image.repository`, `image.tag` | `ghcr.io/openductile/openduct`, the chart's `appVersion` | The release run |
| `replicas` | `1` | Nodes: 1, or 3 or 5 for a cluster |
| `cluster.secret.create` | `true` | Make the cluster's secret at random the first time, and keep it on upgrades |
| `cluster.secret.existingSecret` | | A secret of your own instead, its key `secret` (16 characters or more) |
| `cluster.replicationFactor` | `3` | Copies of each partition |
| `studio.hosts` | `[flows.example.com]` | The names the studio is reached by |
| `studio.url` | | The studio's address as the world sees it (forms, runs that wait) |
| `studio.secureCookies` | `true` | Cookies marked Secure, behind the ingress's TLS |
| `studio.allowPrivateNetwork` | `false` | Let workflows reach the cluster's own network |
| `allowInsecure` | `true` | Plain Kafka inside the cluster's network; turn on TLS and sign-in for clients beyond it |
| `persistence.size`, `persistence.storageClass` | `20Gi` | Each node's volume |
| `resources` | 500m, 1Gi; at most 4Gi | Leave room for each JavaScript or Python step's own process (at most 1 GiB) |
| `extraEnv` | | More `OPENDUCT_*` settings; never secrets |

## What it sets up

- **Secrets are files.** The cluster's secret is mounted from a Kubernetes secret at
  `/run/secrets/openduct/cluster-secret`, readable by the node's group only, and never put in the environment. The
  chart marks the secret it makes `helm.sh/resource-policy: keep`: what the cluster sealed with it opens with it alone.
- **Health.** Liveness asks whether the broker answers (`openduct health --bootstrap`); readiness whether it can be
  relied on - joined, every partition it holds led and level (`openduct health --ready`, on the node's loopback
  metrics address). See [Health](../../../docs/deployment.md#health).
- **Rolling restarts.** An upgrade restarts one node at a time, and the next only once the one before is ready; a
  node asked to stop hands its partitions over first. The disruption budget lets one node at a time go down.
- **Starting a cluster.** Its nodes start together (`podManagementPolicy: Parallel`) and reach each other before
  they are ready (`publishNotReadyAddresses`).

Adding or taking out a node is done as for the manifest ([its README](../../kubernetes/README.md)): change
`replicas`, then `openduct cluster rebalance` or `remove-broker`.
