# OpenDuct on Kubernetes

`openduct.yaml` runs OpenDuct as three nodes of a StatefulSet. Each node is a broker with the studio beside it,
and together they are one OpenDuct: any node serves the studio and takes webhooks. Every partition is kept on three
nodes, and losing one node loses nothing.

```sh
kubectl create secret generic openduct-cluster --from-literal=secret="$(openssl rand -hex 32)"
kubectl apply -f openduct.yaml
kubectl rollout status statefulset/openduct
```

Before you apply it, set three things in `openduct.yaml`:

- **`image`:** the OpenDuct image in your registry, built from this repository's `Dockerfile`.
- **`OPENDUCT_STUDIO_HOSTS`:** the name people reach the studio by.
- **Storage:** the `storage` size in `volumeClaimTemplates`.

Then put an ingress with TLS in front of the `openduct-studio` service. Webhooks are sent to the same address:
`https://<your host>/hooks/...`.

## What it sets up

- **The cluster secret:** in the `openduct-cluster` secret, the same for every node. The nodes prove to each other
  that they belong with it. It also seals the studio's keys, so every node signs sessions and opens credentials
  alike. Keep it for good: a node given another secret does not start.
- **Names:** node `openduct-N` is broker `N+1`. The headless `openduct` service gives each node its name
  (`openduct-0.openduct`, ...), which the other nodes and in-cluster Kafka clients use.
- **Starting:** `podManagementPolicy: Parallel` starts all the nodes together, because a cluster needs most of its
  brokers up to elect leaders. `publishNotReadyAddresses` lets the nodes reach each other before they are ready.
- **Disruption:** the disruption budget lets one node at a time go down, so every partition stays writable.
- **Limits:** each JavaScript or Python step runs in a process of its own, of at most 1 GiB, beside the node. Leave
  room for it in the memory limit.

## Adding a node

1. Raise `replicas`.
2. Add the new node's entry to `OPENDUCT_BROKERS`, for example
   `4=openduct-3.openduct:9093@openduct-3.openduct:9092`.
3. Apply. The nodes restart one at a time with the longer list, and the new node joins.
4. Spread the partitions onto the new node:

   ```sh
   kubectl exec openduct-0 -- openduct cluster rebalance --bootstrap 127.0.0.1:9092
   ```

## Taking a node out

1. Move its partitions to the others:

   ```sh
   kubectl exec openduct-0 -- openduct cluster remove-broker <id> --bootstrap 127.0.0.1:9092
   ```

2. Lower `replicas`. A StatefulSet always takes out its highest-numbered node.
3. Remove that node's entry from `OPENDUCT_BROKERS`.

## Kafka clients beyond the cluster

The manifest speaks plain Kafka inside the cluster's network (`OPENDUCT_ALLOW_INSECURE`). For clients beyond it,
turn on TLS and sign-in ([docs/security.md](../../docs/security.md)), and give each node an address those clients
can reach as its `OPENDUCT_ADVERTISE`.
