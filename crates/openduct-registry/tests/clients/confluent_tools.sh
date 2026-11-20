#!/bin/sh
# Copyright 2026 Ductile AI, Inc
# SPDX-License-Identifier: Apache-2.0
#
# The serializers of Confluent's console tools against OpenDuct's schema registry and broker, from Confluent's own image
# (confluentinc/cp-schema-registry, which carries the Avro, JSON Schema and Protobuf console producers and
# consumers). Not part of `cargo test`: it needs Docker and that image.
#
#   openduct serve --data-dir /tmp/proof --listen 127.0.0.1:19394 --registry-listen 127.0.0.1:18083 \
#       --auto-create-topics
#   sh confluent_tools.sh [broker port] [registry port]
#
# The broker tells its clients to come back to 127.0.0.1, which in a container is the container. So a small
# forwarder (a few lines of Python, run from the same image) listens on those two ports and carries them to this
# machine, and the tools run in its network: to them 127.0.0.1 is OpenDuct. Each check prints one line; exits 1 if any failed.
set -u
BP="${1:-19394}"
RP="${2:-18083}"
BROKER="127.0.0.1:$BP"
REGISTRY="http://127.0.0.1:$RP"
IMAGE="${CONFLUENT_IMAGE:-confluentinc/cp-schema-registry:latest}"
RUN="$(date +%s)"
FORWARD="openduct-proof-forward-$RUN"
failed=0

# The forwarder: a few lines of Python, which Confluent's image has, so nothing else is downloaded.
FORWARDER='
import socket, sys, threading
def pipe(a, b):
    try:
        while (data := a.recv(65536)):
            b.sendall(data)
    except OSError:
        pass
    finally:
        for s in (a, b):
            try: s.shutdown(socket.SHUT_RDWR)
            except OSError: pass
def serve(port):
    listener = socket.create_server(("127.0.0.1", port))
    while True:
        client, _ = listener.accept()
        try:
            host = socket.create_connection(("host.docker.internal", port))
        except OSError:
            client.close()
            continue
        threading.Thread(target=pipe, args=(client, host), daemon=True).start()
        threading.Thread(target=pipe, args=(host, client), daemon=True).start()
ports = [int(p) for p in sys.argv[1:]]
for port in ports[1:]:
    threading.Thread(target=serve, args=(port,), daemon=True).start()
serve(ports[0])
'
docker run -d --rm --name "$FORWARD" --add-host host.docker.internal:host-gateway --entrypoint python3 "$IMAGE" \
    -c "$FORWARDER" "$BP" "$RP" >/dev/null || exit 2
trap 'docker rm -f "$FORWARD" >/dev/null 2>&1' EXIT

tool() { docker run --rm -i --network "container:$FORWARD" "$IMAGE" "$@" 2>&1; }

# check <name> <expected text in what the consumer printed> <what it printed>
check() {
    if printf '%s' "$3" | grep -q -- "$2"; then
        echo "ok    $1"
    else
        failed=1
        echo "FAIL  $1"
        printf '%s\n' "$3" | grep -v -e '^\[' -e 'SLF4J' -e 'WARN' | tail -6 | sed 's/^/        /'
    fi
}

# round <kind> <producer> <consumer> <schema> <two records, one a line> <text expected back>
round() {
    topic="proof-tools-$1-$RUN"
    sent=$(printf '%s\n' "$5" | tool "$2" --bootstrap-server "$BROKER" --topic "$topic" \
        --property schema.registry.url="$REGISTRY" --property value.schema="$4")
    got=$(tool "$3" --bootstrap-server "$BROKER" --topic "$topic" --from-beginning --max-messages 2 \
        --timeout-ms 30000 --property schema.registry.url="$REGISTRY")
    check "$1: the console producer's serializer produces, its deserializer reads back" "$6" "$sent
$got"
}

round avro kafka-avro-console-producer kafka-avro-console-consumer \
    '{"type":"record","name":"Order","namespace":"shop","fields":[{"name":"id","type":"long"},{"name":"item","type":"string"}]}' \
    '{"id":1,"item":"tea"}
{"id":2,"item":"coffee"}' \
    '"item":"coffee"'

round json kafka-json-schema-console-producer kafka-json-schema-console-consumer \
    '{"type":"object","properties":{"page":{"type":"string"},"at":{"type":"integer"}},"required":["page"],"additionalProperties":false}' \
    '{"page":"/home","at":1}
{"page":"/pricing","at":2}' \
    '"page":"/pricing"'

round protobuf kafka-protobuf-console-producer kafka-protobuf-console-consumer \
    'syntax = "proto3"; message View { string page = 1; int64 at = 2; }' \
    '{"page":"/home","at":"10"}
{"page":"/docs","at":"11"}' \
    '"page":"/docs"'

# A schema that cannot read what the subject already holds is refused when the console producer registers it.
topic="proof-tools-avro-$RUN"
refused=$(printf '%s\n' '{"id":3,"item":"milk","total":1.5}' | tool kafka-avro-console-producer \
    --bootstrap-server "$BROKER" --topic "$topic" --property schema.registry.url="$REGISTRY" \
    --property value.schema='{"type":"record","name":"Order","namespace":"shop","fields":[{"name":"id","type":"long"},{"name":"item","type":"string"},{"name":"total","type":"double"}]}')
check "avro: a breaking schema is refused to the console producer (409)" "409" "$refused"

exit $failed
