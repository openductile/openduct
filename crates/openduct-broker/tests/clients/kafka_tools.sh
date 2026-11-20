#!/bin/sh
# Copyright 2026 Ductile AI, Inc
# SPDX-License-Identifier: Apache-2.0
#
# Apache Kafka's own tools against OpenDuct's transactions, from Kafka's image (apache/kafka): the
# transactional producer of its performance test, its exactly-once copier (read, write and commit the group's
# offsets in one transaction, with aborts at random), and a Kafka Streams application run exactly-once.
# Not part of `cargo test`: it needs Docker, that image, and Confluent's (for the forwarder's Python only).
#
#   openduct serve --data-dir /tmp/proof --listen 127.0.0.1:19395
#   sh kafka_tools.sh [broker port] [the other brokers' ports, for a cluster]
#
# The broker tells its clients to come back to 127.0.0.1, which in a container is the container. So a small
# forwarder listens there and carries the connections to this machine, and the tools run in its network.
# Each check prints one line; exits 1 if any failed.
set -u
BP="${1:-19395}"
BROKER="127.0.0.1:$BP"
IMAGE="${KAFKA_IMAGE:-apache/kafka:4.4.0}"
PYTHON_IMAGE="${CONFLUENT_IMAGE:-confluentinc/cp-schema-registry:latest}"
RUN="$(date +%s)"
FORWARD="openduct-proof-forward-$RUN"
failed=0

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
docker run -d --rm --name "$FORWARD" --add-host host.docker.internal:host-gateway --entrypoint python3 \
    "$PYTHON_IMAGE" -c "$FORWARDER" "${@:-$BP}" >/dev/null || exit 2
trap 'docker rm -f "$FORWARD" >/dev/null 2>&1' EXIT

# tool <script in Kafka's bin, or sh> <arguments>
tool() {
    script="$1"
    shift
    case "$script" in sh) entry=sh ;; *) entry="/opt/kafka/bin/$script" ;; esac
    docker run --rm -i --network "container:$FORWARD" --entrypoint "$entry" "$IMAGE" "$@" 2>&1
}
topic() { tool kafka-topics.sh --bootstrap-server "$BROKER" --create --topic "$1" --partitions 1 >/dev/null; }
# committed <topic> <how many to wait for>: what a read_committed consumer reads, a value a line
committed() {
    tool kafka-console-consumer.sh --bootstrap-server "$BROKER" --topic "$1" --from-beginning \
        --isolation-level read_committed --max-messages "$2" --timeout-ms 20000 | grep -v -e '^\[' -e 'Processed a total' -e 'KIP-848' -e 'TimeoutException'
}
# check <name> <wanted> <got>
check() {
    if [ "$2" = "$3" ]; then
        echo "ok    $1: $3"
    else
        failed=1
        echo "FAIL  $1: wanted $2, got $3"
    fi
}

# 1. The performance test's producer, in transactions of 100 ms.
t="tools-perf-$RUN"
topic "$t"
out=$(tool kafka-producer-perf-test.sh --topic "$t" --num-records 3000 --record-size 100 --throughput 1500 \
    --transactional-id "perf-$RUN" --transaction-duration-ms 100 \
    --command-property "bootstrap.servers=$BROKER" 2>&1 || true)
case "$out" in *"records sent"*) ;; *) out=$(tool kafka-producer-perf-test.sh --topic "$t" --num-records 3000 \
    --record-size 100 --throughput 1500 --transactional-id "perf-$RUN" --transaction-duration-ms 100 \
    --producer-props "bootstrap.servers=$BROKER") ;; esac
got=$(committed "$t" 3000 | wc -l | tr -d ' ')
check "Kafka's own producer's transactions: every record is committed and read" 3000 "$got"
[ "$got" = 3000 ] || printf '%s\n' "$out" | tail -4 | sed 's/^/        /'

# 2. Kafka's exactly-once copier: 500 records from one topic to another, 50 a transaction, aborting at random.
in="tools-copy-in-$RUN"
dst="tools-copy-out-$RUN"
topic "$in"
topic "$dst"
seq 1 500 | tool kafka-console-producer.sh --bootstrap-server "$BROKER" --topic "$in" >/dev/null
out=$(tool kafka-run-class.sh org.apache.kafka.tools.TransactionalMessageCopier --input-topic "$in" \
    --input-partition 0 --output-topic "$dst" --broker-list "$BROKER" --max-messages 500 \
    --consumer-group "copier-$RUN" --transaction-size 50 --transactional-id "copier-$RUN" \
    --enable-random-aborts --use-group-metadata)
copied=$(committed "$dst" 500)
check "the exactly-once copier: records read back" 500 "$(printf '%s\n' "$copied" | grep -c .)"
check "the exactly-once copier: none twice, none missing" 500 \
    "$(printf '%s\n' "$copied" | grep . | sort -u | wc -l | tr -d ' ')"
[ "$(printf '%s\n' "$copied" | grep -c .)" = 500 ] || printf '%s\n' "$out" | tail -5 | sed 's/^/        /'

# 3. Kafka Streams, exactly-once: its word count example. (Its counts are kept in memory: the image lacks a
# library the default store on disk needs.)
topic streams-plaintext-input 2>/dev/null
topic streams-wordcount-output 2>/dev/null
printf 'all streams lead to kafka\nhello kafka streams\njoin kafka summit\n' |
    tool kafka-console-producer.sh --bootstrap-server "$BROKER" --topic streams-plaintext-input >/dev/null
out=$(tool sh -c "printf 'bootstrap.servers=$BROKER\nprocessing.guarantee=exactly_once_v2\napplication.id=wordcount-$RUN\ncommit.interval.ms=500\nreplication.factor=1\ndsl.store.suppliers.class=org.apache.kafka.streams.state.BuiltInDslStoreSuppliers\$InMemoryDslStoreSuppliers\n' > /tmp/streams.properties; timeout 60 /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo /tmp/streams.properties")
counts=$(tool kafka-console-consumer.sh --bootstrap-server "$BROKER" --topic streams-wordcount-output \
    --from-beginning --isolation-level read_committed --timeout-ms 15000 --property print.key=true \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer | grep -v -e '^\[' -e 'Processed a total' -e 'KIP-848' -e 'TimeoutException')
kafka=$(printf '%s\n' "$counts" | awk '$1 == "kafka" { n = $2 } END { print n + 0 }')
streams=$(printf '%s\n' "$counts" | awk '$1 == "streams" { n = $2 } END { print n + 0 }')
# The example's topics have fixed names: on a broker this script ran against before, they hold each run's lines.
runs=$(tool kafka-console-consumer.sh --bootstrap-server "$BROKER" --topic streams-plaintext-input \
    --from-beginning --timeout-ms 10000 | grep -c '^join kafka summit')
want="$((3 * runs)) $((2 * runs))"
check "Kafka Streams exactly_once_v2: the word count's last count of 'kafka' and of 'streams'" "$want" "$kafka $streams"
[ "$kafka $streams" = "$want" ] || printf '%s\n' "$out" | grep -i -E "error|exception" | head -8 | cut -c1-240 | sed 's/^/        /'

# 4. Kafka's operator tool, while a producer holds a transaction open (the performance test, slowly, in
# transactions of a minute).
t="tools-open-$RUN"
topic "$t"
HOLD="openduct-proof-hold-$RUN"
docker run -d --rm --name "$HOLD" --network "container:$FORWARD" --entrypoint /opt/kafka/bin/kafka-producer-perf-test.sh \
    "$IMAGE" --topic "$t" --num-records 100000 --record-size 50 --throughput 20 --transactional-id "held-$RUN" \
    --transaction-duration-ms 60000 --command-property "bootstrap.servers=$BROKER" >/dev/null
trap 'docker rm -f "$FORWARD" "$HOLD" >/dev/null 2>&1' EXIT
sleep 12
listed=$(tool kafka-transactions.sh --bootstrap-server "$BROKER" list | grep "held-$RUN" | awk '{ print $NF }')
check "kafka-transactions list: the open transaction" "Ongoing" "$listed"
described=$(tool kafka-transactions.sh --bootstrap-server "$BROKER" describe --transactional-id "held-$RUN" | grep "$t-0")
check "kafka-transactions describe: its stage and its partition" "Ongoing $t-0" \
    "$(printf '%s\n' "$described" | grep -o 'Ongoing' | head -1) $(printf '%s\n' "$described" | grep -o "$t-0" | head -1)"
producers=$(tool kafka-transactions.sh --bootstrap-server "$BROKER" describe-producers --topic "$t" --partition 0 |
    grep -v -e '^\[' -e ProducerId | grep -c '[0-9]')
check "kafka-transactions describe-producers: the partition's producer" 1 "$producers"

# 5. The producer is killed with its transaction open; the operator ends it without waiting for its timeout.
docker rm -f "$HOLD" >/dev/null 2>&1
hanging=$(tool kafka-transactions.sh --bootstrap-server "$BROKER" find-hanging --topic "$t" \
    --max-transaction-timeout 1 | grep -v '^\[' | grep -c "$t")
check "kafka-transactions find-hanging: none (the coordinator still knows this one)" 0 "$hanging"
tool kafka-transactions.sh --bootstrap-server "$BROKER" abort --topic "$t" --partition 0 --start-offset 0 >/dev/null
check "kafka-transactions abort: accepted" 0 "$?"
open=$(tool kafka-transactions.sh --bootstrap-server "$BROKER" describe-producers --topic "$t" --partition 0 |
    grep -v -e '^\[' -e ProducerId | grep '[0-9]' | awk '{ print $NF }')
check "kafka-transactions describe-producers: no transaction is open on the partition now" "None" "$open"
check "what the killed producer wrote is not read by a consumer of committed data" 0 \
    "$(committed "$t" 1 | grep -c .)"

exit $failed
