# Copyright 2026 Ductile AI, Inc
# SPDX-License-Identifier: Apache-2.0
"""A transactional producer and read_committed consumers from Confluent's Python client (librdkafka) against
OpenDuct.

Not part of `cargo test`: it needs `confluent-kafka` and a running `openduct serve`. See README.md beside this
file.

    python confluent_transactions.py <bootstrap>

Each check prints one line; the script exits 1 if any failed.
"""
import sys
import time
import uuid

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = sys.argv[1]
RUN = uuid.uuid4().hex[:8]
ADMIN = AdminClient({"bootstrap.servers": BOOTSTRAP})
failures = []


def check(name, fn):
    try:
        detail = fn()
        print(f"ok    {name}" + (f": {detail}" if detail else ""))
    except Exception as e:  # noqa: BLE001 - every failure is reported, none stops the run
        failures.append(name)
        print(f"FAIL  {name}: {type(e).__name__}: {e}")


def topic(name):
    full = f"txn-{name}-{RUN}"
    ADMIN.create_topics([NewTopic(full, 1, 1)])[full].result(15)
    return full


def transactional(tid, timeout_ms=60000):
    p = Producer({"bootstrap.servers": BOOTSTRAP, "transactional.id": f"{tid}-{RUN}",
                  "transaction.timeout.ms": timeout_ms})
    p.init_transactions(20)
    return p


def read(topics, isolation, quiet=3.0):
    """Every value in `topics` as a consumer of this isolation level sees them, from the start: read until quiet.
    The partitions are assigned, not subscribed to, so no time goes to forming a group."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"reader-{uuid.uuid4().hex[:8]}",
                  "enable.auto.commit": False, "isolation.level": isolation})
    c.assign([TopicPartition(t, 0, OFFSET_BEGINNING) for t in topics])
    got, last = [], time.time()
    while time.time() - last < quiet:
        m = c.poll(0.5)
        if m is None or m.error():
            continue
        got.append(m.value().decode())
        last = time.time()
    c.close()
    return got


def committed_together():
    a, b = topic("a"), topic("b")
    p = transactional("commit")
    p.begin_transaction()
    for i in range(3):
        p.produce(a, f"a{i}")
    for i in range(2):
        p.produce(b, f"b{i}")
    p.flush(10)
    # Written, and not yet committed: a read_committed consumer sees none of it; read_uncommitted all of it.
    assert read([a, b], "read_committed") == [], "seen before the commit"
    got = sorted(read([a, b], "read_uncommitted"))
    assert got == ["a0", "a1", "a2", "b0", "b1"], f"read_uncommitted before the commit: {got}"
    p.commit_transaction(20)
    got = sorted(read([a, b], "read_committed"))
    assert got == ["a0", "a1", "a2", "b0", "b1"], f"read_committed after the commit: {got}"
    # The same producer's next transaction.
    p.begin_transaction()
    p.produce(a, "a3")
    p.commit_transaction(20)
    got = read([a], "read_committed")
    assert got == ["a0", "a1", "a2", "a3"], f"after the second transaction: {got}"
    return "two topics, visible only after the commit"


def aborted_is_never_seen():
    t = topic("abort")
    p = transactional("abort")
    p.begin_transaction()
    for i in range(3):
        p.produce(t, f"gone{i}")
    # Sent before the abort, so the log really holds them (the client drops what it has not sent yet).
    p.flush(10)
    p.abort_transaction(20)
    p.begin_transaction()
    p.produce(t, "kept")
    p.commit_transaction(20)
    assert read([t], "read_committed") == ["kept"], read([t], "read_committed")
    assert read([t], "read_uncommitted") == ["gone0", "gone1", "gone2", "kept"]
    return "read_committed skips the aborted records, read_uncommitted sees them"


def an_open_transaction_holds_back_what_follows_it():
    t = topic("order")
    p = transactional("order")
    p.begin_transaction()
    p.produce(t, "in-transaction")
    p.flush(10)
    plain = Producer({"bootstrap.servers": BOOTSTRAP})
    plain.produce(t, "plain")
    plain.flush(10)
    assert read([t], "read_committed") == [], "what follows an open transaction waits for it"
    p.commit_transaction(20)
    assert read([t], "read_committed") == ["in-transaction", "plain"]
    return "in order, once the transaction ended"


def a_second_producer_with_the_id_fences_the_first():
    t = topic("fence")
    first = transactional("fence")
    first.begin_transaction()
    first.produce(t, "from-the-first")
    first.flush(10)
    second = transactional("fence")  # the same transactional.id
    second.begin_transaction()
    second.produce(t, "from-the-second")
    second.commit_transaction(20)
    try:
        first.commit_transaction(20)
    except KafkaException as e:
        err = e.args[0]
        assert err.fatal() or "fenc" in err.str().lower() or "epoch" in err.str().lower(), err.str()
        assert read([t], "read_committed") == ["from-the-second"], read([t], "read_committed")
        return f"the first is refused ({err.name()}), what it wrote is aborted"
    raise AssertionError("the fenced producer committed")


def offsets_are_committed_with_the_transaction():
    source, sink = topic("in"), topic("out")
    plain = Producer({"bootstrap.servers": BOOTSTRAP})
    for i in range(4):
        plain.produce(source, f"in{i}")
    plain.flush(10)
    group = f"copier-{RUN}"
    conf = {"bootstrap.servers": BOOTSTRAP, "group.id": group, "auto.offset.reset": "earliest",
            "enable.auto.commit": False, "isolation.level": "read_committed"}

    def copy(n, commit):
        c = Consumer(conf)
        c.subscribe([source])
        p = transactional("copier")
        p.begin_transaction()
        taken, deadline = [], time.time() + 30
        while len(taken) < n and time.time() < deadline:
            m = c.poll(1.0)
            if m is None or m.error():
                continue
            taken.append(m)
            p.produce(sink, m.value().decode().replace("in", "out"))
        assert len(taken) == n, f"{len(taken)} of {n} read"
        p.send_offsets_to_transaction(c.position(c.assignment()), c.consumer_group_metadata(), 20)
        (p.commit_transaction if commit else p.abort_transaction)(20)
        c.close()
        return [m.value().decode() for m in taken]

    assert copy(2, commit=False) == ["in0", "in1"]
    # Aborted: nothing was copied, and the group's position did not move - the next run reads the same two.
    assert read([sink], "read_committed") == []
    assert copy(2, commit=True) == ["in0", "in1"]
    assert copy(2, commit=True) == ["in2", "in3"], "the committed position moved with the transaction"
    assert read([sink], "read_committed") == ["out0", "out1", "out2", "out3"]
    c = Consumer(conf)
    at = c.committed([TopicPartition(source, 0)], 10)[0].offset
    c.close()
    assert at == 4, at
    return "exactly once: an aborted copy is redone, a committed one is not"


def a_transaction_left_open_past_its_timeout_is_aborted():
    t = topic("timeout")
    p = transactional("timeout", timeout_ms=2000)
    p.begin_transaction()
    p.produce(t, "too-slow")
    p.flush(10)
    time.sleep(5)
    assert read([t], "read_committed") == [], "aborted, so never seen"
    try:
        p.commit_transaction(20)
    except KafkaException as e:
        return f"the late commit is refused ({e.args[0].name()})"
    raise AssertionError("a commit long after the timeout was taken")


CHECKS = [
    ("a transaction over two topics is seen only once committed", committed_together),
    ("an aborted transaction is never seen by read_committed", aborted_is_never_seen),
    ("an open transaction holds back what follows it", an_open_transaction_holds_back_what_follows_it),
    ("a second producer with the same transactional.id fences the first", a_second_producer_with_the_id_fences_the_first),
    ("a group's offsets are committed with the transaction, or not at all", offsets_are_committed_with_the_transaction),
    ("a transaction left open past its timeout is aborted", a_transaction_left_open_past_its_timeout_is_aborted),
]
for name, fn in CHECKS:
    check(name, fn)

print(f"\n{len(CHECKS) - len(failures)} of {len(CHECKS)} passed")
sys.exit(1 if failures else 0)
