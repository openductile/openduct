# Copyright 2026 Ductile AI, Inc
# SPDX-License-Identifier: Apache-2.0
"""Confluent's own Python client against OpenDuct's schema registry and broker.

Not part of `cargo test`: it needs `confluent-kafka[avro,json,protobuf,schemaregistry]` and a running
`openduct serve --registry-listen ...`. See README.md beside this file.

    python confluent_python.py <bootstrap> <registry url>

Each check prints one line; the script exits 1 if any failed.
"""
import sys
import time
import uuid

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaReference, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

BOOTSTRAP, REGISTRY = sys.argv[1], sys.argv[2]
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


def round_trip(topic, serializer, deserializer, values):
    """Produces `values` with `serializer`, consumes them back with `deserializer`."""
    # OpenDuct makes a topic when asked to, not on first use (unless started with --auto-create-topics).
    ADMIN.create_topics([NewTopic(topic, 1, 1)])[topic].result(15)
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    ctx = SerializationContext(topic, MessageField.VALUE)
    for v in values:
        producer.produce(topic, value=serializer(v, ctx))
    assert producer.flush(15) == 0, "messages were not delivered"
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"proof-{RUN}-{topic}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    got, deadline = [], time.time() + 30
    while len(got) < len(values) and time.time() < deadline:
        m = consumer.poll(1.0)
        if m is None or m.error():
            continue
        got.append((m.value(), deserializer(m.value(), ctx)))
    consumer.close()
    assert len(got) == len(values), f"{len(got)} of {len(values)} messages came back"
    return got


client = SchemaRegistryClient({"url": REGISTRY})

# --- Avro -----------------------------------------------------------------------------------------------------
ORDER_V1 = """{"type":"record","name":"Order","namespace":"shop","fields":[
  {"name":"id","type":"long"},{"name":"item","type":"string"}]}"""
ORDER_V2 = """{"type":"record","name":"Order","namespace":"shop","fields":[
  {"name":"id","type":"long"},{"name":"item","type":"string"},
  {"name":"note","type":["null","string"],"default":null}]}"""
ORDER_BAD = """{"type":"record","name":"Order","namespace":"shop","fields":[
  {"name":"id","type":"long"},{"name":"item","type":"string"},{"name":"total","type":"double"}]}"""
avro_topic = f"proof-avro-{RUN}"
avro_subject = f"{avro_topic}-value"


def avro_round_trip():
    orders = [{"id": 1, "item": "tea"}, {"id": 2, "item": "coffee"}]
    got = round_trip(avro_topic, AvroSerializer(client, ORDER_V1), AvroDeserializer(client), orders)
    assert [d for _, d in got] == orders, got
    raw = got[0][0]
    schema_id = int.from_bytes(raw[1:5], "big")
    assert raw[0] == 0, "the magic byte"
    assert client.get_schema(schema_id).schema_str.replace(" ", "").replace("\n", "") == ORDER_V1.replace(
        " ", ""
    ).replace("\n", "")
    return f"schema id {schema_id} in front of each message"


def avro_evolves():
    orders = [{"id": 3, "item": "milk", "note": "cold"}]
    got = round_trip(avro_topic + "-v2", AvroSerializer(client, ORDER_V2), AvroDeserializer(client, ORDER_V2), orders)
    assert got[0][1] == orders[0]
    # The same subject takes v2 after v1 (a field with a default), and the old data reads with the new schema.
    v2 = client.register_schema(avro_subject, Schema(ORDER_V2, "AVRO"))
    assert client.get_versions(avro_subject) == [1, 2], client.get_versions(avro_subject)
    latest = client.get_latest_version(avro_subject)
    assert (latest.version, latest.schema_id) == (2, v2)
    reader = AvroDeserializer(client, ORDER_V2)
    old = AvroSerializer(client, ORDER_V1)({"id": 9, "item": "x"}, SerializationContext(avro_topic, MessageField.VALUE))
    assert reader(old, SerializationContext(avro_topic, MessageField.VALUE)) == {"id": 9, "item": "x", "note": None}
    return "v1 data read with v2"


def avro_refuses_a_breaking_schema():
    assert client.test_compatibility(avro_subject, Schema(ORDER_V2, "AVRO")) is True
    assert client.test_compatibility(avro_subject, Schema(ORDER_BAD, "AVRO")) is False
    try:
        client.register_schema(avro_subject, Schema(ORDER_BAD, "AVRO"))
    except SchemaRegistryError as e:
        assert e.http_status_code == 409 and e.error_code == 409, (e.http_status_code, e.error_code)
        return f"409: {str(e)[:80]}"
    raise AssertionError("the breaking schema was registered")


def avro_auto_register_off_uses_what_is_registered():
    s = AvroSerializer(client, ORDER_V1, conf={"auto.register.schemas": False})
    out = s({"id": 5, "item": "sugar"}, SerializationContext(avro_topic, MessageField.VALUE))
    registered = client.lookup_schema(avro_subject, Schema(ORDER_V1, "AVRO"))
    assert int.from_bytes(out[1:5], "big") == registered.schema_id
    return f"looked up as version {registered.version}"


def avro_reference():
    money = """{"type":"record","name":"Money","namespace":"shop","fields":[{"name":"amount","type":"long"}]}"""
    priced = """{"type":"record","name":"Priced","namespace":"shop","fields":[{"name":"price","type":"shop.Money"}]}"""
    money_subject, priced_topic = f"money-{RUN}", f"proof-priced-{RUN}"
    client.register_schema(money_subject, Schema(money, "AVRO"))
    ref = SchemaReference("shop.Money", money_subject, 1)
    priced_id = client.register_schema(f"{priced_topic}-value", Schema(priced, "AVRO", [ref]))
    back = client.get_schema(priced_id)
    assert [(r.name, r.subject, r.version) for r in back.references] == [("shop.Money", money_subject, 1)]
    ser = AvroSerializer(client, Schema(priced, "AVRO", [ref]), conf={"auto.register.schemas": False})
    got = round_trip(priced_topic, ser, AvroDeserializer(client), [{"price": {"amount": 250}}])
    assert got[0][1] == {"price": {"amount": 250}}
    try:
        client.delete_subject(money_subject)
    except SchemaRegistryError as e:
        return f"a referenced subject cannot be deleted ({e.error_code})"
    raise AssertionError("a referenced subject was deleted")


# --- JSON Schema ----------------------------------------------------------------------------------------------
CLICK = """{"$schema":"http://json-schema.org/draft-07/schema#","title":"Click","type":"object",
 "properties":{"page":{"type":"string"},"at":{"type":"integer"}},"required":["page"],"additionalProperties":false}"""
CLICK_V2 = CLICK.replace('"at":{"type":"integer"}', '"at":{"type":"integer"},"user":{"type":"string"}')
CLICK_BAD = CLICK.replace('"required":["page"]', '"required":["page","at"]')
json_topic = f"proof-json-{RUN}"


def json_round_trip():
    clicks = [{"page": "/home", "at": 1}, {"page": "/pricing"}]
    got = round_trip(json_topic, JSONSerializer(CLICK, client), JSONDeserializer(CLICK), clicks)
    assert [d for _, d in got] == clicks, got
    latest = client.get_latest_version(f"{json_topic}-value")
    assert latest.schema.schema_type == "JSON", latest.schema.schema_type
    return f"schemaType {latest.schema.schema_type}"


def json_evolves_and_refuses():
    subject = f"{json_topic}-value"
    assert client.test_compatibility(subject, Schema(CLICK_V2, "JSON")) is True
    client.register_schema(subject, Schema(CLICK_V2, "JSON"))
    assert client.test_compatibility(subject, Schema(CLICK_BAD, "JSON")) is False
    try:
        client.register_schema(subject, Schema(CLICK_BAD, "JSON"))
    except SchemaRegistryError as e:
        assert e.http_status_code == 409
        return "an optional property added to a closed object is taken; a newly required one refused"
    raise AssertionError("the breaking schema was registered")


# --- Protobuf -------------------------------------------------------------------------------------------------
def view_class():
    """A message class built without protoc: `message View { string page = 1; int64 at = 2; }`."""
    f = descriptor_pb2.FileDescriptorProto(name=f"view_{RUN}.proto", package=f"proof{RUN}", syntax="proto3")
    m = f.message_type.add(name="View")
    m.field.add(name="page", number=1, type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
                label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL)
    m.field.add(name="at", number=2, type=descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
                label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL)
    pool = descriptor_pool.Default()
    pool.Add(f)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName(f"proof{RUN}.View"))


proto_topic = f"proof-proto-{RUN}"


def protobuf_round_trip():
    View = view_class()
    conf = {"use.deprecated.format": False}
    views = [View(page="/home", at=10), View(page="/docs", at=11)]
    got = round_trip(proto_topic, ProtobufSerializer(View, client, conf), ProtobufDeserializer(View, conf), views)
    assert [(d.page, d.at) for _, d in got] == [("/home", 10), ("/docs", 11)], got
    latest = client.get_latest_version(f"{proto_topic}-value")
    assert latest.schema.schema_type == "PROTOBUF"
    return "the .proto the client wrote was read: " + " ".join(latest.schema.schema_str.split())[:70]


def protobuf_evolves_and_refuses():
    subject = f"{proto_topic}-value"
    text = client.get_latest_version(subject).schema.schema_str
    assert "int64 at = 2;" in text, text
    wider = text.replace("int64 at = 2;", "int64 at = 2;\n  string user = 3;")
    wrong = text.replace("int64 at = 2;", "string at = 2;")
    assert client.test_compatibility(subject, Schema(wider, "PROTOBUF")) is True
    client.register_schema(subject, Schema(wider, "PROTOBUF"))
    assert client.test_compatibility(subject, Schema(wrong, "PROTOBUF")) is False
    return "a field added is taken; int64 changed to string is not"


# --- the registry's own API, through the client ---------------------------------------------------------------
def levels_and_listing():
    assert avro_subject in client.get_subjects()
    assert client.get_compatibility() in ("BACKWARD",), client.get_compatibility()
    client.set_compatibility(avro_subject, "FULL")
    assert client.get_compatibility(avro_subject) == "FULL"
    v = client.get_version(avro_subject, 1)
    assert v.subject == avro_subject and v.version == 1
    return "subject level set and read back"


def deletes():
    subject = f"proof-delete-{RUN}"
    client.register_schema(subject, Schema('"string"', "AVRO"))
    assert client.delete_subject(subject) == [1]
    assert subject not in client.get_subjects()
    assert client.delete_subject(subject, permanent=True) == [1]
    try:
        client.get_latest_version(subject)
    except SchemaRegistryError as e:
        assert e.http_status_code == 404 and e.error_code == 40401, (e.http_status_code, e.error_code)
        return "soft, then for good; then 40401"
    raise AssertionError("a deleted subject is still there")


for name, fn in [
    ("avro: serialize, produce, consume, deserialize", avro_round_trip),
    ("avro: a compatible schema is taken, old data reads with it", avro_evolves),
    ("avro: a breaking schema is refused", avro_refuses_a_breaking_schema),
    ("avro: auto.register.schemas=false finds the registered schema", avro_auto_register_off_uses_what_is_registered),
    ("avro: a schema that refers to another subject's", avro_reference),
    ("json: serialize, produce, consume, deserialize", json_round_trip),
    ("json: evolution and refusal", json_evolves_and_refuses),
    ("protobuf: serialize, produce, consume, deserialize", protobuf_round_trip),
    ("protobuf: evolution and refusal", protobuf_evolves_and_refuses),
    ("levels and listing through the client", levels_and_listing),
    ("deletes through the client", deletes),
]:
    check(name, fn)

print(f"\n{11 - len(failures)} of 11 passed")
sys.exit(1 if failures else 0)
