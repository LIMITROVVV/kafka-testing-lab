"""
outbox relay tests.

checks that:
- events written to outbox reach the kafka topic
- relay preserves insertion order (important for downstream processors that care about sequence)
- relay marks entries as published, so they're not sent twice
"""

import uuid
from datetime import datetime, timezone

import pytest

from app.outbox import OutboxEntry, OutboxRelay, OutboxStore
from app.producer import OrderProducer
from app.consumer import OrderConsumer


def _entry(topic: str, order_id: str, amount: float) -> OutboxEntry:
    return OutboxEntry(
        event_id=str(uuid.uuid4()),
        topic=topic,
        key=order_id,
        payload={
            "event_id": str(uuid.uuid4()),
            "order_id": order_id,
            "event_type": "order.created",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": {"amount": amount, "currency": "USD"},
        },
    )


def test_outbox_event_reaches_kafka(unique_topic, producer, make_consumer):
    store = OutboxStore()
    relay = OutboxRelay(store, producer)

    entry = _entry(unique_topic, "order-outbox-1", 100.0)
    store.append(entry)

    sent = relay.relay()
    assert sent == 1

    consumer: OrderConsumer = make_consumer()
    consumer.subscribe([unique_topic])

    msg = consumer.poll_one(timeout=5.0)
    assert msg is not None, "event should arrive from outbox relay"
    assert msg["order_id"] == "order-outbox-1"


def test_outbox_order_preserved(unique_topic, producer, make_consumer):
    """relay sends entries in insertion order."""
    store = OutboxStore()
    relay = OutboxRelay(store, producer)

    order_ids = [f"order-{i}" for i in range(5)]
    for i, oid in enumerate(order_ids):
        store.append(_entry(unique_topic, oid, float(i * 10)))

    sent = relay.relay()
    assert sent == 5

    consumer: OrderConsumer = make_consumer()
    consumer.subscribe([unique_topic])

    received = list(consumer.poll_messages(count=10, timeout_per_msg=3))
    assert len(received) == 5

    received_order_ids = [m["order_id"] for m in received]
    assert received_order_ids == order_ids, "order must match insertion order"


def test_relay_marks_published(unique_topic, producer):
    store = OutboxStore()
    relay = OutboxRelay(store, producer)

    entry = _entry(unique_topic, "order-mark-check", 55.0)
    store.append(entry)

    assert len(store.unpublished()) == 1

    relay.relay()

    # after relay, entry should be marked so a second relay doesn't resend
    assert len(store.unpublished()) == 0


def test_relay_does_not_resend_published(unique_topic, producer, make_consumer):
    """second call to relay() sends nothing if all entries already published."""
    store = OutboxStore()
    relay = OutboxRelay(store, producer)

    entry = _entry(unique_topic, "order-no-resend", 20.0)
    store.append(entry)

    relay.relay()  # first pass
    sent_again = relay.relay()  # second pass, nothing unpublished

    assert sent_again == 0

    consumer: OrderConsumer = make_consumer()
    consumer.subscribe([unique_topic])

    messages = list(consumer.poll_messages(count=5, timeout_per_msg=3))
    assert len(messages) == 1, "only one message should be in the topic"
