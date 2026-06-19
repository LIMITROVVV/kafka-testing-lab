"""
at-least-once delivery test.

scenario: consumer receives a message, crashes before committing the offset.
on restart with the same group id, the message is redelivered.
we assert it shows up again and can be processed.

this is the core guarantee of manual commit - if your handler dies mid-flight,
kafka does not lose the message.
"""

import uuid
from datetime import datetime, timezone

import pytest

from app.consumer import OrderConsumer
from app.producer import OrderProducer


def _order_event(order_id: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "event_type": "order.created",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {"amount": 50.0, "currency": "EUR"},
    }


def test_message_redelivered_after_consumer_crash(unique_topic, producer, make_consumer):
    order_id = "order-crash-test"
    event = _order_event(order_id)

    producer.send(unique_topic, event, key=order_id)
    producer.flush()

    shared_group = f"crash-test-group-{uuid.uuid4().hex[:6]}"

    # --- consumer 1: receives message, does NOT commit (simulates crash) ---
    consumer_1: OrderConsumer = make_consumer(group_id=shared_group)
    consumer_1.subscribe([unique_topic])

    msg = consumer_1.poll_one(timeout=5.0)
    assert msg is not None, "message should arrive on first consumer"
    assert msg["order_id"] == order_id

    # crash - close without committing
    # calling close() without commit() leaves the offset uncommitted
    consumer_1.close()

    # --- consumer 2: same group, picks up from last committed offset (start) ---
    consumer_2: OrderConsumer = make_consumer(group_id=shared_group)
    consumer_2.subscribe([unique_topic])

    redelivered = consumer_2.poll_one(timeout=5.0)
    assert redelivered is not None, "message must be redelivered after crash"
    assert redelivered["order_id"] == order_id
    assert redelivered["event_id"] == msg["event_id"], "same message, not a new one"

    consumer_2.commit()


def test_commit_prevents_redelivery(unique_topic, producer, make_consumer):
    """contrast: when consumer commits, restart does not redeliver."""
    order_id = "order-committed"
    event = _order_event(order_id)

    producer.send(unique_topic, event, key=order_id)
    producer.flush()

    shared_group = f"commit-test-group-{uuid.uuid4().hex[:6]}"

    consumer_1: OrderConsumer = make_consumer(group_id=shared_group)
    consumer_1.subscribe([unique_topic])

    msg = consumer_1.poll_one(timeout=5.0)
    assert msg is not None
    consumer_1.commit()  # this time we commit
    consumer_1.close()

    # restart with same group - offset was committed, nothing to redeliver
    consumer_2: OrderConsumer = make_consumer(group_id=shared_group)
    consumer_2.subscribe([unique_topic])

    redelivered = consumer_2.poll_one(timeout=4.0)
    assert redelivered is None, "committed offset means no redelivery"
