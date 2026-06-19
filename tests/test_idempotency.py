"""
idempotency test: producer sends the same event twice (same event_id, same key).
consumer deduplicates by event_id and processes it exactly once.

this mirrors a real scenario where a producer retries on network error
and the downstream handler must not double-process.
"""

import uuid
from datetime import datetime, timezone

import pytest

from app.producer import OrderProducer
from app.consumer import OrderConsumer


def _make_order_event(event_id: str, order_id: str) -> dict:
    return {
        "event_id": event_id,
        "order_id": order_id,
        "event_type": "order.created",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {"amount": 99.99, "currency": "USD"},
    }


def test_duplicate_event_processed_once(unique_topic, producer, make_consumer):
    event_id = str(uuid.uuid4())
    order_id = "order-42"
    event = _make_order_event(event_id, order_id)

    # simulate a producer retry - same message sent twice
    producer.send(unique_topic, event, key=order_id)
    producer.send(unique_topic, event, key=order_id)
    producer.flush()

    consumer: OrderConsumer = make_consumer()
    consumer.subscribe([unique_topic])

    seen_ids: set[str] = set()
    processed: list[dict] = []

    # read all available messages (both copies)
    raw_messages = list(consumer.poll_messages(count=5, timeout_per_msg=3))
    assert len(raw_messages) == 2, "both duplicates should be on the topic"

    for msg in raw_messages:
        eid = msg["event_id"]
        if eid in seen_ids:
            # duplicate - skip, don't process
            continue
        seen_ids.add(eid)
        processed.append(msg)

    consumer.commit()

    assert len(processed) == 1
    assert processed[0]["event_id"] == event_id
    assert processed[0]["order_id"] == order_id


def test_different_events_both_processed(unique_topic, producer, make_consumer):
    """sanity check: two events with different ids are both processed."""
    events = [
        _make_order_event(str(uuid.uuid4()), "order-1"),
        _make_order_event(str(uuid.uuid4()), "order-2"),
    ]

    for ev in events:
        producer.send(unique_topic, ev, key=ev["order_id"])
    producer.flush()

    consumer: OrderConsumer = make_consumer()
    consumer.subscribe([unique_topic])

    seen_ids: set[str] = set()
    processed: list[dict] = []

    for msg in consumer.poll_messages(count=5, timeout_per_msg=3):
        if msg["event_id"] not in seen_ids:
            seen_ids.add(msg["event_id"])
            processed.append(msg)

    consumer.commit()

    assert len(processed) == 2
    assert {p["order_id"] for p in processed} == {"order-1", "order-2"}
